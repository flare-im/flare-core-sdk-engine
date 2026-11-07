//! 链路生命周期：只有链路状态机能建连与重连。断网恢复、服务端停机窗口、暂时性拒绝的退避、
//! 终止类拒绝与结束会话的踢线、网络变化验活、无网络挂起，以及 CONNECT 带上的应用版本与连接代数。
//!
//! 本机真实的 flare-core WebSocket 服务端 + 可控中继（见 `link_lab`），SDK 以开发模式接入。

#![cfg(not(target_arch = "wasm32"))]

mod link_lab;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flare_core::common::device::DevicePlatform;
use flare_core::common::protocol::{Reliability, frame_with_system_command};
use flare_im_core_sdk::client::HeartbeatAppState;
use flare_im_core_sdk::client::config::SdkAuthConfig;
use flare_im_core_sdk::prelude::*;

use link_lab::*;

async fn one_access_point(case: &str) -> (AccessPoint, Arc<Mutex<Ledger>>, std::path::PathBuf) {
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let a = AccessPoint::start("development", ledger.clone(), ServerOptions::default()).await;
    (a, ledger, data_root("link-lifecycle", case))
}

/// 断网：没有任何别的路径去复活连接——发送不会自己重连；链路按退避重建，断线期间发出的消息
/// 在新连接上发出且只发一次；全程同时最多一条连接。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_outage_is_recovered_by_the_link_alone() {
    let (a, ledger, root) = one_access_point("outage").await;
    let client = lab_client(development_access(&[(&a.id, a.ws_url())]), &root, |_| {}).await;
    let (observed, _subscriptions) = Observed::record(&client);
    connect(&client).await;
    wait_connected(&client, Duration::from_secs(10)).await;
    let generation = client.session_generation().await;

    a.relay.cut().await;
    let offline = send_text(&client, "sent while offline").await;
    wait_until(
        "accepted after the reconnect",
        Duration::from_secs(15),
        || async { observed.accepted_distinct().contains(&offline) },
    )
    .await;
    assert_eq!(a.relay.accepted(), 2, "exactly one new connection");
    assert_eq!(ledger.lock().unwrap().first_arrivals, vec![offline]);
    assert_eq!(client.session_generation().await, generation);
    assert!(observed.failed.lock().unwrap().is_empty());
    let phases = observed.phases();
    assert!(
        phases.contains(&ConnectionPhase::Reconnecting)
            || phases.contains(&ConnectionPhase::Switching),
        "the outage is visible to the host: {phases:?}"
    );
    let _ = client.logout().await;
}

/// 服务端停机（CLOSE(SERVER_SHUTDOWN)，窗口 1–5 s）：在窗口内重连，而不是立刻冲上去。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_shutdown_reconnects_inside_the_announced_window() {
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let a = AccessPoint::start("development", ledger.clone(), ServerOptions::default()).await;
    let b = AccessPoint::start("ap-b", ledger, ServerOptions::default()).await;
    let root = data_root("link-lifecycle", "shutdown");
    let client = lab_client(
        development_access(&[(&a.id, a.ws_url()), (&b.id, b.ws_url())]),
        &root,
        |_| {},
    )
    .await;
    connect(&client).await;
    let online = wait_connected(&client, Duration::from_secs(10)).await;
    let (current, other) = if online.access_point_id.as_deref() == Some("development") {
        (&a, &b)
    } else {
        (&b, &a)
    };
    let before = other.relay.accepted();
    current
        .server
        .stop()
        .await
        .expect("stop the current server");
    let stopped_at = Instant::now();
    wait_until(
        "reconnected to the other access point",
        Duration::from_secs(10),
        || async { other.relay.accepted() > before },
    )
    .await;
    let waited = other.relay.accepted_at()[before].duration_since(stopped_at);
    assert!(
        waited >= Duration::from_millis(900),
        "waits inside the 1-5 s window: {waited:?}"
    );
    assert!(waited <= Duration::from_secs(6), "{waited:?}");
    let _ = client.logout().await;
}

/// 服务端暂时放不了行（SERVICE_UNAVAILABLE）：链路保留会话、按退避继续，`connect` 照常成功；放行后上线。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transient_rejection_backs_off_until_admitted() {
    let (a, _ledger, root) = one_access_point("transient").await;
    *a.gate.mode.lock().unwrap() = GateMode::Retryable;
    let client = lab_client(development_access(&[(&a.id, a.ws_url())]), &root, |_| {}).await;
    client
        .connect(USER_ID, Some("token"))
        .await
        .expect("a recoverable rejection keeps the session");
    let waiting = wait_phase(
        &client,
        ConnectionPhase::Reconnecting,
        Duration::from_secs(5),
    )
    .await;
    assert!(waiting.next_retry_at.is_some());
    *a.gate.mode.lock().unwrap() = GateMode::Admit;
    wait_connected(&client, Duration::from_secs(15)).await;
    assert!(a.gate.attempts.load(Ordering::SeqCst) >= 2);
    let _ = client.logout().await;
}

/// 租户不可用：终止，不再尝试；只能由用户动作离开（重新登录）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_unavailable_terminates_the_link() {
    let (a, _ledger, root) = one_access_point("tenant").await;
    *a.gate.mode.lock().unwrap() = GateMode::TenantUnavailable;
    let client = lab_client(development_access(&[(&a.id, a.ws_url())]), &root, |_| {}).await;
    let Err(error) = client.connect(USER_ID, Some("token")).await else {
        panic!("tenant unavailable");
    };
    assert_eq!(error.code(), Some(ErrorCode::PermissionDenied), "{error}");
    let status = wait_phase(
        &client,
        ConnectionPhase::ActionRequired,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(status.reason.as_deref(), Some("tenantUnavailable"));
    let attempts = a.gate.attempts.load(Ordering::SeqCst);
    client.reconnect_now().await.expect("accepted, but ignored");
    client
        .notify_network_change(NetworkChangeEvent {
            available: true,
            interface: Some("wifi".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(a.gate.attempts.load(Ordering::SeqCst), attempts, "no retry");
    let _ = client.logout().await;
}

/// 被同平台新设备顶下线（结束会话的踢线）：终止，不重连（连回去会把新设备再顶下线）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_ending_kick_needs_the_user() {
    let (a, _ledger, root) = one_access_point("kick").await;
    let client = lab_client(development_access(&[(&a.id, a.ws_url())]), &root, |_| {}).await;
    connect(&client).await;
    wait_connected(&client, Duration::from_secs(10)).await;
    let kick = flare_core::common::kick::device_conflict_kicked(&DevicePlatform::PC, "new-device");
    a.server
        .broadcast(&frame_with_system_command(kick, Reliability::AtLeastOnce))
        .await
        .expect("kick");
    let status = wait_phase(
        &client,
        ConnectionPhase::ActionRequired,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(status.reason.as_deref(), Some("kicked"));
    let accepted = a.relay.accepted();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(a.relay.accepted(), accepted, "no reconnect after a kick");
    let _ = client.logout().await;
}

/// 平台报告网络变化：对还在的连接立即验活；半开的连接在验活窗口内被判死，换到另一个接入点。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_network_change_probes_a_half_open_link() {
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let a = AccessPoint::start("development", ledger.clone(), ServerOptions::default()).await;
    let b = AccessPoint::start("ap-b", ledger, ServerOptions::default()).await;
    let root = data_root("link-lifecycle", "network-probe");
    let client = lab_client(
        development_access(&[(&a.id, a.ws_url()), (&b.id, b.ws_url())]),
        &root,
        |_| {},
    )
    .await;
    connect(&client).await;
    let online = wait_connected(&client, Duration::from_secs(10)).await;
    let (current, other) = if online.access_point_id.as_deref() == Some("development") {
        (&a, &b)
    } else {
        (&b, &a)
    };
    current.relay.freeze();
    let changed_at = Instant::now();
    client
        .notify_network_change(NetworkChangeEvent {
            available: true,
            interface: Some("cellular".into()),
            fingerprint_hint: Some("460-01".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_until(
        "moved to the other access point",
        Duration::from_secs(15),
        || async {
            client.connection_status().await.access_point_id.as_deref() == Some(other.id.as_str())
        },
    )
    .await;
    assert!(
        changed_at.elapsed() <= Duration::from_secs(12),
        "the probe window (8 s) finds the dead link: {:?}",
        changed_at.elapsed()
    );
    let _ = client.logout().await;
}

/// 没有网络：链路挂起、发件箱不派发（不白耗重试）；网络回来、连接验活通过后消息发出。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_network_suspends_and_the_outbox_waits() {
    let (a, ledger, root) = one_access_point("suspend").await;
    let client = lab_client(development_access(&[(&a.id, a.ws_url())]), &root, |_| {}).await;
    let (observed, _subscriptions) = Observed::record(&client);
    connect(&client).await;
    wait_connected(&client, Duration::from_secs(10)).await;

    client
        .notify_network_change(NetworkChangeEvent {
            available: false,
            ..Default::default()
        })
        .await
        .unwrap();
    wait_phase(
        &client,
        ConnectionPhase::WaitingForNetwork,
        Duration::from_secs(3),
    )
    .await;
    let queued = send_text(&client, "queued while offline").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        ledger.lock().unwrap().first_arrivals.is_empty(),
        "the outbox waits"
    );

    client
        .notify_network_change(NetworkChangeEvent {
            available: true,
            ..Default::default()
        })
        .await
        .unwrap();
    wait_until(
        "sent after the probe passes",
        Duration::from_secs(15),
        || async { observed.accepted_distinct().contains(&queued) },
    )
    .await;
    assert_eq!(a.relay.accepted(), 1, "the surviving link was kept");
    let _ = client.logout().await;
}

/// 平台在 init 之后、登录之前就首报离线，之后对相同状态去重、不再重报：登录新建的链路要沿用它——
/// 登录成功、显示等网络、不去建连、发件箱暂停；网络恢复后正常上线，排队的消息发出且只发一次。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_reported_before_login_carries_into_the_new_link() {
    let (a, ledger, root) = one_access_point("offline-before-login").await;
    let client = IMClient::new();
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(root.display().to_string()),
                access: Some(development_access(&[(&a.id, a.ws_url())])),
                ..Default::default()
            }),
        )
        .await
        .expect("init");
    client
        .notify_network_change(NetworkChangeEvent {
            available: false,
            interface: Some("wifi".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    let login = client
        .login(
            USER_ID,
            Some("token"),
            LoginDbKind::IndexedDb(in_memory_im_provider()),
            |_, _| {},
        )
        .await;
    // 链路自己会恢复（等网络）：登录成功、会话保留，在不在线只看 connection://status。
    if let Err(error) = login {
        panic!("offline login keeps the session and succeeds: {error}");
    }
    let (observed, _subscriptions) = Observed::record(&client);
    wait_phase(
        &client,
        ConnectionPhase::WaitingForNetwork,
        Duration::from_secs(3),
    )
    .await;
    let queued = send_text(&client, "queued before the network came back").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(a.relay.accepted(), 0, "no connection is attempted offline");
    assert!(
        ledger.lock().unwrap().first_arrivals.is_empty(),
        "the outbox waits"
    );

    client
        .notify_network_change(NetworkChangeEvent {
            available: true,
            interface: Some("wifi".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    wait_until(
        "online and the queued message sent",
        Duration::from_secs(15),
        || async { observed.accepted_distinct().contains(&queued) },
    )
    .await;
    wait_phase(&client, ConnectionPhase::Online, Duration::from_secs(10)).await;
    {
        let ledger = ledger.lock().unwrap();
        assert_eq!(ledger.first_arrivals, vec![queued]);
        assert_eq!(ledger.received_total, 1);
    }
    let _ = client.logout().await;
}

/// SDK 托管令牌（配了 `auth`）的离线冷启动：登录时不先签发——离线时登录成功、链路照常起来、显示
/// 等网络，不发签发请求；联网后先签发、再握手上线，排队的消息恰好送达一次。签发请求经 HttpRouter 走到
/// 接入点的 Core API（开发模式即 coreApiUrl）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_managed_tokens_are_issued_by_the_link_once_the_network_is_back() {
    let (a, ledger, root) = one_access_point("sdk-managed-offline").await;
    let issuer = TokenIssuer::start().await;
    let mut access = development_access(&[(&a.id, a.ws_url())]);
    if let Some(endpoints) = access.development_endpoints.as_mut() {
        endpoints.core_api_url = Some(issuer.url.clone());
    }
    let client = IMClient::new();
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(root.display().to_string()),
                access: Some(access),
                // SDK 托管：签发走 Core API（开发模式即 coreApiUrl）上的标准路径。
                auth: Some(SdkAuthConfig::default()),
                ..Default::default()
            }),
        )
        .await
        .expect("init");
    client
        .notify_network_change(NetworkChangeEvent {
            available: false,
            ..Default::default()
        })
        .await
        .unwrap();

    let login = client
        .login(
            USER_ID,
            None,
            LoginDbKind::IndexedDb(in_memory_im_provider()),
            |_, _| {},
        )
        .await;
    // 链路自己会恢复（等网络）：登录成功、会话保留，在不在线只看 connection://status。
    if let Err(error) = login {
        panic!("offline login keeps the session and succeeds: {error}");
    }
    let (observed, _subscriptions) = Observed::record(&client);
    wait_phase(
        &client,
        ConnectionPhase::WaitingForNetwork,
        Duration::from_secs(3),
    )
    .await;
    let queued = send_text(&client, "queued before the token was issued").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(issuer.issued_at().is_empty(), "nothing is issued offline");
    assert_eq!(a.relay.accepted(), 0, "no connection is attempted offline");

    client
        .notify_network_change(NetworkChangeEvent {
            available: true,
            ..Default::default()
        })
        .await
        .unwrap();
    wait_until(
        "online and the queued message sent",
        Duration::from_secs(15),
        || async { observed.accepted_distinct().contains(&queued) },
    )
    .await;
    let issued = issuer.issued_at();
    assert_eq!(issued.len(), 1, "one token for the session");
    assert!(
        issued[0] <= a.relay.accepted_at()[0],
        "the token is issued before the handshake"
    );
    assert!(client.access_token().await.is_some());
    {
        let ledger = ledger.lock().unwrap();
        assert_eq!(ledger.first_arrivals, vec![queued]);
        assert_eq!(ledger.received_total, 1);
    }
    let _ = client.logout().await;
}

/// 心跳只由接入策略决定：清单给了非默认心跳，建连后实际用的就是它；切到后台用后台间隔，回前台恢复。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_connection_heartbeats_on_the_policy_rhythm() {
    let (a, _ledger, root) = one_access_point("heartbeat-policy").await;
    let mut access = development_access(&[(&a.id, a.ws_url())]);
    if let Some(endpoints) = access.development_endpoints.as_mut() {
        let mut policy = drill_policy();
        policy["heartbeat"] = serde_json::json!({
            "intervalSec": 45,
            "timeoutSec": 150,
            "backgroundIntervalSec": 300
        });
        endpoints.client_policy = Some(policy);
    }
    let client = lab_client(access, &root, |_| {}).await;
    connect(&client).await;
    wait_connected(&client, Duration::from_secs(10)).await;
    assert_eq!(
        client.heartbeat_effective_interval().await.unwrap(),
        Some(Duration::from_secs(45))
    );

    client
        .set_app_state(HeartbeatAppState::Background)
        .await
        .unwrap();
    assert_eq!(
        client.heartbeat_effective_interval().await.unwrap(),
        Some(Duration::from_secs(300)),
        "the background interval comes from the policy, not the transport default"
    );
    client
        .set_app_state(HeartbeatAppState::Foreground)
        .await
        .unwrap();
    assert_eq!(
        client.heartbeat_effective_interval().await.unwrap(),
        Some(Duration::from_secs(45))
    );
    let _ = client.logout().await;
}

/// CONNECT_ACK 的诊断字段（网关 ID、服务端报的接入点、清单 serial、网关心跳间隔）出现在 `access.get_status`
/// 与 `access.export_diagnostics` 里；服务端报的接入点和选的不一致时告警、记进健康簿，但不断连。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_ack_fields_show_up_in_the_access_diagnostics() {
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let a = AccessPoint::start(
        "development",
        ledger,
        ServerOptions {
            gateway_id: Some("gw-lab-7"),
            reported_ap_id: Some("ap-elsewhere"),
            heartbeat_interval: Some(Duration::from_secs(25)),
            ..Default::default()
        },
    )
    .await;
    let root = data_root("link-lifecycle", "connect-ack");
    let client = lab_client(development_access(&[(&a.id, a.ws_url())]), &root, |_| {}).await;
    connect(&client).await;
    wait_phase(&client, ConnectionPhase::Online, Duration::from_secs(10)).await;

    let status = client.access_status().await.unwrap();
    let ack = status
        .connect_ack
        .expect("connected: the CONNECT_ACK fields are shown");
    assert_eq!(ack.gateway_id.as_deref(), Some("gw-lab-7"));
    assert_eq!(ack.access_point_id.as_deref(), Some("ap-elsewhere"));
    assert!(ack.access_point_mismatch);
    assert_eq!(
        ack.access_serial,
        Some(0),
        "the gateway knows no manifest yet"
    );
    assert_eq!(ack.heartbeat_interval_hint_sec, Some(25));

    let diagnostics = client.export_access_diagnostics().await.unwrap();
    assert_eq!(diagnostics["status"]["connectAck"]["gatewayId"], "gw-lab-7");
    assert_eq!(
        diagnostics["status"]["connectAck"]["accessPointId"],
        "ap-elsewhere"
    );
    assert!(
        diagnostics["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|warning| warning["kind"] == "accessPointMismatch"
                && warning["expected"] == "development"
                && warning["reported"] == "ap-elsewhere"),
        "{diagnostics}"
    );
    // 不断连：仍在线，接入点还是选的那个。
    let now = client.connection_status().await;
    assert_eq!(now.phase, ConnectionPhase::Online);
    assert_eq!(now.access_point_id.as_deref(), Some("development"));
    let _ = client.logout().await;
}

/// CONNECT 带上宿主给的应用信息与链路字段；每次重连的连接代数递增。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_carries_the_app_and_the_connection_epoch() {
    let (a, _ledger, root) = one_access_point("connect-metadata").await;
    let client = lab_client(
        development_access(&[(&a.id, a.ws_url())]),
        &root,
        |config| {
            config.app = SdkAppConfig {
                version: Some("3.2.1".into()),
                build: Some(30201),
                distribution: Some("direct".into()),
                channel: Some("stable".into()),
                ..Default::default()
            };
        },
    )
    .await;
    connect(&client).await;
    wait_connected(&client, Duration::from_secs(10)).await;
    let device = a
        .gate
        .last_device
        .lock()
        .unwrap()
        .clone()
        .expect("device info");
    assert_eq!(device.app_version.as_deref(), Some("3.2.1"));
    let metadata = a.gate.last_metadata.lock().unwrap().clone();
    assert_eq!(metadata.get("app_build").map(String::as_str), Some("30201"));
    assert_eq!(
        metadata.get("distribution").map(String::as_str),
        Some("direct")
    );
    let first_epoch: u64 = metadata
        .get("connection_epoch")
        .and_then(|epoch| epoch.parse().ok())
        .expect("connection_epoch");

    a.relay.cut().await;
    wait_until("reconnected", Duration::from_secs(10), || async {
        a.gate.admitted.load(Ordering::SeqCst) >= 2
    })
    .await;
    let second_epoch: u64 = a
        .gate
        .last_metadata
        .lock()
        .unwrap()
        .get("connection_epoch")
        .and_then(|epoch| epoch.parse().ok())
        .expect("connection_epoch");
    assert!(second_epoch > first_epoch);
    let _ = client.logout().await;
}
