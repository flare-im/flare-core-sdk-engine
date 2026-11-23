#![cfg(all(feature = "quic", not(target_arch = "wasm32")))]

mod link_lab;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use flare_im_core_sdk::prelude::*;
use link_lab::{
    AccessPoint, Ledger, Observed, ServerOptions, connect, data_root, development_access,
    lab_client, send_text, wait_until,
};

async fn connected_over(client: &IMClient, protocol: &str) {
    wait_until(protocol, Duration::from_secs(15), || async {
        let status = client.connection_status().await;
        matches!(
            status.phase,
            ConnectionPhase::Syncing | ConnectionPhase::Online
        ) && status.protocol.as_deref() == Some(protocol)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_protocol_selection_reconnects_without_replacing_the_session() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let port = {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap().port()
    };
    let edge = AccessPoint::start(
        "development",
        Arc::new(Mutex::new(Ledger::default())),
        ServerOptions {
            quic_port: Some(port),
            ..Default::default()
        },
    )
    .await;
    let mut access = development_access(&[(&edge.id, edge.ws_url())]);
    access.development_endpoints.as_mut().unwrap().quic_url =
        Some(format!("quic://127.0.0.1:{port}"));
    let client = lab_client(access, &data_root("link-protocol-mode", "switch"), |_| {}).await;
    let (observed, _subscriptions) = Observed::record(&client);

    client.set_protocol_mode("ws").await.unwrap();
    connect(&client).await;
    println!("initial WebSocket connection");
    connected_over(&client, "ws").await;
    let session = client.session_generation().await;

    client.set_protocol_mode("quic").await.unwrap();
    println!("switch to QUIC");
    connected_over(&client, "quic").await;
    let sent = send_text(&client, "message after switching to QUIC").await;
    wait_until(
        "QUIC send acknowledged",
        Duration::from_secs(15),
        || async { observed.accepted_distinct().contains(&sent) },
    )
    .await;

    client.set_protocol_mode("ws").await.unwrap();
    println!("switch back to WebSocket");
    connected_over(&client, "ws").await;

    // With WebSocket no longer reachable, a new race can still connect via QUIC.
    edge.relay.take_down().await;
    client.set_protocol_mode("race").await.unwrap();
    println!("race with WebSocket down");
    connected_over(&client, "quic").await;
    assert_eq!(client.session_generation().await, session);
    client.disconnect().await.unwrap();
    edge.server.stop().await.unwrap();
}
