#![cfg(not(target_arch = "wasm32"))]

mod link_lab;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flare_im_core_sdk::client::config::SdkAuthConfig;
use flare_im_core_sdk::prelude::*;

use link_lab::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_endpoints_connect_and_replace_without_rebuilding_the_session() {
    let deadline = Duration::from_secs(15);
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let first = AccessPoint::start("first-gateway", ledger.clone(), ServerOptions::default()).await;
    let next = AccessPoint::start("next-gateway", ledger.clone(), ServerOptions::default()).await;
    let issuer = TokenIssuer::start().await;
    let endpoints = CoreEndpoints {
        base_url: issuer.url.clone(),
        websocket_url: first.ws_url(),
        quic_url: None,
        media_base_url: None,
    };
    let client = IMClient::new();
    client
        .init(
            Some("development".into()),
            Some(SdkConfigOverlay {
                endpoints: Some(endpoints.clone()),
                data_url: Some(data_root("core-endpoints", "replace").display().to_string()),
                tenant_id: Some("tenant-a".into()),
                device_id: Some("direct-device".into()),
                auth: Some(SdkAuthConfig::default()),
                connect_timeout_secs: Some(3),
                ack_timeout_secs: Some(60),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert_eq!(client.connection_status().await.endpoint_revision, Some(1));
    client
        .prepare(USER_ID, LoginDbKind::IndexedDb(in_memory_im_provider()))
        .await
        .unwrap();
    let (observed, _subscriptions) = Observed::record(&client);
    client.connect(USER_ID, None).await.unwrap();
    assert_eq!(
        wait_connected(&client, deadline).await.endpoint_revision,
        Some(1)
    );
    assert_eq!(issuer.issued_at().len(), 1);
    let session = client.session_generation().await;
    let first_message = send_text(&client, "before replacing the endpoints").await;
    wait_until("first message accepted", deadline, || async {
        observed.accepted_distinct().contains(&first_message)
    })
    .await;

    let media = client.media_http_client().await.unwrap();
    assert_eq!(
        media.url_for("/test").await.unwrap(),
        format!("{}/test", issuer.url)
    );
    let with_media = CoreEndpoints {
        media_base_url: Some(format!("{}/media", issuer.url)),
        ..endpoints.clone()
    };
    assert_eq!(
        client.replace_endpoints(with_media.clone()).await.unwrap(),
        EndpointUpdate {
            revision: 2,
            changed: true
        }
    );
    assert_eq!(
        media.url_for("/test").await.unwrap(),
        format!("{}/media/test", issuer.url)
    );
    assert_eq!(first.gate.admitted.load(Ordering::SeqCst), 1);
    assert_eq!(
        client.replace_endpoints(with_media.clone()).await.unwrap(),
        EndpointUpdate {
            revision: 2,
            changed: false
        }
    );
    assert!(
        client
            .replace_endpoints(CoreEndpoints {
                websocket_url: "https://invalid.example".into(),
                ..with_media.clone()
            })
            .await
            .is_err()
    );
    assert_eq!(client.connection_status().await.endpoint_revision, Some(2));

    first.relay.take_down().await;
    client
        .notify_network_change(NetworkChangeEvent::default())
        .await
        .unwrap();
    let queued_message = send_text(&client, "queued while changing endpoints").await;
    let updated = client
        .replace_endpoints(CoreEndpoints {
            websocket_url: next.ws_url(),
            quic_url: Some("quic://127.0.0.1:9".into()),
            ..with_media
        })
        .await
        .unwrap();
    assert_eq!(
        updated,
        EndpointUpdate {
            revision: 3,
            changed: true
        }
    );
    let waiting = wait_phase(&client, ConnectionPhase::WaitingForNetwork, deadline).await;
    assert_eq!(waiting.endpoint_revision, Some(3));
    assert_eq!(next.gate.attempts.load(Ordering::SeqCst), 0);
    client
        .notify_network_change(NetworkChangeEvent {
            available: true,
            ..Default::default()
        })
        .await
        .unwrap();
    let status = wait_connected(&client, deadline).await;
    assert_eq!(status.endpoint_revision, Some(3));
    assert_eq!(status.protocol.as_deref(), Some("ws"));
    wait_until("queued message accepted", deadline, || async {
        observed.accepted_distinct().contains(&queued_message)
    })
    .await;
    assert_eq!(next.gate.admitted.load(Ordering::SeqCst), 1);
    assert_eq!(client.session_generation().await, session);
    assert_eq!(
        ledger.lock().unwrap().first_arrivals,
        [first_message, queued_message]
    );
    client.logout().await.unwrap();
}
