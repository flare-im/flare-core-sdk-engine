use std::sync::Arc;

use flare_im_core_sdk::prelude::*;

#[tokio::test]
async fn runtime_health_reports_event_bus_backpressure_metrics() {
    let external_metrics = Arc::new(InMemoryMetricsSink::default());
    let client = IMClient::builder()
        .config(
            SdkConfig::builder()
                .enable_metrics(true)
                .event_bus_capacity(1)
                .build(),
        )
        .stores(in_memory_im_provider())
        .metrics_sink_arc(external_metrics.clone())
        .build()
        .expect("client should build with in-memory stores");

    let bus = client.bus().await.expect("event bus should be available");
    let _slow_subscriber = bus.subscribe_raw();

    for attempt in 0..4 {
        let _ = bus.publish(SdkEvent::Connection(ConnectionEvent::Status(
            ConnectionStatus {
                attempt,
                ..ConnectionStatus::idle(false)
            },
        )));
    }

    let snapshot = client.runtime_health_snapshot().await;
    let local_drop_count = snapshot
        .metrics
        .counters
        .get("event.raw_subscriber_dropped_total")
        .copied()
        .unwrap_or_default();
    let exported_drop_count = external_metrics
        .snapshot()
        .counters
        .get("event.raw_subscriber_dropped_total")
        .copied()
        .unwrap_or_default();

    assert!(snapshot.metrics_enabled);
    assert!(snapshot.raw_subscriber_dropped_total >= local_drop_count);
    assert!(local_drop_count > 0);
    assert_eq!(local_drop_count, exported_drop_count);
}
