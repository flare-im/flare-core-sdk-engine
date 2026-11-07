use super::{EventBus, PublishOutcome, RecoverableRwLock, ReportingGuard};
use crate::kernel::SyncRunContext;
use crate::kernel::event::{
    AccessChanged, AccessChanges, ConnectionEvent, ConnectionEventType, ConnectionPhase,
    ConnectionStatus, CustomEventDefinition, MessageEvent, MessageEventType, SdkEvent, SyncNotify,
};
use crate::kernel::fsm::SyncState;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc::{self, error::TryRecvError};

/// 测试用的连接状态事件（上线）。
fn online() -> SdkEvent {
    SdkEvent::Connection(ConnectionEvent::Status(ConnectionStatus {
        phase: ConnectionPhase::Online,
        ..ConnectionStatus::idle(false)
    }))
}

/// 测试用的连接状态事件，`reason` 用来区分先后。
fn status_with_reason(reason: impl Into<String>) -> SdkEvent {
    SdkEvent::Connection(ConnectionEvent::Status(ConnectionStatus {
        phase: ConnectionPhase::Reconnecting,
        reason: Some(reason.into()),
        ..ConnectionStatus::idle(false)
    }))
}

#[tokio::test]
async fn publish_drops_silent_sync_events_before_raw_subscribers() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe_raw();

    let outcome = bus.publish(SdkEvent::Sync(SyncNotify::Started {
        run: SyncRunContext::silent_gap_repair(),
    }));

    assert_eq!(outcome, PublishOutcome::DroppedSilentSync);
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn publish_keeps_user_visible_sync_events() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe_raw();

    let outcome = bus.publish(SdkEvent::Sync(SyncNotify::Started {
        run: SyncRunContext::initial_login(),
    }));

    assert!(matches!(
        outcome,
        PublishOutcome::Published { receiver_count } if receiver_count >= 1
    ));
    let received = rx.try_recv().expect("user visible sync event emitted");
    assert!(matches!(
        received,
        SdkEvent::Sync(SyncNotify::Started { .. })
    ));
}

#[tokio::test]
async fn publish_keeps_warm_start_calibration_sync_status_events() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe_raw();

    let outcome = bus.publish(SdkEvent::Sync(SyncNotify::Started {
        run: SyncRunContext::warm_start(),
    }));

    assert!(matches!(
        outcome,
        PublishOutcome::Published { receiver_count } if receiver_count >= 1
    ));
    let received = rx
        .try_recv()
        .expect("warm-start calibration sync status should be emitted");
    assert!(matches!(
        received,
        SdkEvent::Sync(SyncNotify::Started { .. })
    ));
}

#[tokio::test]
async fn publish_keeps_warm_start_calibration_sync_state_events() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe_raw();

    let outcome = bus.publish(SdkEvent::Sync(SyncNotify::StateChanged {
        run: SyncRunContext::warm_start(),
        state: SyncState::CatchingUp,
    }));

    assert!(matches!(
        outcome,
        PublishOutcome::Published { receiver_count } if receiver_count >= 1
    ));
    let received = rx
        .try_recv()
        .expect("warm-start calibration sync state should be emitted");
    assert!(matches!(
        received,
        SdkEvent::Sync(SyncNotify::StateChanged {
            state: SyncState::CatchingUp,
            ..
        })
    ));
}

#[tokio::test]
async fn raw_subscribers_share_event_and_cached_json() {
    let bus = EventBus::new();
    let mut first_rx = bus.subscribe_shared_raw();
    let mut second_rx = bus.subscribe_shared_raw();

    let outcome = bus.publish(online());

    assert!(matches!(
        outcome,
        PublishOutcome::Published { receiver_count } if receiver_count == 2
    ));
    let first = first_rx
        .try_recv()
        .expect("first raw subscriber receives event");
    let second = second_rx
        .try_recv()
        .expect("second raw subscriber receives event");
    assert!(Arc::ptr_eq(&first, &second));

    let serialize_count = AtomicU64::new(0);
    let first_json = first.cached_json(|_| {
        serialize_count.fetch_add(1, Ordering::Relaxed);
        "{}".to_string()
    });
    let second_json = second.cached_json(|_| {
        serialize_count.fetch_add(1, Ordering::Relaxed);
        "{\"unexpected\":true}".to_string()
    });

    assert!(Arc::ptr_eq(&first_json, &second_json));
    assert_eq!(serialize_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn raw_subscriber_queue_honors_capacity_hint() {
    let bus = EventBus::with_capacity(1);
    let mut rx = bus.subscribe_raw();

    let first_outcome = bus.publish(SdkEvent::Sync(SyncNotify::Started {
        run: SyncRunContext::initial_login(),
    }));
    let second_outcome = bus.publish(SdkEvent::Sync(SyncNotify::Finished {
        run: SyncRunContext::initial_login(),
        phase: crate::kernel::event::SyncPhase::Init,
    }));

    assert!(matches!(
        first_outcome,
        PublishOutcome::Published { receiver_count } if receiver_count == 1
    ));
    assert_eq!(second_outcome, PublishOutcome::NoReceivers);

    let resync = rx.try_recv().expect("overflow emits resync marker first");
    assert!(matches!(
        resync,
        SdkEvent::Sync(SyncNotify::ResyncNeeded {
            scope,
            reason,
            dropped_events: 1,
        }) if scope == "global" && reason == "event_queue_lagged"
    ));
    let first = rx.try_recv().expect("first event retained");
    assert!(matches!(first, SdkEvent::Sync(SyncNotify::Started { .. })));
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn raw_subscriber_queue_drops_overflow_for_slow_consumers() {
    let bus = EventBus::with_capacity(2);
    let mut rx = bus.subscribe_raw();
    let total = 10_000usize;

    for i in 0..total {
        bus.publish(status_with_reason(format!("network-{i}")));
    }

    let resync = rx
        .try_recv()
        .expect("resync marker emitted before retained events");
    assert!(matches!(
        resync,
        SdkEvent::Sync(SyncNotify::ResyncNeeded {
            dropped_events,
            ..
        }) if dropped_events == (total - 2) as u64
    ));
    let received = rx.try_recv().expect("first burst event retained");
    assert!(matches!(
        received,
        SdkEvent::Connection(ConnectionEvent::Status(ConnectionStatus {
            reason: Some(reason),
            ..
        }))
            if reason == "network-0"
    ));
    let second = rx.try_recv().expect("second burst event retained");
    assert!(matches!(
        second,
        SdkEvent::Connection(ConnectionEvent::Status(ConnectionStatus {
            reason: Some(reason),
            ..
        }))
            if reason == "network-1"
    ));
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn shared_raw_subscriber_queue_emits_resync_after_overflow() {
    let bus = EventBus::with_capacity(1);
    let mut rx = bus.subscribe_shared_raw();

    bus.publish(online());
    bus.publish(status_with_reason("network".to_string()));

    let resync = rx
        .try_recv()
        .expect("shared raw overflow emits resync marker first");
    assert!(matches!(
        resync.event(),
        SdkEvent::Sync(SyncNotify::ResyncNeeded {
            scope,
            reason,
            dropped_events: 1,
        }) if scope == "global" && reason == "event_queue_lagged"
    ));
    let retained = rx.try_recv().expect("first shared event retained");
    assert!(matches!(
        retained.event(),
        SdkEvent::Connection(ConnectionEvent::Status(_))
    ));
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn filtered_receiver_gets_resync_marker_after_own_queue_overflows() {
    let bus = EventBus::with_capacity(1);
    let mut rx = bus.subscribe_event_type(ConnectionEventType::Status.into());

    bus.publish(online());
    bus.publish(online());

    let resync = rx.try_recv().expect("resync marker emitted first");
    assert!(matches!(
        resync,
        SdkEvent::Sync(SyncNotify::ResyncNeeded {
            dropped_events: 1,
            ..
        })
    ));
    assert!(matches!(
        rx.try_recv().expect("retained matching event follows"),
        SdkEvent::Connection(ConnectionEvent::Status(_))
    ));
}

#[tokio::test]
async fn filtered_subscribers_are_isolated_under_burst() {
    let bus = EventBus::new();
    let mut connection_rx = bus.subscribe_event_type(ConnectionEventType::Status.into());
    let mut custom_rx = bus.subscribe_event_type(MessageEventType::Custom.into());
    let total = 1_000usize;

    for i in 0..total {
        bus.publish(online());
        bus.publish(SdkEvent::Message(MessageEvent::Custom {
            conversation_id: "c1".into(),
            event: CustomEventDefinition::new("app.iso", format!("event_{i}")).build(vec![]),
        }));
    }

    for _ in 0..total {
        assert!(matches!(
            connection_rx.try_recv().expect("connection event retained"),
            SdkEvent::Connection(ConnectionEvent::Status(_))
        ));
    }
    assert!(matches!(connection_rx.try_recv(), Err(TryRecvError::Empty)));

    for i in 0..total {
        assert!(matches!(
            custom_rx.try_recv().expect("custom event retained"),
            SdkEvent::Message(MessageEvent::Custom { event, .. })
                if event.name == format!("event_{i}")
        ));
    }
    assert!(matches!(custom_rx.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn closed_raw_subscriber_is_pruned_on_next_publish() {
    let bus = EventBus::new();
    let rx = bus.subscribe_raw();

    assert_eq!(bus.subscribers.safe_read("event_bus").len(), 1);
    drop(rx);
    bus.publish(online());
    assert_eq!(bus.subscribers.safe_read("event_bus").len(), 0);
}

#[test]
fn dropping_subscription_removes_registered_callback() {
    let bus = EventBus::new();
    let subscription = bus.on_connection_status(|_| {});

    assert_eq!(bus.on_connection_status.safe_read("event_bus").len(), 1);
    drop(subscription);
    assert_eq!(bus.on_connection_status.safe_read("event_bus").len(), 0);
}

#[test]
fn callback_dispatch_gates_track_subscription_lifecycle() {
    let bus = EventBus::new();

    assert!(!bus.has_typed_callbacks());
    assert!(!bus.has_route_callbacks());
    assert!(!bus.has_any_callbacks());

    let typed = bus.on_connection_status(|_| {});
    let route = bus.on_event_type(ConnectionEventType::Status.into(), |_| {});
    let any = bus.on_any(|_| {});

    assert!(bus.has_typed_callbacks());
    assert!(bus.has_route_callbacks());
    assert!(bus.has_any_callbacks());
    assert_eq!(bus.typed_callback_count.load(Ordering::Acquire), 1);
    assert_eq!(bus.route_callback_count.load(Ordering::Acquire), 1);
    assert_eq!(bus.any_callback_count.load(Ordering::Acquire), 1);

    drop(typed);
    drop(route);
    drop(any);

    assert!(!bus.has_typed_callbacks());
    assert!(!bus.has_route_callbacks());
    assert!(!bus.has_any_callbacks());
}

#[tokio::test]
async fn raw_only_publish_keeps_callback_gates_closed() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe_raw();

    let outcome = bus.publish(online());

    assert!(matches!(
        outcome,
        PublishOutcome::Published { receiver_count } if receiver_count == 1
    ));
    assert!(!bus.has_typed_callbacks());
    assert!(!bus.has_route_callbacks());
    assert!(!bus.has_any_callbacks());
    assert!(matches!(
        rx.try_recv().expect("raw subscriber receives event"),
        SdkEvent::Connection(ConnectionEvent::Status(_))
    ));
}

#[test]
fn dropped_route_and_any_callbacks_leave_no_receivers() {
    let bus = EventBus::new();
    let route = bus.on_event_type(ConnectionEventType::Status.into(), |_| {});
    let any = bus.on_any(|_| {});

    assert!(matches!(
        bus.publish(online()),
        PublishOutcome::Published { receiver_count } if receiver_count == 2
    ));

    drop(route);
    drop(any);

    assert_eq!(bus.publish(online()), PublishOutcome::NoReceivers);
}

#[tokio::test]
async fn connected_callback_replays_last_state_after_registration() {
    let bus = EventBus::new();
    bus.publish(online());

    let (tx, mut rx) = mpsc::channel(1);
    let _subscription = bus.on_connection_status(move |_| {
        let _ = tx.try_send(());
    });

    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("connected replay callback invoked")
        .expect("connected replay value");
}

#[test]
fn dropping_filtered_subscription_removes_registered_route() {
    let bus = EventBus::new();
    let subscription = bus.on_event_type(ConnectionEventType::Status.into(), |_| {});

    assert_eq!(bus.routes.safe_read("event_bus").len(), 1);
    drop(subscription);
    assert_eq!(bus.routes.safe_read("event_bus").len(), 0);
}

#[tokio::test]
async fn filtered_receiver_skips_unmatched_events() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe_event_type(ConnectionEventType::Status.into());

    bus.publish(SdkEvent::Connection(ConnectionEvent::AccessChanged(
        AccessChanged {
            serial: 1,
            source: "development".into(),
            stale: false,
            development_mode: true,
            changes: AccessChanges {
                access_points_added: vec!["development".into()],
                access_points_removed: vec![],
                policy_changed: true,
            },
        },
    )));
    bus.publish(online());

    let received = rx.try_recv().expect("status event emitted");
    assert!(matches!(
        received,
        SdkEvent::Connection(ConnectionEvent::Status(_))
    ));
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[tokio::test]
async fn event_type_routes_fan_out_to_multiple_handlers() {
    let bus = EventBus::new();
    let (tx, mut rx) = mpsc::unbounded_channel();

    let tx1 = tx.clone();
    let _sub1 = bus.on_event_type(ConnectionEventType::Status.into(), move |_| {
        tx1.send("first").expect("first route sends");
    });
    let _sub2 = bus.on_event_type(ConnectionEventType::Status.into(), move |_| {
        tx.send("second").expect("second route sends");
    });

    bus.publish(online());

    let mut seen = vec![
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("first route invoked")
            .expect("first route value"),
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("second route invoked")
            .expect("second route value"),
    ];
    seen.sort_unstable();
    assert_eq!(seen, vec!["first", "second"]);
}

#[tokio::test]
async fn callbacks_fire_in_publish_order() {
    let bus = EventBus::new();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _sub = bus.on_event_type(ConnectionEventType::Status.into(), move |ev| {
        if let SdkEvent::Connection(ConnectionEvent::Status(ConnectionStatus {
            reason: Some(reason),
            ..
        })) = ev.as_ref()
        {
            let _ = tx.send(reason.clone());
        }
    });

    const N: u32 = 64;
    for i in 0..N {
        bus.publish(status_with_reason(i.to_string()));
    }

    let mut got = Vec::with_capacity(N as usize);
    for _ in 0..N {
        let reason = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("callback invoked within window")
            .expect("callback value");
        got.push(reason);
    }
    let expected: Vec<String> = (0..N).map(|i| i.to_string()).collect();
    assert_eq!(
        got, expected,
        "single dispatch thread must preserve publish order"
    );
}

#[tokio::test]
async fn custom_event_definition_builds_and_filters_user_event() {
    let bus = EventBus::new();
    let definition = CustomEventDefinition::new("app.orders", "order_paid").with_version("v1");
    let mut rx = bus.subscribe_event_type(definition.event_type());

    bus.publish(SdkEvent::Message(MessageEvent::Custom {
        conversation_id: "c1".into(),
        event: CustomEventDefinition::new("app.orders", "order_cancelled")
            .with_version("v1")
            .build(Vec::new()),
    }));
    bus.publish(SdkEvent::Message(MessageEvent::Custom {
        conversation_id: "c1".into(),
        event: definition.build(b"{\"order_id\":\"o1\"}".to_vec()),
    }));

    let received = rx.try_recv().expect("custom event emitted");
    assert!(matches!(
        received,
        SdkEvent::Message(MessageEvent::Custom { event, .. })
            if event.namespace == "app.orders"
                && event.name == "order_paid"
                && event.version == "v1"
    ));
}

#[tokio::test]
async fn broad_custom_event_filter_matches_all_custom_events() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe_event_type(MessageEventType::Custom.into());

    bus.publish(online());
    bus.publish(SdkEvent::Message(MessageEvent::Custom {
        conversation_id: "c1".into(),
        event: CustomEventDefinition::new("app.any", "anything").build(Vec::new()),
    }));

    let received = rx.try_recv().expect("custom event emitted");
    assert!(matches!(
        received,
        SdkEvent::Message(MessageEvent::Custom { .. })
    ));
}

/// 补发路径 panic 时 `reporting_callback_drops` 门必须仍被释放——否则门永久占用，
/// 此后所有回调丢弃都不再向应用层通报（静默降级，比丢弃本身更难排查）。
#[test]
fn reporting_guard_releases_gate_even_on_panic() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicBool, Ordering};

    let gate = AtomicBool::new(false);

    assert!(!gate.swap(true, Ordering::AcqRel), "gate starts free");
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _guard = ReportingGuard(&gate);
        panic!("resync publish blew up");
    }));
    assert!(result.is_err(), "the panic must have propagated");
    assert!(
        !gate.load(Ordering::Acquire),
        "gate must be released so later drops are still reported"
    );
}
