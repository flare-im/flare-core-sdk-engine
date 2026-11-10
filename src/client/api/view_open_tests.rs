//! Opening must resolve an unknown empty latest range without waiting for manual history paging.

use std::sync::atomic::Ordering;

use flare_proto::common::{
    SingleConversationSyncRes, SyncSliceItem, SyncTombstoneItem,
    sync_slice_item::Payload as ItemPayload,
};

use super::harness::{CONVERSATION, Harness, Req, Setup, proto, row, seqs};
use crate::domain::{SeqRange, SyncCursorVo};
use crate::kernel::event::{MessageEvent, SdkEvent};
use crate::model::{CloseViewRequest, OpenTimelineViewRequest};
use crate::shared::error::{ErrorCode, FlareError};

async fn cached_gap() -> Harness {
    Harness::new(Setup {
        page_size: 40,
        max_seq: 17,
        visible_after_seq: 0,
        server: &[[1, 4], [7, 7], [10, 10], [13, 13], [15, 15], [17, 17]],
        local: &[[1, 4], [7, 7], [10, 10], [13, 13], [15, 15], [17, 17]],
        coverage: &[[1, 16], [18, 18]],
    })
    .await
}

fn proof_page(seq: u64) -> SingleConversationSyncRes {
    SingleConversationSyncRes {
        conversation_id: CONVERSATION.into(),
        items: vec![SyncSliceItem {
            conversation_seq: seq,
            payload: Some(ItemPayload::Tombstone(SyncTombstoneItem {
                tombstone_id: format!("{CONVERSATION}:{seq}"),
                reason: None,
            })),
            ..Default::default()
        }],
        has_more: true,
        ..Default::default()
    }
}

fn message_page(seq: u64) -> SingleConversationSyncRes {
    SingleConversationSyncRes {
        conversation_id: CONVERSATION.into(),
        items: vec![SyncSliceItem {
            conversation_seq: seq,
            payload: Some(ItemPayload::Message(proto(seq))),
            ..Default::default()
        }],
        has_more: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn opening_repairs_the_cached_message_below_an_empty_event_only_latest_range() {
    let h = cached_gap().await;
    h.stores
        .cursors
        .save_conversation_cursor(&SyncCursorVo {
            user_id: "me".into(),
            conversation_id: CONVERSATION.into(),
            last_seq: 18,
            synced_at: 1,
        })
        .await
        .unwrap();

    h.open().await;
    let expected = vec![1, 2, 3, 4, 7, 10, 13, 15, 17];
    assert_eq!(h.window().await, (expected.clone(), false));
    assert_eq!(h.server.take_requests(), vec![Req::Before(18, 40)]);
    assert_eq!(h.coverage().await, vec![[1, 18]]);
    assert_eq!(
        h.stores
            .cursors
            .get_conversation_cursor("me", CONVERSATION)
            .await
            .unwrap()
            .unwrap()
            .last_seq,
        18,
        "opening history never rewinds or advances the synchronization cursor"
    );

    // Leaving/re-entering, including returning from a plugin, must show the repaired cache
    // immediately and must not duplicate rows or perform a second history request.
    for _ in 0..3 {
        h.view
            .close(CloseViewRequest {
                view_id: h.view_id(),
            })
            .await
            .unwrap();
        h.open().await;
        assert_eq!(h.window().await, (expected.clone(), false));
    }
    assert!(h.server.take_requests().is_empty());
    assert_eq!(h.server.cursor_syncs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn pending_only_is_not_mistaken_for_loaded_history_and_keeps_its_identity() {
    let h = cached_gap().await;
    h.send("pending-same-key").await;
    h.open().await;
    let state = h.state().await;
    assert_eq!(state.seqs, vec![1, 2, 3, 4, 7, 10, 13, 15, 17]);
    assert_eq!(state.pending, vec!["pending-same-key"]);
    assert_eq!(h.server.take_requests(), vec![Req::Before(18, 40)]);
    h.live_message(19).await;
    assert_eq!(h.state().await.pending, vec!["pending-same-key"]);
    assert_eq!(h.state().await.seqs.last(), Some(&19));
}

#[tokio::test]
async fn proof_only_tail_pages_unlock_existing_rows_without_a_message_arrival_event() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 18,
        visible_after_seq: 0,
        server: &[[1, 12]],
        local: &[[1, 12]],
        coverage: &[[1, 12], [18, 18]],
    })
    .await;
    h.server.backfill_pages.lock().unwrap().extend([
        Ok(proof_page(17)),
        Ok(proof_page(15)),
        Ok(proof_page(13)),
    ]);
    h.open().await;
    assert_eq!(h.window().await, (seqs(1, 12), false));
    assert_eq!(h.coverage().await, vec![[1, 18]]);
    assert_eq!(
        h.server.take_requests(),
        vec![
            Req::Before(18, 40),
            Req::Before(17, 40),
            Req::Before(15, 40)
        ]
    );
    assert!(
        h.take_updates().is_empty(),
        "the initial snapshot itself contains the rows"
    );
}

#[tokio::test]
async fn unknown_empty_history_is_bounded_and_is_an_error_instead_of_true_empty() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 18,
        visible_after_seq: 0,
        server: &[[1, 12]],
        local: &[[1, 12]],
        coverage: &[[1, 12], [18, 18]],
    })
    .await;
    h.server.backfill_pages.lock().unwrap().extend([
        Ok(proof_page(17)),
        Ok(proof_page(16)),
        Ok(proof_page(15)),
    ]);
    let error = h
        .try_open()
        .await
        .expect_err("three pages still leave an unknown gap");
    assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    assert!(error.is_retryable());
    assert_eq!(h.server.take_requests().len(), 3);
    assert!(h.view.inner.views.read().await.timelines.is_empty());
    h.open().await;
    assert_eq!(
        h.window().await,
        (seqs(1, 12), false),
        "retry continues from proven progress"
    );
}

#[tokio::test]
async fn an_offline_first_screen_keeps_pending_storage_and_reconnect_can_retry() {
    let h = cached_gap().await;
    h.send("pending-offline").await;
    h.server.offline.store(true, Ordering::SeqCst);
    let error = h
        .try_open()
        .await
        .expect_err("unknown history cannot be called empty offline");
    assert_eq!(error.code(), Some(ErrorCode::NetworkError));
    assert!(h.view.inner.views.read().await.timelines.is_empty());
    assert!(
        h.stores
            .messages
            .get_by_client_msg_id("pending-offline")
            .await
            .unwrap()
            .is_some()
    );
    h.server.offline.store(false, Ordering::SeqCst);
    h.open().await;
    assert_eq!(h.state().await.pending, vec!["pending-offline"]);
    assert_eq!(h.window().await.0.last(), Some(&17));
}

#[tokio::test]
async fn an_older_gap_failure_keeps_a_materialized_latest_row_in_the_first_snapshot() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 18,
        visible_after_seq: 0,
        server: &[[1, 12], [17, 17]],
        local: &[[1, 12], [17, 17]],
        coverage: &[[1, 12], [18, 18]],
    })
    .await;
    h.server.backfill_pages.lock().unwrap().extend([
        Ok(message_page(17)),
        Err(FlareError::localized(ErrorCode::NetworkError, "offline")),
    ]);
    h.open().await;
    assert_eq!(
        h.window().await,
        (vec![17], true),
        "never cross the older gap or discard valid latest history"
    );
    assert_eq!(h.server.take_requests().len(), 2);
    h.load_older().await.unwrap();
    assert_eq!(h.window().await, ([seqs(1, 12), vec![17]].concat(), false));
}

#[tokio::test]
async fn a_live_push_during_a_failed_bootstrap_is_re_read_and_does_not_cross_the_gap() {
    let h = cached_gap().await;
    h.server.offline.store(true, Ordering::SeqCst);
    let stores = h.stores.clone();
    *h.server.after_backfill.lock().unwrap() = Some(Box::new(move || {
        Box::pin(async move { stores.messages.save_batch(&[row(19)]).await })
    }));
    h.open().await;
    assert_eq!(h.window().await, (vec![19], true));
    assert!(h.coverage().await.iter().all(|range| range != &[1, 18]));
    assert_eq!(h.server.take_requests(), vec![Req::Before(18, 40)]);
}

#[tokio::test]
async fn true_empty_and_fully_hidden_history_do_not_backfill() {
    for (max_seq, floor, local, coverage) in [
        (0, 0, vec![], vec![]),
        (18, 18, vec![[1, 17]], vec![[18, 18]]),
    ] {
        let h = Harness::new(Setup {
            page_size: 40,
            max_seq,
            visible_after_seq: floor,
            server: &[],
            local: &local,
            coverage: &coverage,
        })
        .await;
        h.open().await;
        assert_eq!(h.window().await, (vec![], false));
        assert!(h.server.take_requests().is_empty());
    }
}

#[tokio::test]
async fn an_empty_last_server_page_can_prove_the_conversation_truly_empty() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 18,
        visible_after_seq: 0,
        server: &[],
        local: &[],
        coverage: &[[18, 18]],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (vec![], false));
    assert_eq!(h.coverage().await, vec![[1, 18]]);
    assert_eq!(h.server.take_requests(), vec![Req::Before(18, 40)]);
}

#[tokio::test]
async fn an_anchor_window_is_not_replaced_by_latest_bootstrap() {
    let h = Harness::new(Setup {
        page_size: 4,
        max_seq: 18,
        visible_after_seq: 0,
        server: &[[1, 16]],
        local: &[[1, 16]],
        coverage: &[[1, 16], [18, 18]],
    })
    .await;
    h.open_around(Some("s3"), None).await.unwrap();
    assert_eq!(h.state().await.anchor_key.as_deref(), Some("m3"));
    assert!(!h.state().await.attached_to_latest);
    assert!(h.server.take_requests().is_empty());
}

#[tokio::test]
async fn foreground_completion_delivers_a_write_whose_event_preceded_view_registration() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 17,
        visible_after_seq: 0,
        server: &[[1, 17]],
        local: &[[1, 17]],
        coverage: &[[1, 17]],
    })
    .await;
    let view = h.view.clone();
    let stores = h.stores.clone();
    *h.reads.after_read.lock().unwrap() = Some(Box::new(move || {
        Box::pin(async move {
            assert!(view.inner.views.read().await.timelines.is_empty());
            let message = row(18);
            stores
                .messages
                .save_batch(std::slice::from_ref(&message))
                .await?;
            // Deliberately process the event while there is no registered view. No later
            // message event exists to rescue this opening, and the foreground sync is a no-op.
            view.refresh_open_views(
                super::view_refresh_plan(&SdkEvent::Message(MessageEvent::Received {
                    message: Box::new(message),
                }))
                .unwrap(),
            )
            .await;
            Ok(())
        })
    }));
    let mut events = h.bus.subscribe_raw();
    h.open().await;
    // The host subscribes before open, like FlareOpenTimeline, and must observe the repair
    // through view.updated; a snapshot read or a user load-older is not the recovery path.
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if h.state().await.seqs.last() == Some(&18) {
                h.settle().await;
                break;
            }
            let _ = events.recv().await.unwrap();
        }
    })
    .await
    .expect("foreground completion must repair the opening race");
    assert_eq!(h.window().await, (seqs(1, 18), false));
    assert!(h.server.take_requests().is_empty());
    assert!(
        h.take_updates()
            .iter()
            .any(|update| update.view_id == h.view_id())
    );
}

#[tokio::test]
async fn newly_discovered_foreground_history_is_repaired_after_registration() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 0,
        visible_after_seq: 0,
        server: &[[17, 17]],
        local: &[],
        coverage: &[],
    })
    .await;
    let view = h.view.clone();
    let stores = h.stores.clone();
    *h.server.foreground.lock().unwrap() = Some(Box::new(move || {
        Box::pin(async move {
            assert_eq!(view.inner.views.read().await.timelines.len(), 1);
            let mut conversation = stores.conversations.get(CONVERSATION).await?.unwrap();
            conversation.max_seq = 17;
            stores.conversations.save_one(&conversation).await?;
            let message = row(17);
            stores
                .messages
                .save_batch(std::slice::from_ref(&message))
                .await?;
            stores
                .timeline_coverage
                .add(CONVERSATION, SeqRange::new(18, 18).unwrap())
                .await?;
            view.refresh_open_views(
                super::view_refresh_plan(&SdkEvent::Message(MessageEvent::Received {
                    message: Box::new(message),
                }))
                .unwrap(),
            )
            .await;
            Ok(())
        })
    }));
    let mut events = h.bus.subscribe_raw();
    h.open().await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if h.state().await.seqs == vec![17] {
                h.settle().await;
                break;
            }
            let _ = events.recv().await.unwrap();
        }
    })
    .await
    .expect("the foreground repair cannot depend on another message event");
    assert_eq!(h.window().await, (vec![17], false));
    assert_eq!(h.server.take_requests(), vec![Req::Before(18, 40)]);
}

#[tokio::test]
async fn an_older_slow_open_cannot_replace_a_newer_same_conversation_view() {
    let h = cached_gap().await;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    *h.server.after_backfill.lock().unwrap() = Some(Box::new(move || {
        Box::pin(async move {
            started_tx.send(()).unwrap();
            release_rx.await.unwrap();
            Ok(())
        })
    }));
    let older_view = h.view.clone();
    let older = tokio::spawn(async move {
        older_view
            .open_timeline(OpenTimelineViewRequest {
                conversation_id: CONVERSATION.into(),
                message_limit: 40,
                anchor: None,
            })
            .await
            .unwrap()
    });
    started_rx.await.unwrap();
    h.open().await;
    let newest_id = h.view_id();
    release_tx.send(()).unwrap();
    let late = older.await.unwrap();
    {
        let views = h.view.inner.views.read().await;
        assert!(views.timelines.contains_key(&newest_id));
        assert!(!views.timelines.contains_key(&late.view_id));
        assert_eq!(views.timelines.len(), 1);
    }
    let closed = h
        .view
        .close(CloseViewRequest {
            view_id: late.view_id,
        })
        .await
        .unwrap();
    assert!(!closed.closed, "closing the superseded opening is harmless");
    assert!(
        h.view
            .inner
            .views
            .read()
            .await
            .timelines
            .contains_key(&newest_id)
    );
}
