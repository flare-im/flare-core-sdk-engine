//! 时间线视图的行键、增量、版本号、按需刷新与滑动窗口。
//!
//! 测试台（`view_test_harness.rs`）每一步都把推出的更新应用到宿主侧窗口并与视图比对，所以
//! 这里的每个用例同时验证「核心产出的增量应用到上一版本后等于新快照」。最后一个用例重放
//! 一组场景，与 sdk-spec 的 `golden/view/timeline-delta.json` 逐条比对：那份向量就是核心的产出。

use flare_proto::common::{Message as ProtoMessage, MessageDeleteEvent};
use serde_json::{Value, json};

use super::harness::{
    ApplyOutcome, CONVERSATION, Harness, HostTimeline, ReadCounts, Req, Setup, row, seqs,
};
use crate::kernel::event::{ConversationEvent, MessageEvent, SdkEvent};
use crate::model::{
    IMMessage, TimelineViewPageResponse, TimelineViewRequest, ViewDelta, ViewSnapshot, ViewUpdate,
    ViewUpdateKind,
};

fn setup(page_size: u32, max_seq: u64, local: &'static [[u64; 2]]) -> Setup<'static> {
    Setup {
        page_size,
        max_seq,
        visible_after_seq: 0,
        server: Box::leak(Box::new([[1, max_seq]])),
        local,
        coverage: local,
    }
}

fn timeline_delta(update: &ViewUpdate) -> &ViewDelta {
    assert_eq!(update.kind, ViewUpdateKind::Delta, "a delta update");
    update.delta.as_ref().expect("delta payload")
}

fn keys(rows: &[IMMessage]) -> Vec<String> {
    rows.iter().map(IMMessage::row_key).collect()
}

/// 发送确认前后行键都是 `client_msg_id`：确认是同一个键的原位更新，不是删掉旧行再插入新行。
#[tokio::test]
async fn the_ack_keeps_the_row_key_and_updates_the_row_in_place() {
    let h = Harness::new(setup(40, 50, &[[1, 50]])).await;
    h.open().await;
    h.send("cm-a").await;
    let sent = h.take_updates();
    let ViewDelta::Timeline { append, .. } = timeline_delta(&sent[0]) else {
        panic!("timeline delta");
    };
    assert_eq!(keys(append), vec!["cm-a"]);

    h.ack("cm-a", 51).await;
    let acked = h.take_updates();
    assert_eq!(acked.len(), 1);
    let ViewDelta::Timeline {
        prepend,
        append,
        upsert,
        remove,
        ..
    } = timeline_delta(&acked[0])
    else {
        panic!("timeline delta");
    };
    assert!(prepend.is_empty() && append.is_empty() && remove.is_empty());
    assert_eq!(keys(upsert), vec!["cm-a"]);
    assert_eq!(upsert[0].conversation_seq, 51);
    assert_eq!(upsert[0].server_id, "s51");
    let state = h.state().await;
    assert_eq!(state.seqs, seqs(11, 51));
    assert!(state.pending.is_empty());
}

/// 脱离最新的窗口不追加实时消息，只保证 hasNewer；已在窗口里的行照常更新。
#[tokio::test]
async fn a_detached_window_takes_no_live_messages() {
    let h = Harness::new(setup(40, 1000, &[[1, 1000]])).await;
    h.open_around(None, Some(100)).await.unwrap();
    let before = h.state().await;
    assert_eq!(before.seqs, seqs(81, 120));
    assert!(before.has_newer && !before.attached_to_latest);

    h.live_message(1001).await;
    h.live_message(1002).await;
    let after = h.state().await;
    assert_eq!(after.seqs, seqs(81, 120), "live messages stay out");
    assert!(after.has_newer);
    assert_eq!(after.revision, before.revision, "nothing to publish");
    assert!(h.take_updates().is_empty());
}

/// `load_older` 并入的行只经 `view.updated` 推出；返回值只有并入了几行与包含这一页的版本号。
#[tokio::test]
async fn older_rows_arrive_only_through_view_updated() {
    let h = Harness::new(setup(40, 100, &[[1, 100]])).await;
    h.open().await;
    let response = h.load_older_page().await.unwrap();
    assert_eq!(
        serde_json::to_value(&response).unwrap(),
        json!({ "viewId": h.view_id(), "loadedCount": 40, "revision": 2 })
    );
    let updates = h.take_updates();
    assert_eq!(updates.len(), 1, "one event carries the page");
    assert_eq!(updates[0].revision, response.revision);
    let ViewDelta::Timeline {
        prepend, has_older, ..
    } = timeline_delta(&updates[0])
    else {
        panic!("timeline delta");
    };
    assert_eq!(prepend.len(), response.loaded_count as usize);
    assert_eq!(
        prepend.first().map(|message| message.conversation_seq),
        Some(21)
    );
    assert!(*has_older);
}

/// 按需刷新：一个表情事件只按 id 读它涉及的那一行，不重读窗口。
#[tokio::test]
async fn a_reaction_rereads_exactly_one_row() {
    let h = Harness::new(setup(40, 40, &[[1, 40]])).await;
    h.open().await;
    let mut edited = row(10);
    edited.is_edited = true;
    h.stores
        .messages
        .save_batch(std::slice::from_ref(&edited))
        .await
        .unwrap();
    h.reads.counts.reset();

    h.event("reaction").await;

    let counts = &h.reads.counts;
    assert_eq!(ReadCounts::get(&counts.pages), 0, "no window reread");
    assert_eq!(ReadCounts::get(&counts.newer_pages), 0);
    assert_eq!(ReadCounts::get(&counts.by_id_calls), 1, "one batch read");
    assert_eq!(ReadCounts::get(&counts.by_id_ids), 1, "for one id");
    assert_eq!(ReadCounts::get(&counts.by_id_rows), 1, "returning one row");
    let updates = h.take_updates();
    assert_eq!(updates.len(), 1);
    let ViewDelta::Timeline { upsert, remove, .. } = timeline_delta(&updates[0]) else {
        panic!("timeline delta");
    };
    assert_eq!(keys(upsert), vec!["m10"]);
    assert!(upsert[0].is_edited);
    assert!(remove.is_empty());

    // 不在窗口里的消息：一次也不读。
    h.reads.counts.reset();
    h.refresh(SdkEvent::Message(MessageEvent::ReactionChanged {
        conversation_id: CONVERSATION.into(),
        server_msg_id: "s900".into(),
        user_id: "u2".into(),
        emoji: "+1".into(),
        action: 1,
    }))
    .await;
    assert_eq!(ReadCounts::get(&h.reads.counts.by_id_calls), 0);
    assert_eq!(ReadCounts::get(&h.reads.counts.pages), 0);
}

/// 一条被删除的消息：按 id 重读不到，移出窗口。
#[tokio::test]
async fn a_deleted_message_leaves_the_window_by_key() {
    let h = Harness::new(setup(40, 40, &[[1, 40]])).await;
    h.open().await;
    h.stores.messages.delete("s7").await.unwrap();
    h.refresh(SdkEvent::Message(MessageEvent::Deleted {
        conversation_id: CONVERSATION.into(),
        event: MessageDeleteEvent {
            server_msg_id: "s7".into(),
            ..Default::default()
        },
    }))
    .await;
    let updates = h.take_updates();
    let ViewDelta::Timeline { remove, upsert, .. } = timeline_delta(&updates[0]) else {
        panic!("timeline delta");
    };
    assert_eq!(remove, &vec!["m7".to_string()]);
    assert!(upsert.is_empty());
    assert!(!h.state().await.seqs.contains(&7));
}

/// 版本号逐次加 1；宿主漏了一次更新时发现跳号，取快照整体替换；比已应用版本旧的更新丢弃。
#[tokio::test]
async fn a_skipped_revision_is_healed_with_the_snapshot() {
    let h = Harness::new(setup(40, 200, &[[1, 200]])).await;
    h.open().await;
    let mut lagging = h.host.lock().unwrap().clone();
    h.load_older().await.unwrap();
    h.live_message(201).await;
    let updates = h.take_updates();
    assert_eq!(
        updates
            .iter()
            .map(|update| update.revision)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );

    // 漏掉了版本 2。
    assert_eq!(lagging.apply(&updates[1]), ApplyOutcome::SnapshotRequired);
    let response = h
        .view
        .timeline_snapshot(TimelineViewRequest {
            view_id: h.view_id(),
        })
        .await
        .unwrap();
    assert_eq!(response.revision, 3);
    let ViewSnapshot::Timeline(snapshot) = &response.snapshot else {
        panic!("timeline snapshot");
    };
    lagging = HostTimeline::from_snapshot(response.revision, snapshot);
    assert_eq!(
        serde_json::to_value(&lagging.rows).unwrap(),
        serde_json::to_value(h.window_rows().await).unwrap()
    );
    assert_eq!(lagging.apply(&updates[0]), ApplyOutcome::Stale);
    assert_eq!(lagging.apply(&updates[1]), ApplyOutcome::Stale);

    let closed = h.view.timeline_snapshot(TimelineViewRequest {
        view_id: "timeline:missing".into(),
    });
    assert_eq!(closed.await.unwrap_err().reason(), "sdk.view.not_open");
}

/// 滑动窗口 2,000 行：往旧翻超过上限从新的一端裁掉、脱离最新；往新翻回去超过上限从旧的
/// 一端裁掉，接上最新时重新接在最新。
#[tokio::test]
async fn the_sliding_window_keeps_two_thousand_rows() {
    let h = Harness::new(setup(100, 2100, &[[1, 2100]])).await;
    h.open().await;
    for _ in 0..19 {
        assert_eq!(h.load_older().await.unwrap(), 100);
    }
    let full = h.state().await;
    assert_eq!(full.seqs, seqs(101, 2100));
    assert!(full.attached_to_latest && full.has_older);

    assert_eq!(h.load_older().await.unwrap(), 100);
    let trimmed = h.state().await;
    assert_eq!(trimmed.seqs, seqs(1, 2000), "the newer end is cut");
    assert!(!trimmed.has_older);
    assert!(!trimmed.attached_to_latest && trimmed.has_newer);

    h.live_message(2101).await;
    assert_eq!(
        h.state().await.seqs,
        seqs(1, 2000),
        "detached: no live rows"
    );

    // 接上最新时，脱离期间到达的头部未证实行（2101）一并并入。
    assert_eq!(h.load_newer().await.unwrap(), 101);
    let back = h.state().await;
    assert_eq!(back.seqs, seqs(102, 2101), "the older end is cut");
    assert!(back.has_older);
    assert!(back.attached_to_latest && !back.has_newer);
    assert_eq!(h.server.take_requests(), Vec::<Req>::new());
}

/// 会话摘要变化（免打扰）只换 `conversation`，行不动。
#[tokio::test]
async fn a_header_change_carries_only_the_conversation() {
    let h = Harness::new(setup(40, 40, &[[1, 40]])).await;
    h.open().await;
    let mut conversation = h
        .stores
        .conversations
        .get(CONVERSATION)
        .await
        .unwrap()
        .unwrap();
    conversation.is_muted = true;
    h.stores
        .conversations
        .save_one(&conversation)
        .await
        .unwrap();
    h.refresh(SdkEvent::Conversation(ConversationEvent::Updated {
        conversation_id: CONVERSATION.into(),
    }))
    .await;
    let updates = h.take_updates();
    assert_eq!(updates.len(), 1);
    let ViewDelta::Timeline {
        prepend,
        append,
        upsert,
        remove,
        conversation,
        ..
    } = timeline_delta(&updates[0])
    else {
        panic!("timeline delta");
    };
    assert!(prepend.is_empty() && append.is_empty() && upsert.is_empty() && remove.is_empty());
    assert_eq!(
        conversation
            .as_ref()
            .map(|conversation| conversation.is_muted),
        Some(true)
    );
}

/// 打开时间线的默认页大小是 40。
#[tokio::test]
async fn the_default_page_is_forty_rows() {
    let h = Harness::new(setup(0, 100, &[[1, 100]])).await;
    let request: crate::model::OpenTimelineViewRequest =
        serde_json::from_value(json!({ "conversationId": CONVERSATION })).unwrap();
    let response = h.view.open_timeline(request).await.unwrap();
    let ViewSnapshot::Timeline(snapshot) = response.snapshot else {
        panic!("timeline snapshot");
    };
    assert_eq!(snapshot.messages.len(), 40);
}

// ---- timeline-delta.json -------------------------------------------------------------------

/// 一条向量：应用前的宿主窗口、一条更新、应用结果与应用后的窗口。
fn vector_case(name: &str, before: &HostTimeline, update: &ViewUpdate) -> Value {
    let mut after = before.clone();
    let applied = after.apply(update);
    let mut case = json!({
        "name": name,
        "before": host_json(before),
        "update": serde_json::to_value(update).unwrap(),
        "expect": match applied {
            ApplyOutcome::Applied => "applied",
            ApplyOutcome::SnapshotRequired => "snapshotRequired",
            ApplyOutcome::Stale => "stale",
        },
    });
    if applied == ApplyOutcome::Applied {
        case["after"] = host_json(&after);
    }
    case
}

fn host_json(host: &HostTimeline) -> Value {
    json!({
        "revision": host.revision,
        "rows": serde_json::to_value(&host.rows).unwrap(),
        "hasOlder": host.has_older,
        "hasNewer": host.has_newer,
        "attachedToLatest": host.attached_to_latest,
    })
}

/// 跑一步，把这一步推出的每条更新连同应用前的宿主窗口记成一条向量。
async fn record(
    h: &Harness,
    cases: &mut Vec<Value>,
    name: &str,
    step: impl std::future::Future<Output = ()>,
) {
    let before = h.host.lock().unwrap().clone();
    step.await;
    let mut host = before;
    let updates = h.take_updates();
    assert!(!updates.is_empty(), "{name}: the step published an update");
    for (index, update) in updates.iter().enumerate() {
        let label = if updates.len() == 1 {
            name.to_string()
        } else {
            format!("{name} ({})", index + 1)
        };
        cases.push(vector_case(&label, &host, update));
        host.apply(update);
    }
}

async fn produced_delta_cases() -> Vec<Value> {
    let mut cases = Vec::new();

    let h = Harness::new(setup(2, 4, &[[1, 4]])).await;
    h.open().await;
    record(&h, &mut cases, "older rows are prepended", async {
        h.load_older().await.unwrap();
    })
    .await;
    let before_live = h.host.lock().unwrap().clone();
    record(&h, &mut cases, "a live message is appended", async {
        h.live_message(5).await;
    })
    .await;
    let live_update = cases
        .last()
        .map(|case| serde_json::from_value::<ViewUpdate>(case["update"].clone()).unwrap())
        .unwrap();
    record(
        &h,
        &mut cases,
        "an edited row is replaced in place",
        async {
            let mut edited = row(3);
            edited.is_edited = true;
            h.stores
                .messages
                .save_batch(std::slice::from_ref(&edited))
                .await
                .unwrap();
            h.refresh(SdkEvent::Message(MessageEvent::Edited {
                conversation_id: CONVERSATION.into(),
                server_msg_id: "s3".into(),
            }))
            .await;
        },
    )
    .await;
    record(&h, &mut cases, "a deleted row is removed by key", async {
        h.stores.messages.delete("s4").await.unwrap();
        h.refresh(SdkEvent::Message(MessageEvent::Deleted {
            conversation_id: CONVERSATION.into(),
            event: MessageDeleteEvent {
                server_msg_id: "s4".into(),
                ..Default::default()
            },
        }))
        .await;
    })
    .await;
    record(&h, &mut cases, "a pending row is appended", async {
        h.send("cm-a").await;
    })
    .await;
    record(
        &h,
        &mut cases,
        "the ack updates the pending row under the same key",
        async {
            h.ack("cm-a", 6).await;
        },
    )
    .await;
    record(&h, &mut cases, "another pending row is appended", async {
        h.send("cm-b").await;
    })
    .await;
    record(
        &h,
        &mut cases,
        "a live message lands before the pending row",
        async {
            h.live_message(8).await;
        },
    )
    .await;
    record(
        &h,
        &mut cases,
        "the ack moves the row before a later one with a reorder",
        async {
            h.ack("cm-b", 7).await;
        },
    )
    .await;

    let mut skipped = live_update.clone();
    skipped.revision = before_live.revision + 2;
    cases.push(vector_case(
        "a skipped revision asks for the snapshot",
        &before_live,
        &skipped,
    ));
    let mut stale = live_update;
    stale.revision = before_live.revision;
    cases.push(vector_case(
        "an update the host already has is dropped",
        &before_live,
        &stale,
    ));

    let h = Harness::new(Setup {
        page_size: 4,
        max_seq: 8,
        visible_after_seq: 0,
        server: &[[1, 8]],
        local: &[[1, 6]],
        coverage: &[[1, 6]],
    })
    .await;
    h.open().await;
    h.live_message(8).await;
    h.take_updates();
    record(
        &h,
        &mut cases,
        "a row filled in between comes with a reorder",
        async {
            h.gap_filled(7, 8).await;
        },
    )
    .await;

    let h = Harness::new(setup(4, 20, &[[1, 20]])).await;
    h.open_around(None, Some(5)).await.unwrap();
    record(
        &h,
        &mut cases,
        "rows read towards the latest are appended",
        async {
            h.load_newer().await.unwrap();
        },
    )
    .await;
    record(
        &h,
        &mut cases,
        "going back to the latest is a snapshot",
        async {
            h.jump_to_latest().await;
        },
    )
    .await;
    cases
}

/// 比较时经核心模型解码再编码一遍，模型新增带默认值的字段不会让向量失效。
fn normalized(case: &Value) -> Value {
    let mut case = case.clone();
    for side in ["before", "after"] {
        if let Some(rows) = case.get_mut(side).and_then(|side| side.get_mut("rows")) {
            let decoded: Vec<IMMessage> = serde_json::from_value(rows.clone()).unwrap();
            *rows = serde_json::to_value(decoded).unwrap();
        }
    }
    let update: ViewUpdate = serde_json::from_value(case["update"].clone()).unwrap();
    case["update"] = serde_json::to_value(update).unwrap();
    case
}

const DELTA_VECTORS_DESCRIPTION: &str = "Timeline view update vectors (view.updated). Each case is the host's window before an update, one update exactly as the core published it, and the result: applied (with the window after), snapshotRequired (the revision skipped one: the host leaves its window as it is and calls view.timeline.snapshot) or stale (the host already has that revision: the update is dropped). before and after list the rows in timeline order with the three state fields and the revision; the conversation header is not part of them: a host replaces its header whenever an update carries one. core-sdk produces every case in src/client/api/view_window_tests.rs and checks this file against what it produces.";

const DELTA_APPLY_RULES: [&str; 6] = [
    "If revision is not the applied revision plus 1, do not apply: drop it when it is not newer, otherwise take view.timeline.snapshot and replace the window.",
    "Remove the rows whose keys are in remove.",
    "Replace each row in upsert in place, found by its key.",
    "Put prepend before the first row and append after the last row, both already in timeline order.",
    "When reorder is present, order the rows by the keys it lists (it lists every row).",
    "Take hasOlder, hasNewer and attachedToLatest, and conversation when present; kind snapshot replaces the whole window.",
];

fn delta_vectors_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../flare-im-core-client-sdk/sdk-spec/golden/view/timeline-delta.json")
}

#[tokio::test]
async fn timeline_delta_vectors_are_what_the_core_produces() {
    let produced = produced_delta_cases().await;
    let kinds = produced
        .iter()
        .map(|case| case["expect"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert!(kinds.iter().any(|kind| kind == "snapshotRequired"));
    assert!(kinds.iter().any(|kind| kind == "stale"));
    assert!(
        produced
            .iter()
            .any(|case| case["update"]["kind"] == json!("snapshot"))
    );
    assert!(
        produced
            .iter()
            .any(|case| !case["update"]["delta"]["reorder"].is_null())
    );

    let path = delta_vectors_path();
    if std::env::var_os("FLARE_WRITE_VIEW_VECTORS").is_some() {
        let file = json!({
            "schemaVersion": 1,
            "description": DELTA_VECTORS_DESCRIPTION,
            "rowKey": "clientMsgId when non-empty, else serverId when non-empty, else seq:<conversationId>:<conversationSeq>",
            "applyRules": DELTA_APPLY_RULES,
            "cases": produced,
        });
        std::fs::write(&path, serde_json::to_string_pretty(&file).unwrap() + "\n").unwrap();
    }
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let file: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(file["schemaVersion"], json!(1));
    let cases = file["cases"].as_array().expect("cases");
    assert_eq!(
        cases.len(),
        produced.len(),
        "every produced update is listed"
    );
    for (expected, produced) in cases.iter().zip(&produced) {
        let name = expected["name"].as_str().unwrap_or_default();
        assert_eq!(
            normalized(expected),
            normalized(produced),
            "{name}: the vector is what the core produces"
        );
        // 应用器照做：按规则应用到 before 上得到 after。
        let before = host_from_json(&expected["before"]);
        let update: ViewUpdate = serde_json::from_value(expected["update"].clone()).unwrap();
        let mut host = before;
        let applied = host.apply(&update);
        assert_eq!(
            json!(match applied {
                ApplyOutcome::Applied => "applied",
                ApplyOutcome::SnapshotRequired => "snapshotRequired",
                ApplyOutcome::Stale => "stale",
            }),
            expected["expect"],
            "{name}"
        );
        if applied == ApplyOutcome::Applied {
            assert_eq!(
                normalized(&json!({ "after": host_json(&host), "update": expected["update"] }))["after"],
                normalized(expected)["after"],
                "{name}: applying the update gives the next window"
            );
        }
    }
}

fn host_from_json(value: &Value) -> HostTimeline {
    HostTimeline {
        revision: value["revision"].as_u64().unwrap(),
        rows: serde_json::from_value(value["rows"].clone()).unwrap(),
        has_older: value["hasOlder"].as_bool().unwrap(),
        has_newer: value["hasNewer"].as_bool().unwrap(),
        attached_to_latest: value["attachedToLatest"].as_bool().unwrap(),
        conversation: None,
    }
}

#[test]
fn the_page_response_only_names_the_revision() {
    let response = TimelineViewPageResponse {
        view_id: "timeline:1".into(),
        loaded_count: 3,
        revision: 9,
    };
    assert_eq!(
        serde_json::to_value(response).unwrap(),
        json!({ "viewId": "timeline:1", "loadedCount": 3, "revision": 9 })
    );
    let message = IMMessage::new(ProtoMessage {
        client_msg_id: "cm-1".into(),
        server_id: "s-1".into(),
        ..Default::default()
    });
    assert_eq!(message.row_key(), "cm-1");
}
