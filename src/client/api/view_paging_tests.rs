//! 往旧翻页与 hasOlder：只由已证实连续区间回答。
//!
//! 测试台见 `view_test_harness.rs`。用例分三组（区间、load_older、不回跳），最后一组逐条读
//! sdk-spec 的 `golden/view/timeline-paging.json`（往旧、往新、定位、实时合并各一节）。

use std::sync::Arc;
use std::sync::atomic::Ordering;

use serde::Deserialize;
use tokio::sync::RwLock;

use super::harness::{CONVERSATION, Harness, Req, Setup, expand, proto, seqs};
use crate::application::notification::{NotificationHandlerRegistry, NotificationInboundPipeline};
use crate::application::services::{EventDeduper, MessageDeduper};
use crate::infrastructure::persistence::in_memory_im_provider;
use crate::kernel::event::EventBus;
use crate::shared::error::{ErrorCode, Result};

// ---- 区间 ---------------------------------------------------------------------------------

/// 单条实时推送不写区间：在游标推进或缺口补拉证明它之前，它是头部未证实行。
#[tokio::test]
async fn a_realtime_push_writes_no_range() {
    use crate::infrastructure::protocol::DownlinkPayload;

    let stores = in_memory_im_provider();
    let bus = EventBus::new();
    let dispatcher = crate::runtime::Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores.clone()),
        Arc::new(RwLock::new("me".to_string())),
        EventDeduper::new(Some(64)),
        NotificationInboundPipeline::new(
            Arc::new(NotificationHandlerRegistry::new()),
            MessageDeduper::new(Some(64)),
            bus,
        ),
        crate::spi::metrics::MetricsRecorder::disabled(),
    );
    dispatcher
        .dispatch(DownlinkPayload::MessagePush(
            flare_proto::common::MessagePush {
                messages: vec![proto(1001)],
                notifications: Vec::new(),
            },
        ))
        .await
        .unwrap();
    assert!(
        stores.messages.get("s1001").await.unwrap().is_some(),
        "the pushed message is stored"
    );
    assert!(
        stores
            .timeline_coverage
            .coverage(CONVERSATION)
            .await
            .unwrap()
            .is_empty()
    );
}

/// 回填写入的区间与已有区间合并；本地删除不改区间。
#[tokio::test]
async fn backfilled_ranges_merge_and_local_deletes_leave_them() {
    let h = Harness::new(Setup {
        page_size: 10,
        max_seq: 50,
        visible_after_seq: 0,
        server: &[[1, 50]],
        local: &[[41, 50]],
        coverage: &[[41, 50]],
    })
    .await;
    h.open().await;
    h.load_older().await.unwrap();
    assert_eq!(h.coverage().await, vec![[31, 50]]);
    h.stores.messages.delete("s45").await.unwrap();
    h.event("delete").await;
    assert_eq!(h.coverage().await, vec![[31, 50]]);
    let (window, has_older) = h.window().await;
    assert!(!window.contains(&45));
    assert!(has_older);
}

// ---- load_older --------------------------------------------------------------------------

#[tokio::test]
async fn load_older_reads_a_full_page_inside_a_range_without_a_request() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 100,
        visible_after_seq: 0,
        server: &[[1, 100]],
        local: &[[1, 100]],
        coverage: &[[1, 100]],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (seqs(61, 100), true));
    assert_eq!(h.load_older().await.unwrap(), 40);
    assert_eq!(h.window().await, (seqs(21, 100), true));
    assert_eq!(h.load_older().await.unwrap(), 20);
    assert_eq!(h.window().await, (seqs(1, 100), false));
    assert_eq!(h.server.take_requests(), vec![]);
}

/// 读到区间边界：以边界为锚回填缺的行数，再在扩展后的区间内读满。
#[tokio::test]
async fn load_older_backfills_at_the_range_boundary() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 100,
        visible_after_seq: 0,
        server: &[[1, 100]],
        local: &[[50, 100]],
        coverage: &[[50, 100]],
    })
    .await;
    h.open().await;
    assert_eq!(h.load_older().await.unwrap(), 40);
    assert_eq!(h.server.take_requests(), vec![Req::Before(50, 29)]);
    assert_eq!(h.window().await, (seqs(21, 100), true));
    assert_eq!(h.coverage().await, vec![[21, 100]]);
}

/// 窗口紧挨着洞（本地没有任何区间）：以窗口最旧一行为锚回填一整页。
#[tokio::test]
async fn load_older_backfills_before_a_window_next_to_a_hole() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 100,
        visible_after_seq: 0,
        server: &[[1, 100]],
        local: &[[61, 100]],
        coverage: &[],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (seqs(61, 100), true));
    h.load_older().await.unwrap();
    assert_eq!(h.server.take_requests(), vec![Req::Before(61, 40)]);
    assert_eq!(h.window().await, (seqs(21, 100), true));
}

/// 回填失败：窗口不变、返回错误，缺口下面的本地行不读。
#[tokio::test]
async fn a_failed_backfill_keeps_the_window_and_does_not_cross_the_gap() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 1000,
        visible_after_seq: 0,
        server: &[[1, 1000]],
        local: &[[1, 36], [980, 1000]],
        coverage: &[[1, 36], [980, 1000]],
    })
    .await;
    h.open().await;
    let before = h.window().await;
    assert_eq!(before, (seqs(980, 1000), true));
    h.server.offline.store(true, Ordering::SeqCst);
    let error = h.load_older().await.expect_err("the backfill fails");
    assert_eq!(error.code(), Some(ErrorCode::NetworkError));
    assert_eq!(h.window().await, before, "the window is unchanged");
    assert_eq!(h.server.take_requests(), vec![Req::Before(980, 40)]);
}

/// 回填失败时，先把区间内已经读到的连续行并入窗口，再在缺口处返回错误：这些行由区间证明
/// 与窗口连续，不算越过缺口。恢复后从同一个边界接着回填。
#[tokio::test]
async fn a_failed_backfill_merges_the_rows_read_inside_the_range_first() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 100,
        visible_after_seq: 0,
        server: &[[1, 100]],
        local: &[[50, 100]],
        coverage: &[[50, 100]],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (seqs(61, 100), true));
    h.server.offline.store(true, Ordering::SeqCst);
    let error = h.load_older().await.expect_err("the backfill fails");
    assert_eq!(error.code(), Some(ErrorCode::NetworkError));
    assert_eq!(h.server.take_requests(), vec![Req::Before(50, 29)]);
    assert_eq!(
        h.window().await,
        (seqs(50, 100), true),
        "the rows proven contiguous are in the window"
    );
    let updates = h.take_updates();
    assert_eq!(
        updates.len(),
        1,
        "the merged rows come through view.updated"
    );

    h.server.offline.store(false, Ordering::SeqCst);
    assert_eq!(h.load_older().await.unwrap(), 40);
    assert_eq!(h.server.take_requests(), vec![Req::Before(50, 40)]);
    assert_eq!(h.window().await, (seqs(10, 100), true));
}

/// 到达起点：回填说没有更早的，写入 [1, before-1]，hasOlder 变为 false。
#[tokio::test]
async fn reaching_the_start_turns_has_older_off() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 12,
        visible_after_seq: 0,
        server: &[[3, 12]],
        local: &[[9, 12]],
        coverage: &[[9, 12]],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (seqs(9, 12), true));
    h.load_older().await.unwrap();
    assert_eq!(h.server.take_requests(), vec![Req::Before(9, 40)]);
    assert_eq!(h.window().await, (seqs(3, 12), false));
    assert_eq!(h.coverage().await, vec![[1, 12]]);
}

/// 会话第一条消息是 seq 3：区间已证明到 1 时，打开即知没有更早的，也不再请求。
#[tokio::test]
async fn the_first_message_at_seq_three_is_the_start_when_proven() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 12,
        visible_after_seq: 0,
        server: &[[3, 12]],
        local: &[[3, 12]],
        coverage: &[[1, 12]],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (seqs(3, 12), false));
    assert_eq!(h.load_older().await.unwrap(), 0);
    assert_eq!(h.window().await, (seqs(3, 12), false));
    assert_eq!(h.server.take_requests(), vec![]);
}

/// 窗口为空、会话有消息：以会话已知最新 seq 之上一位为锚回填一页。
#[tokio::test]
async fn an_empty_window_backfills_above_the_newest_known_seq() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 50,
        visible_after_seq: 0,
        server: &[[1, 50]],
        local: &[],
        coverage: &[],
    })
    .await;
    h.open().await;
    assert_eq!(h.window().await, (vec![], true));
    h.load_older().await.unwrap();
    assert_eq!(h.server.take_requests(), vec![Req::Before(51, 40)]);
    assert_eq!(h.window().await, (seqs(11, 50), true));
}

/// 服务端回了空页却说还有更早：不重复请求，留给下一次往旧翻页。
#[tokio::test]
async fn a_backfill_that_proves_nothing_is_not_repeated() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 100,
        visible_after_seq: 0,
        server: &[[1, 100]],
        local: &[[61, 100]],
        coverage: &[[61, 100]],
    })
    .await;
    h.open().await;
    h.server.empty_pages_with_more.store(true, Ordering::SeqCst);
    assert_eq!(h.load_older().await.unwrap(), 0);
    assert_eq!(h.server.take_requests(), vec![Req::Before(61, 40)]);
    assert_eq!(h.window().await, (seqs(61, 100), true));
}

// ---- 不回跳 --------------------------------------------------------------------------------

/// 翻到起点后，表情、撤回、删除、同步完成都触发重读：hasOlder 保持 false，窗口不缩回。
/// 窗口有 120 行，超过打开时页大小的上限（100）：按打开时的「最新窗口」整窗重读会把它截短。
#[tokio::test]
async fn has_older_stays_off_after_the_start_through_events() {
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 122,
        visible_after_seq: 0,
        server: &[[3, 122]],
        local: &[[83, 122]],
        coverage: &[[83, 122]],
    })
    .await;
    h.open().await;
    h.load_older().await.unwrap();
    h.load_older().await.unwrap();
    assert_eq!(h.window().await, (seqs(3, 122), false));
    assert_eq!(
        h.server.take_requests(),
        vec![Req::Before(83, 40), Req::Before(43, 40)],
        "two backfills reached the start"
    );

    for kind in [
        "reaction",
        "recall",
        "syncFinished",
        "reaction",
        "syncFinished",
    ] {
        h.event(kind).await;
        assert_eq!(h.window().await, (seqs(3, 122), false), "after {kind}");
    }
    h.stores.messages.delete("s60").await.unwrap();
    h.event("delete").await;
    let (window, has_older) = h.window().await;
    assert_eq!(window.len(), 119);
    assert!(!has_older, "a local delete does not bring load-more back");
    assert_eq!(h.load_older().await.unwrap(), 0);
    assert_eq!(h.server.take_requests(), vec![]);
}

// ---- sdk-spec 向量 -------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vectors {
    schema_version: u32,
    older_paging: Section,
    newer_paging: Section,
    anchors: Section,
    live_updates: Section,
}

#[derive(Deserialize)]
struct Section {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: String,
    boundary_case: String,
    page_size: u32,
    conversation: CaseConversation,
    server: CaseServer,
    local: CaseLocal,
    steps: Vec<Step>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaseConversation {
    max_seq: u64,
    #[serde(default)]
    visible_after_seq: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaseServer {
    visible_messages: Vec<[u64; 2]>,
}

#[derive(Deserialize)]
struct CaseLocal {
    messages: Vec<[u64; 2]>,
    coverage: Vec<[u64; 2]>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Step {
    #[serde(rename = "do")]
    action: String,
    seq: Option<u64>,
    from: Option<u64>,
    to: Option<u64>,
    kind: Option<String>,
    message_id: Option<String>,
    client_msg_id: Option<String>,
    expect: Option<Expect>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Expect {
    window: Option<Vec<[u64; 2]>>,
    pending: Option<Vec<String>>,
    has_older: Option<bool>,
    has_newer: Option<bool>,
    attached_to_latest: Option<bool>,
    /// 缺省不查；写成 `null` 表示快照不带 anchorKey。
    #[serde(default, deserialize_with = "present")]
    anchor_key: Option<Option<String>>,
    requests: Option<Vec<Request>>,
    coverage: Option<Vec<[u64; 2]>>,
    /// 这一步推出的更新的 kind，按顺序。
    updates: Option<Vec<String>>,
    #[serde(default)]
    error: bool,
    error_reason: Option<String>,
}

fn present<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
struct Request {
    before: Option<u64>,
    after: Option<u64>,
    limit: usize,
}

impl Request {
    fn req(&self) -> Req {
        match (self.before, self.after) {
            (Some(before), None) => Req::Before(before, self.limit),
            (None, Some(after)) => Req::After(after, self.limit),
            _ => panic!("a request has exactly one of before / after"),
        }
    }
}

/// 向量在本仓同级的客户端 SDK 规格里。
fn vectors() -> Vectors {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../flare-im-core-client-sdk/sdk-spec/golden/view/timeline-paging.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|error| panic!("decode {}: {error}", path.display()))
}

async fn run_step(h: &Harness, step: &Step, at: &str) -> Result<()> {
    match step.action.as_str() {
        "open" => h.open().await,
        "openAround" => return h.open_around(step.message_id.as_deref(), step.seq).await,
        "loadOlder" => return h.load_older().await.map(|_| ()),
        "loadNewer" => return h.load_newer().await.map(|_| ()),
        "jumpToLatest" => {
            h.jump_to_latest().await;
        }
        "offline" => h.server.offline.store(true, Ordering::SeqCst),
        "online" => h.server.offline.store(false, Ordering::SeqCst),
        "liveMessage" => {
            h.live_message(step.seq.expect("liveMessage has a seq"))
                .await
        }
        "send" => {
            h.send(
                step.client_msg_id
                    .as_deref()
                    .expect("send has a clientMsgId"),
            )
            .await
        }
        "ack" => {
            h.ack(
                step.client_msg_id
                    .as_deref()
                    .expect("ack has a clientMsgId"),
                step.seq.expect("ack has a seq"),
            )
            .await
        }
        "gapFilled" => {
            h.gap_filled(
                step.from.expect("gapFilled has from"),
                step.to.expect("gapFilled has to"),
            )
            .await
        }
        "event" => {
            h.event(step.kind.as_deref().expect("event has a kind"))
                .await
        }
        other => panic!("{at}: unknown step {other}"),
    }
    Ok(())
}

async fn run_cases(section: &str, cases: Vec<Case>) {
    assert!(!cases.is_empty(), "{section} lists its cases");
    for case in cases {
        assert!(!case.boundary_case.is_empty(), "{}", case.name);
        let h = Harness::new(Setup {
            page_size: case.page_size,
            max_seq: case.conversation.max_seq,
            visible_after_seq: case.conversation.visible_after_seq,
            server: &case.server.visible_messages,
            local: &case.local.messages,
            coverage: &case.local.coverage,
        })
        .await;
        for (index, step) in case.steps.iter().enumerate() {
            let at = format!(
                "{section} / {} / step {} ({})",
                case.name,
                index + 1,
                step.action
            );
            let outcome = run_step(&h, step, &at).await;
            let requests = h.server.take_requests();
            let updates = h.take_updates();
            assert_eq!(
                h.server.cursor_syncs.load(Ordering::SeqCst),
                0,
                "{at}: the view never moves the sync cursor"
            );
            let Some(expect) = &step.expect else {
                assert!(outcome.is_ok(), "{at}: {outcome:?}");
                continue;
            };
            assert_eq!(outcome.is_err(), expect.error, "{at}: {outcome:?}");
            if let (Some(reason), Err(error)) = (&expect.error_reason, &outcome) {
                assert_eq!(error.reason(), reason.as_str(), "{at}: error reason");
            }
            if let Some(expected) = &expect.requests {
                let expected = expected.iter().map(Request::req).collect::<Vec<_>>();
                assert_eq!(requests, expected, "{at}: requests");
            }
            if let Some(expected) = &expect.coverage {
                assert_eq!(&h.coverage().await, expected, "{at}: coverage");
            }
            if let Some(expected) = &expect.updates {
                let kinds = updates
                    .iter()
                    .map(|update| {
                        serde_json::to_value(&update.kind)
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_string()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(&kinds, expected, "{at}: updates");
            }
            if h.view_id().is_empty() {
                continue;
            }
            let state = h.state().await;
            if let Some(expected) = &expect.window {
                assert_eq!(state.seqs, expand(expected), "{at}: window");
            }
            if let Some(expected) = &expect.pending {
                assert_eq!(&state.pending, expected, "{at}: pending rows");
            }
            if let Some(expected) = expect.has_older {
                assert_eq!(state.has_older, expected, "{at}: hasOlder");
            }
            if let Some(expected) = expect.has_newer {
                assert_eq!(state.has_newer, expected, "{at}: hasNewer");
            }
            if let Some(expected) = expect.attached_to_latest {
                assert_eq!(state.attached_to_latest, expected, "{at}: attachedToLatest");
            }
            if let Some(expected) = &expect.anchor_key {
                assert_eq!(&state.anchor_key, expected, "{at}: anchorKey");
            }
        }
    }
}

#[tokio::test]
async fn timeline_paging_follows_the_shared_vectors() {
    let vectors = vectors();
    assert_eq!(vectors.schema_version, 1);
    assert!(
        vectors.older_paging.cases.len() >= 6,
        "every older-paging boundary case is listed"
    );
    run_cases("olderPaging", vectors.older_paging.cases).await;
    run_cases("newerPaging", vectors.newer_paging.cases).await;
    run_cases("anchors", vectors.anchors.cases).await;
    run_cases("liveUpdates", vectors.live_updates.cases).await;
}
