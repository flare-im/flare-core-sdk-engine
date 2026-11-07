//! 跨端互通：核心层判据。
//!
//! 五端（web / Tauri / Flutter / iOS / Android）共用同一个 Rust 核心，所以"功能通不通"
//! 由核心与协议决定，一套测试即可覆盖五端；真正会分叉的是各端 UI 接线，那部分靠
//! 逐端对照，不靠把同一个核心测五遍。
//!
//! 本用例开两个真实会话（观察端 A + 动作端 B）连生产网关，B 依次执行动作，
//! 断言 A 端收到对应事件。**只认推送**：任何断言都不靠主动 sync 兜底，
//! 否则推送坏了也会报绿。
//!
//! 默认跳过。要跑就给环境变量——**别把 token 写进仓库**：
//!   FLARE_E2E_WS_URL=ws://<host>/ws \
//!   FLARE_E2E_USER_A=<观察端> FLARE_E2E_TOKEN_A="$(ssh <server> mint_token.py <a>)" \
//!   FLARE_E2E_USER_B=<动作端> FLARE_E2E_TOKEN_B="$(ssh <server> mint_token.py <b>)" \
//!   cargo test --features lifecycle-sqlite --test cross_client_interop_test -- --nocapture

use std::env;
use std::time::Duration;

use flare_im_core_sdk::SdkEvent;
use flare_im_core_sdk::client::IMClient;
use flare_im_core_sdk::client::lifecycle::{LoginDbKind, SdkConfigOverlay};
use flare_im_core_sdk::model::StartupHomeSyncRequest;
use flare_im_core_sdk::prelude::MessageEvent;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

fn env_var(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

/// 一个已登录的核心会话。
struct Session {
    client: IMClient,
    #[allow(dead_code)]
    root: std::path::PathBuf,
}

async fn login(ws_url: &str, user_id: &str, token: &str) -> (Session, UnboundedReceiver<SdkEvent>) {
    let root = std::env::temp_dir().join(format!(
        "flare-interop-{user_id}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create data root");

    let client = IMClient::new();
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(format!("file://{}", root.display())),
                // 联调：开发模式接入，端点就是给定的网关。
                access: Some(flare_im_core_sdk::prelude::AccessConfig::development(
                    flare_im_core_sdk::prelude::DevelopmentEndpoints {
                        ws_url: Some(ws_url.to_string()),
                        ..Default::default()
                    },
                )),
                tenant_id: Some("0".to_string()),
                ..Default::default()
            }),
        )
        .await
        .expect("init");

    let (tx, rx) = unbounded_channel::<SdkEvent>();
    client
        .login(user_id, Some(token), LoginDbKind::Sqlite, move |bus, _| {
            let mut raw = bus.subscribe_shared_raw();
            tokio::spawn(async move {
                while let Ok(event) = raw.recv().await {
                    if tx.send(event.cloned_event()).is_err() {
                        break;
                    }
                }
            });
        })
        .await
        .unwrap_or_else(|e| panic!("{user_id} 登录失败: {e}"));

    client
        .bootstrap_startup_home(StartupHomeSyncRequest {
            conversation_limit: 50,
            start_background_convergence: true,
            ..Default::default()
        })
        .await
        .expect("冷启首屏同步");

    (Session { client, root }, rx)
}

/// 等一个满足条件的事件；超时即失败（不做主动 sync 兜底）。
async fn wait_for<F>(rx: &mut UnboundedReceiver<SdkEvent>, what: &str, mut pred: F) -> SdkEvent
where
    F: FnMut(&SdkEvent) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(event)) => {
                if pred(&event) {
                    return event;
                }
            }
            Ok(None) => panic!("事件通道已关闭，等待 [{what}] 失败"),
            Err(_) => panic!("60 秒内没有等到 [{what}]"),
        }
    }
}

fn text_of(message: &flare_im_core_sdk::model::IMMessage) -> String {
    format!("{message:?}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_client_actions_reach_the_other_side() {
    let (Some(ws), Some(ua), Some(ta), Some(ub), Some(tb)) = (
        env_var("FLARE_E2E_WS_URL"),
        env_var("FLARE_E2E_USER_A"),
        env_var("FLARE_E2E_TOKEN_A"),
        env_var("FLARE_E2E_USER_B"),
        env_var("FLARE_E2E_TOKEN_B"),
    ) else {
        return;
    };

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();

    let (observer, mut events) = login(&ws, &ua, &ta).await;
    let (actor, mut actor_events) = login(&ws, &ub, &tb).await;

    // 两端在同一个会话里：按成员取群，双方都能定位到同一个 cid。
    let members = vec![ua.clone(), ub.clone()];
    let conversation = actor
        .client
        .conversation_async()
        .await
        .expect("conversation api")
        .get_group_by_user_ids(&members, Some("interop"))
        .await
        .expect("取/建互通测试会话");
    let cid = conversation.conversation_id.clone();
    let _ = observer
        .client
        .conversation_async()
        .await
        .expect("conversation api")
        .get_group_by_user_ids(&members, Some("interop"))
        .await
        .expect("观察端也要物化这个会话");
    println!("INTEROP_CID={cid}");

    let tag = format!(
        "interop-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs()
    );
    let mut passed: Vec<&str> = Vec::new();

    // ---- 1. 文本消息 ----
    let draft = actor
        .client
        .message_build()
        .expect("builder")
        .create_text(&cid, &tag, false, &[])
        .await
        .expect("构建文本");
    actor
        .client
        .message_async()
        .await
        .expect("message api")
        .send(draft)
        .await
        .expect("发送");

    // server_id 由服务端在落库时分配，从**观察端收到的那条**上取——
    // 后续所有动作都以它为目标，这样也顺带证明两端拿到的是同一个 id。
    let mut server_id = String::new();
    wait_for(&mut events, "文本消息", |event| {
        if let SdkEvent::Message(MessageEvent::ReceivedBatch { messages }) = event
            && let Some(hit) = messages.iter().find(|m| text_of(m).contains(&tag))
        {
            server_id = hit.server_id.clone();
            return true;
        }
        false
    })
    .await;
    assert!(!server_id.is_empty(), "观察端收到的消息必须带 server_id");
    passed.push("1 文本消息");

    // ---- 2. 表情回应 ----
    actor
        .client
        .message_async()
        .await
        .expect("message api")
        .add_reaction(&server_id, "👍")
        .await
        .expect("加表情");
    wait_for(&mut events, "表情回应", |event| {
        matches!(
            event,
            SdkEvent::Message(MessageEvent::ReactionChanged { .. })
        )
    })
    .await;
    passed.push("2 表情回应");

    // ---- 3. 编辑 ----
    let edited = format!("{tag}-edited");
    actor
        .client
        .message_async()
        .await
        .expect("message api")
        .edit_text_by_message_id(&server_id, &edited)
        .await
        .expect("编辑");
    wait_for(&mut events, "编辑", |event| {
        matches!(event, SdkEvent::Message(MessageEvent::Edited { .. }))
    })
    .await;
    passed.push("3 编辑");

    // ---- 4. 撤回 ----
    actor
        .client
        .message_async()
        .await
        .expect("message api")
        .recall(&server_id)
        .await
        .expect("撤回");
    wait_for(&mut events, "撤回", |event| {
        matches!(event, SdkEvent::Message(MessageEvent::Recalled { .. }))
    })
    .await;
    passed.push("4 撤回");

    // ---- 5. 正在输入 ----
    //
    // 输入中按设计是**可折叠、带 TTL 的尽力而为信号**：客户端侧 3s 节流，
    // 网关侧按会话 1s 合并窗口 + 6s TTL。真实客户端是按心跳重复发的，
    // 所以判据也必须按心跳来 —— 断言"单次 typing 必须被看到"是过度规定，
    // 实测约每 5 轮会假失败一次。
    let typing_actor = actor.client.message_async().await.expect("message api");
    let typing_cid = cid.clone();
    let typing_pulse = tokio::spawn(async move {
        for _ in 0..8 {
            let _ = typing_actor.typing(&typing_cid, true).await;
            tokio::time::sleep(Duration::from_millis(1_200)).await;
        }
    });
    wait_for(&mut events, "正在输入", |event| {
        matches!(
            event,
            SdkEvent::Message(MessageEvent::Typing { .. })
                | SdkEvent::Message(MessageEvent::TypingAggregate { .. })
        )
    })
    .await;
    typing_pulse.abort();
    passed.push("5 正在输入");

    // 后面几项需要一条**没被撤回**的消息，重新发一条。
    let tag2 = format!("{tag}-b");
    let draft2 = actor
        .client
        .message_build()
        .expect("builder")
        .create_text(&cid, &tag2, false, &[])
        .await
        .expect("构建第二条");
    actor
        .client
        .message_async()
        .await
        .expect("message api")
        .send(draft2)
        .await
        .expect("发送第二条");
    let mut second_id = String::new();
    wait_for(&mut events, "第二条文本", |event| {
        if let SdkEvent::Message(MessageEvent::ReceivedBatch { messages }) = event
            && let Some(hit) = messages.iter().find(|m| text_of(m).contains(&tag2))
        {
            second_id = hit.server_id.clone();
            return true;
        }
        false
    })
    .await;

    // ---- 6. 已读回执：观察端已读 → **动作端**（发送者）应看到 ----
    // 已读到第二条所在的位点：seq 从观察端本地那条消息上取，不猜。
    let read_seq = observer
        .client
        .message_async()
        .await
        .expect("message api")
        .get(&second_id)
        .await
        .expect("取本地第二条")
        .map(|m| m.conversation_seq)
        .expect("观察端本地必须已有第二条");
    observer
        .client
        .conversation_async()
        .await
        .expect("conversation api")
        .mark_read(&cid, read_seq)
        .await
        .expect("标记已读");
    wait_for(&mut actor_events, "已读回执", |event| {
        matches!(event, SdkEvent::Message(MessageEvent::ReadReceipt { .. }))
    })
    .await;
    passed.push("6 已读回执");

    // ---- 7. 置顶 / 取消置顶 ----
    actor
        .client
        .message_async()
        .await
        .expect("message api")
        .pin_by_message_id(&second_id, 0)
        .await
        .expect("置顶");
    wait_for(&mut events, "置顶", |event| {
        matches!(event, SdkEvent::Message(MessageEvent::Pinned { .. }))
    })
    .await;
    actor
        .client
        .message_async()
        .await
        .expect("message api")
        .unpin_by_message_id(&second_id, 0)
        .await
        .expect("取消置顶");
    wait_for(&mut events, "取消置顶", |event| {
        matches!(event, SdkEvent::Message(MessageEvent::Unpinned { .. }))
    })
    .await;
    passed.push("7 置顶/取消");

    // ---- 8. 对所有人删除 ----
    actor
        .client
        .message_async()
        .await
        .expect("message api")
        .delete_for_everyone(&second_id, None)
        .await
        .expect("对所有人删除");
    wait_for(&mut events, "对所有人删除", |event| {
        matches!(event, SdkEvent::Message(MessageEvent::Deleted { .. }))
    })
    .await;
    passed.push("8 对所有人删除");

    // ---- 9. @全员：中文记号必须真的产出 mentionAll ----
    //
    // 这条曾经在五端都不成立：判定规则只有 Vue kit 里的 `/(^|\s)@all(\s|$)/i`，
    // 中文打 `@全员` 不生效，其余四端根本不解析。现在规则在核心，
    // 判据是**对端收到的那条消息**上的 mentionAll，不是本地构建时的入参。
    let tag_all = format!("{tag}-atall");
    let draft_all = actor
        .client
        .message_build()
        .expect("builder")
        .create_text(&cid, &format!("@全员 {tag_all}"), false, &[])
        .await
        .expect("构建 @全员 消息");
    assert!(
        draft_all.mention_all,
        "核心必须从正文解析出 @全员 —— 入参给的是 false"
    );
    actor
        .client
        .message_async()
        .await
        .expect("message api")
        .send(draft_all)
        .await
        .expect("发送 @全员");
    let mut received_mention_all = false;
    wait_for(&mut events, "@全员 消息", |event| {
        if let SdkEvent::Message(MessageEvent::ReceivedBatch { messages }) = event
            && let Some(hit) = messages.iter().find(|m| text_of(m).contains(&tag_all))
        {
            received_mention_all = hit.mention_all;
            return true;
        }
        false
    })
    .await;
    assert!(
        received_mention_all,
        "对端收到的消息必须带 mentionAll —— 否则 @全员 在跨端这一段丢了"
    );
    passed.push("9 @全员(中文)");

    println!("INTEROP_PASSED={}", passed.join(" | "));
}
