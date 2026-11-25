//! 冷启动基线：一个进程只量一次（真正的冷启动），由外部脚本按进程重复跑、取中位数做前后对比。
//!
//! 量四步，每步给出宿主线程上的同步执行时间（这一步的 future 各次 poll 耗时之和，宿主线程被占住的
//! 时间）与墙钟时间：`IMClient::new` → `init`（含接入运行时与本地存储）→ `prepare`（SQLite）→
//! `bootstrap_startup_home`（只读本地，首屏可交互）。不连网络。
//!
//! 需要 `FLARE_COLD_START_ROOT`（数据目录）才会跑；输出一行 `COLD_START {json}`（微秒）。
//! `FLARE_COLD_START_FRESH=1` 先清空数据目录（首次安装），否则沿用（再次启动）。
//! `cargo test -p flare-im-core-sdk --features lifecycle-sqlite --test cold_start_baseline -- --ignored --nocapture`

#![cfg(all(not(target_arch = "wasm32"), feature = "lifecycle-sqlite"))]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use flare_im_core_sdk::client::IMClient;

use flare_im_core_sdk::model::StartupHomeSyncRequest;
use flare_im_core_sdk::prelude::SdkConfigOverlay;

/// 记下每次 poll 的耗时：总和就是这一步在调用线程上同步执行的时间。
struct PollTimed<F> {
    inner: Pin<Box<F>>,
    spent: Duration,
}

impl<F: Future> Future for PollTimed<F> {
    type Output = (F::Output, Duration);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let started = Instant::now();
        let polled = this.inner.as_mut().poll(cx);
        this.spent += started.elapsed();
        match polled {
            Poll::Ready(output) => Poll::Ready((output, this.spent)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 在调用线程上跑完一步：返回结果、同步执行时间、墙钟时间。
fn step<F: Future>(
    runtime: &tokio::runtime::Runtime,
    future: F,
) -> (F::Output, Duration, Duration) {
    let started = Instant::now();
    let (output, spent) = runtime.block_on(PollTimed {
        inner: Box::pin(future),
        spent: Duration::ZERO,
    });
    (output, spent, started.elapsed())
}

/// 初始化本机地址，用于测量本地准备阶段。
fn overlay(root: &std::path::Path) -> SdkConfigOverlay {
    let endpoints = Some(flare_im_core_sdk::client::CoreEndpoints {
        base_url: "http://127.0.0.1:9".into(),
        websocket_url: "ws://127.0.0.1:9".into(),
        quic_url: None,
        media_base_url: None,
    });
    SdkConfigOverlay {
        data_url: Some(root.display().to_string()),
        endpoints,
        ..Default::default()
    }
}

#[test]
#[ignore = "baseline harness: run once per process from the comparison script"]
fn cold_start_once() {
    let Some(root) = std::env::var_os("FLARE_COLD_START_ROOT").map(std::path::PathBuf::from) else {
        eprintln!("FLARE_COLD_START_ROOT is not set; nothing measured");
        return;
    };
    if std::env::var_os("FLARE_COLD_START_FRESH").is_some() {
        let _ = std::fs::remove_dir_all(&root);
    }
    std::fs::create_dir_all(&root).expect("data root");
    // 宿主自己的运行时（与 SDK 无关），不计入。
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");

    let started = Instant::now();
    let client = runtime.block_on(async { IMClient::new() });
    let new = started.elapsed();
    let (init, init_sync, init_wall) = step(
        &runtime,
        client.init(Some("development".into()), Some(overlay(&root))),
    );
    init.expect("init");
    let (prepare, prepare_sync, prepare_wall) = step(
        &runtime,
        client.prepare("cold-start-user", flare_im_core_sdk_sqlite_im::sqlite()),
    );
    prepare.expect("prepare");
    let (home, home_sync, home_wall) = step(
        &runtime,
        client.bootstrap_startup_home(StartupHomeSyncRequest {
            conversation_limit: 50,
            start_background_convergence: false,
            ..Default::default()
        }),
    );
    home.expect("local startup home");
    let total_wall = started.elapsed();
    let us = |duration: Duration| duration.as_micros() as u64;
    println!(
        "COLD_START {}",
        serde_json::json!({
            "newUs": us(new),
            "initSyncUs": us(init_sync),
            "initWallUs": us(init_wall),
            "prepareSyncUs": us(prepare_sync),
            "prepareWallUs": us(prepare_wall),
            "homeSyncUs": us(home_sync),
            "homeWallUs": us(home_wall),
            "totalSyncUs": us(new + init_sync + prepare_sync + home_sync),
            "totalWallUs": us(total_wall),
        })
    );
    // 不登出：再次启动的轮次要沿用这份本地数据。
    drop(client);
}

fn storage_root() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::var_os("FLARE_COLD_START_ROOT").expect("data root"))
        .join("storage-baseline")
}

async fn open_storage_client(root: &std::path::Path) -> IMClient {
    let client = IMClient::new();
    client
        .init(Some("development".into()), Some(overlay(root)))
        .await
        .unwrap();
    client
        .prepare("baseline-user", flare_im_core_sdk_sqlite_im::sqlite())
        .await
        .unwrap();
    client
}

/// 同一 SQLite 用户库上测量 20 个 32 条文本消息的小批次，随后向断网队列连续投递 64 条。
/// 只创建新的样本子目录，避免历史数据改变负载或覆盖调用方的数据。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "local SQLite baseline: set FLARE_COLD_START_ROOT to a fresh temporary directory"]
async fn sqlite_batch_and_offline_queue() {
    let root = storage_root();
    std::fs::create_dir(&root).expect("fresh storage baseline directory");
    let client = open_storage_client(&root).await;
    let stores = client.stores_async().await.unwrap();
    let builder = client.message_build().unwrap();
    let template = builder
        .create_text("group-baseline-history", "baseline text", false, &[])
        .await
        .unwrap();
    let mut timings = Vec::new();
    for batch in 0..20 {
        let messages: Vec<_> = (1..=32)
            .map(|offset| {
                let seq = batch * 32 + offset;
                let mut message = template.clone();
                message.server_id = format!("server-{seq}");
                message.client_msg_id = format!("client-{seq}");
                message.conversation_seq = seq;
                message.local_state = Default::default();
                message
            })
            .collect();
        let start = Instant::now();
        stores.messages.save_batch(&messages).await.unwrap();
        timings.push(start.elapsed().as_micros());
    }
    assert_eq!(
        stores
            .messages
            .get_by_conversation("group-baseline-history", 0, 640)
            .await
            .unwrap()
            .len(),
        640
    );
    let api = client.message_async().await.unwrap();
    let pending = stores.pending_send_reader.expect("SQLite outbox");
    assert!(pending.list().await.unwrap().is_empty());
    let mut sent = Vec::new();
    for _ in 0..64 {
        let draft = builder
            .create_text("group-baseline-outbox", "queued offline", false, &[])
            .await
            .unwrap();
        sent.push(draft.client_msg_id.clone());
        api.send(draft).await.unwrap();
    }
    // 此次负载没有网络或 ACK，末次入队后的持久条数就是队列深度峰值。
    let queued: Vec<_> = pending
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.client_msg_id)
        .collect();
    assert_eq!(queued, sent);
    timings.sort_unstable();
    let p95_index = (95 * timings.len()).div_ceil(100) - 1;
    println!(
        "SQLITE_BASELINE {}",
        serde_json::json!({
            "batchSize": 32,
            "batchCount": timings.len(),
            "batchP95Us": timings[p95_index],
            "peakPending": queued.len(),
            "pendingIds": queued,
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "second process of the SQLite outbox baseline"]
async fn read_reopened_outbox() {
    let client = open_storage_client(&storage_root()).await;
    let stores = client.stores_async().await.unwrap();
    let reopened: Vec<_> = stores
        .pending_send_reader
        .expect("SQLite outbox")
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.client_msg_id)
        .collect();
    assert_eq!(reopened.len(), 64);
    println!(
        "REOPENED_OUTBOX {}",
        serde_json::to_string(&reopened).unwrap()
    );
}
