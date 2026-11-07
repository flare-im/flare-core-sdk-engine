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
use flare_im_core_sdk::client::lifecycle::LoginDbKind;
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

/// 初始化配置：开发模式接入（不会连接）。`FLARE_COLD_START_NO_ACCESS=1` 不给接入配置
/// （接入运行时不装配、不开 `access.db`），用来把接入运行时的开销单独量出来。
fn overlay(root: &std::path::Path) -> SdkConfigOverlay {
    use flare_im_core_sdk::prelude::{AccessConfig, DevelopmentEndpoints};
    let access = std::env::var_os("FLARE_COLD_START_NO_ACCESS")
        .is_none()
        .then(|| {
            AccessConfig::development(DevelopmentEndpoints {
                ws_url: Some("ws://127.0.0.1:9".into()),
                ..Default::default()
            })
        });
    SdkConfigOverlay {
        data_url: Some(root.display().to_string()),
        access,
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
    let (init, init_sync, init_wall) = step(&runtime, client.init(None, Some(overlay(&root))));
    init.expect("init");
    let (prepare, prepare_sync, prepare_wall) = step(
        &runtime,
        client.prepare("cold-start-user", LoginDbKind::Sqlite),
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
