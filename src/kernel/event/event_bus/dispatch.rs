use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use tracing::warn;

use super::super::types::SdkEvent;
use super::{EventRoute, FnAny};
#[cfg(target_arch = "wasm32")]
use crate::shared::util::{delay, spawn_background};
#[cfg(target_arch = "wasm32")]
use std::time::Duration;

const CALLBACK_DISPATCH_CAPACITY: usize = 8192;
const REPLAY_DISPATCH_CAPACITY: usize = 1024;
const REPLAY_DELAY_MS: u64 = 10;
static CALLBACK_DISPATCH_DROPPED: AtomicU64 = AtomicU64::new(0);
static REPLAY_DISPATCH_DROPPED: AtomicU64 = AtomicU64::new(0);
/// 已丢弃但尚未向应用层通报的回调数（见 [`take_unreported_callback_drops`]）。
static CALLBACK_DROP_UNREPORTED: AtomicU64 = AtomicU64::new(0);

/// Single long-lived event dispatch thread sender.
///
/// Replaces per-publish thread spawning with a bounded FIFO queue. Callback
/// invocation order follows publish order and callback panics are contained per job.
#[cfg(not(target_arch = "wasm32"))]
fn event_dispatch_sender() -> &'static std::sync::mpsc::SyncSender<Box<dyn FnOnce() + Send>> {
    use std::sync::OnceLock;
    static DISPATCH: OnceLock<std::sync::mpsc::SyncSender<Box<dyn FnOnce() + Send>>> =
        OnceLock::new();
    DISPATCH.get_or_init(|| {
        let (tx, rx) =
            std::sync::mpsc::sync_channel::<Box<dyn FnOnce() + Send>>(CALLBACK_DISPATCH_CAPACITY);
        if let Err(error) = std::thread::Builder::new()
            .name("flare-sdk-event-dispatch".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    job();
                }
            })
        {
            warn!(%error, "failed to spawn flare-sdk-event-dispatch thread");
        }
        tx
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn spawn_callback(f: impl FnOnce() + Send + 'static) {
    let job = Box::new(move || {
        if catch_unwind(AssertUnwindSafe(f)).is_err() {
            warn!("EventBus callback panicked; continuing");
        }
    }) as Box<dyn FnOnce() + Send>;
    match event_dispatch_sender().try_send(job) {
        Ok(()) => {}
        Err(std::sync::mpsc::TrySendError::Full(_)) => {
            let dropped = CALLBACK_DISPATCH_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
            CALLBACK_DROP_UNREPORTED.fetch_add(1, Ordering::Relaxed);
            if dropped == 1 || dropped.is_multiple_of(1024) {
                warn!(
                    total_dropped = dropped,
                    "EventBus callback dispatch queue full; callback dropped"
                );
            }
        }
        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
            CALLBACK_DROP_UNREPORTED.fetch_add(1, Ordering::Relaxed);
            warn!("EventBus dispatch thread unavailable; callback dropped");
        }
    }
}

/// 取走「尚未向应用层通报」的回调丢弃计数（取走即清零）。
///
/// 回调队列满时丢弃是有意的削峰，但 `on_message` 这类 `Fn` 回调消费者一旦丢了就是丢了
/// （raw subscriber 有 lag→ResyncNeeded 兜底，回调没有）→ 消息不上屏且无重同步提示。
/// `EventBus::publish` 在下一次发布前取走该计数并补发一条 `ResyncNeeded`。
///
/// **进程级语义**：分发队列本身是进程级单例（一条 `flare-sdk-event-dispatch` 线程供所有
/// EventBus 共用），故计数也是进程级——由**下一个发布的 bus** 通报。单 client（当前唯一的
/// 构造路径 `client/builder.rs`）下等价于按 bus 归属；若将来支持多账号多 bus 并存，需把
/// 计数下放到 bus 并由 `dispatch_callbacks*` 逐层传入，否则 A 号的丢弃会通报到 B 号。
pub(super) fn take_unreported_callback_drops() -> u64 {
    CALLBACK_DROP_UNREPORTED.swap(0, Ordering::AcqRel)
}

#[cfg(target_arch = "wasm32")]
pub(super) fn spawn_callback(f: impl FnOnce() + 'static) {
    spawn_background(async move {
        if catch_unwind(AssertUnwindSafe(f)).is_err() {
            warn!("EventBus callback panicked; continuing");
        }
    });
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn replay_after_dispatch_window(f: impl FnOnce() + Send + 'static) {
    match replay_dispatch_sender().try_send(Box::new(f)) {
        Ok(()) => {}
        Err(std::sync::mpsc::TrySendError::Full(_)) => {
            let dropped = REPLAY_DISPATCH_DROPPED.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped == 1 || dropped.is_multiple_of(1024) {
                warn!(
                    total_dropped = dropped,
                    "EventBus replay dispatch queue full; replay callback dropped"
                );
            }
        }
        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
            warn!("EventBus replay dispatch thread unavailable; replay callback dropped");
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn replay_dispatch_sender() -> &'static std::sync::mpsc::SyncSender<Box<dyn FnOnce() + Send>> {
    use std::sync::OnceLock;
    static REPLAY: OnceLock<std::sync::mpsc::SyncSender<Box<dyn FnOnce() + Send>>> =
        OnceLock::new();
    REPLAY.get_or_init(|| {
        let (tx, rx) =
            std::sync::mpsc::sync_channel::<Box<dyn FnOnce() + Send>>(REPLAY_DISPATCH_CAPACITY);
        if let Err(error) = std::thread::Builder::new()
            .name("flare-sdk-event-replay".into())
            .spawn(move || {
                while let Ok(first) = rx.recv() {
                    std::thread::sleep(std::time::Duration::from_millis(REPLAY_DELAY_MS));
                    spawn_callback(first);
                    while let Ok(job) = rx.try_recv() {
                        spawn_callback(job);
                    }
                }
            })
        {
            warn!(%error, "failed to spawn flare-sdk-event-replay thread");
        }
        tx
    })
}

#[cfg(target_arch = "wasm32")]
pub(super) fn replay_after_dispatch_window(f: impl FnOnce() + 'static) {
    spawn_background(async move {
        delay(Duration::from_millis(REPLAY_DELAY_MS)).await;
        if catch_unwind(AssertUnwindSafe(f)).is_err() {
            warn!("EventBus callback panicked; continuing");
        }
    });
}

pub(super) trait RecoverableRwLock<T> {
    fn safe_read(&self, name: &'static str) -> RwLockReadGuard<'_, T>;
    fn safe_write(&self, name: &'static str) -> RwLockWriteGuard<'_, T>;
}

impl<T> RecoverableRwLock<T> for RwLock<T> {
    fn safe_read(&self, name: &'static str) -> RwLockReadGuard<'_, T> {
        match self.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!(
                    lock = name,
                    "EventBus lock poisoned; recovering read access"
                );
                poisoned.into_inner()
            }
        }
    }

    fn safe_write(&self, name: &'static str) -> RwLockWriteGuard<'_, T> {
        match self.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!(
                    lock = name,
                    "EventBus lock poisoned; recovering write access"
                );
                poisoned.into_inner()
            }
        }
    }
}

fn callback_snapshot<T: Clone>(callbacks: &Arc<RwLock<Vec<T>>>) -> Vec<T> {
    callbacks.safe_read("event_bus").clone()
}

pub(super) fn dispatch_callbacks<T, F>(
    lifetime: &super::SessionLifetime,
    callbacks: &Arc<RwLock<Vec<T>>>,
    invoke: F,
) -> usize
where
    T: Clone + Send + 'static,
    F: Fn(&T) + Send + 'static,
{
    let callbacks = callback_snapshot(callbacks);
    let count = callbacks.len();
    if callbacks.is_empty() {
        return 0;
    }

    let lifetime = lifetime.clone();
    spawn_callback(move || {
        for callback in callbacks {
            if lifetime.retired() {
                break;
            }
            invoke(&callback);
        }
    });
    count
}

pub(super) fn dispatch_callbacks_with<T, P, M, F>(
    lifetime: &super::SessionLifetime,
    callbacks: &Arc<RwLock<Vec<T>>>,
    make_payload: M,
    invoke: F,
) -> usize
where
    T: Clone + Send + 'static,
    P: Send + 'static,
    M: FnOnce() -> P,
    F: Fn(&T, &P) + Send + 'static,
{
    let callbacks = callback_snapshot(callbacks);
    let count = callbacks.len();
    if callbacks.is_empty() {
        return 0;
    }

    let payload = make_payload();
    let lifetime = lifetime.clone();
    spawn_callback(move || {
        for callback in callbacks {
            if lifetime.retired() {
                break;
            }
            invoke(&callback, &payload);
        }
    });
    count
}

pub(super) fn dispatch_any_callbacks(
    lifetime: &super::SessionLifetime,
    callbacks: &Arc<RwLock<Vec<FnAny>>>,
    event: SdkEvent,
) -> usize {
    let callbacks = callback_snapshot(callbacks);
    let count = callbacks.len();
    if callbacks.is_empty() {
        return 0;
    }

    let event = Arc::new(event);
    let lifetime = lifetime.clone();
    spawn_callback(move || {
        for callback in callbacks {
            if lifetime.retired() {
                break;
            }
            callback(Arc::clone(&event));
        }
    });
    count
}

pub(super) fn dispatch_route_callbacks(
    lifetime: &super::SessionLifetime,
    routes: &Arc<RwLock<Vec<EventRoute>>>,
    event: &SdkEvent,
) -> usize {
    let callbacks = {
        let routes = routes.safe_read("event_bus");
        routes
            .iter()
            .filter(|route| route.filter.matches(event))
            .map(|route| route.callback.clone())
            .collect::<Vec<_>>()
    };
    let count = callbacks.len();
    if callbacks.is_empty() {
        return 0;
    }

    let event = Arc::new(event.clone());
    let lifetime = lifetime.clone();
    spawn_callback(move || {
        for callback in callbacks {
            if lifetime.retired() {
                break;
            }
            callback(Arc::clone(&event));
        }
    });
    count
}
