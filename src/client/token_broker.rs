//! 宿主托管令牌的续期桥：给跨语言宿主（C ABI、Tauri 前端）用的 [`ConnectTokenRefresher`]。
//!
//! 这些宿主没法把一个 Rust 异步函数交给核心，只能「收到通知 → 自己去换令牌 → 回调交回」。
//! 桥把这一来一回变成一次 `await`：核心要新令牌时分配一个请求号并通知宿主，宿主换到令牌后
//! 用 [`HostTokenBroker::complete`] 按请求号交回。没交回（宿主无响应、核心等待超时）时请求号
//! 随等待方一起作废，迟到的交回会被忽略，不会写进下一轮。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::client::token_provider::ConnectTokenRefresher;

/// 同时挂起的续期请求上限。核心每代会话只会单飞一次续期，超过说明宿主一直不答，新请求直接按
/// 「没换到」处理，避免挂起表无界增长。
const MAX_PENDING_RENEWALS: usize = 8;

type Notify = dyn Fn(u64) + Send + Sync;

struct BrokerInner {
    next_request_id: AtomicU64,
    pending: Mutex<HashMap<u64, oneshot::Sender<Option<String>>>>,
    notify: Box<Notify>,
}

/// 续期请求与交回之间的配对表。克隆共享同一张表。
#[derive(Clone)]
pub struct HostTokenBroker {
    inner: Arc<BrokerInner>,
}

impl HostTokenBroker {
    /// `notify(request_id)` 在核心需要新令牌时被调用，运行在 SDK 的后台线程上；宿主应尽快返回，
    /// 把换令牌放到自己的线程里做，完成后调用 [`HostTokenBroker::complete`]。
    pub fn new(notify: impl Fn(u64) + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(BrokerInner {
                next_request_id: AtomicU64::new(1),
                pending: Mutex::new(HashMap::new()),
                notify: Box::new(notify),
            }),
        }
    }

    /// 交回请求号 `request_id` 的结果：`Some(token)` 为换到的连接令牌，`None` 为换不到（会话已失效）。
    /// 请求号未知或已作废时返回 `false`。
    pub fn complete(&self, request_id: u64, token: Option<String>) -> bool {
        let sender = self
            .inner
            .pending
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&request_id));
        match sender {
            Some(sender) => sender.send(token).is_ok(),
            None => false,
        }
    }

    /// 装进 [`crate::client::IMClient::set_connect_token_refresher`] 的回调。
    pub fn refresher(&self) -> Arc<dyn ConnectTokenRefresher> {
        Arc::new(self.clone())
    }

    fn begin(&self) -> Option<(PendingGuard, oneshot::Receiver<Option<String>>)> {
        let request_id = self.inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.inner.pending.lock().ok()?;
            if pending.len() >= MAX_PENDING_RENEWALS {
                tracing::warn!(
                    pending = pending.len(),
                    "connect token renewals are piling up unanswered by the host"
                );
                return None;
            }
            pending.insert(request_id, tx);
        }
        Some((
            PendingGuard {
                inner: Arc::clone(&self.inner),
                request_id,
            },
            rx,
        ))
    }
}

/// 等待方被丢弃（核心超时或会话结束）时把请求号从挂起表里摘掉。
struct PendingGuard {
    inner: Arc<BrokerInner>,
    request_id: u64,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.inner.pending.lock() {
            pending.remove(&self.request_id);
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl ConnectTokenRefresher for HostTokenBroker {
    async fn refresh_connect_token(&self) -> Option<String> {
        let (guard, rx) = self.begin()?;
        (self.inner.notify)(guard.request_id);
        let token = rx.await.ok().flatten();
        drop(guard);
        token
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[tokio::test]
    async fn the_host_answer_resolves_the_waiting_renewal() {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let seen_in_notify = Arc::clone(&seen);
        let broker = HostTokenBroker::new(move |id| seen_in_notify.lock().unwrap().push(id));
        let refresher = broker.refresher();

        let waiting = tokio::spawn(async move { refresher.refresh_connect_token().await });
        let request_id = loop {
            if let Some(id) = seen.lock().unwrap().first().copied() {
                break id;
            }
            tokio::task::yield_now().await;
        };
        assert!(broker.complete(request_id, Some("fresh".to_string())));
        assert_eq!(waiting.await.unwrap().as_deref(), Some("fresh"));
        // 同一请求号只能交回一次。
        assert!(!broker.complete(request_id, Some("again".to_string())));
    }

    #[tokio::test]
    async fn an_abandoned_renewal_ignores_a_late_answer() {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let seen_in_notify = Arc::clone(&seen);
        let broker = HostTokenBroker::new(move |id| seen_in_notify.lock().unwrap().push(id));
        let refresher = broker.refresher();

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(20),
            refresher.refresh_connect_token(),
        )
        .await;
        assert!(result.is_err(), "nobody answered, the wait times out");
        let request_id = seen.lock().unwrap()[0];
        assert!(!broker.complete(request_id, Some("late".to_string())));
    }

    #[tokio::test]
    async fn too_many_unanswered_renewals_fail_fast() {
        let broker = HostTokenBroker::new(|_| {});
        let mut held = Vec::new();
        for _ in 0..MAX_PENDING_RENEWALS {
            held.push(broker.begin().expect("room for a pending renewal"));
        }
        assert!(broker.begin().is_none());
        drop(held);
        assert!(broker.begin().is_some());
    }
}
