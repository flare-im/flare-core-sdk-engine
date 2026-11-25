//! 连接代数：每条连接一个单调递增的编号，旧连接的帧与回调一律丢弃。
//!
//! 切换线路的瞬间，旧 socket 仍可能交付帧（迟到的推送、迟到的回执）或报一次断开。没有代数过滤时，
//! 这些帧会被重复处理，迟到的断开还会把刚连上的新连接当成断了（历史上「SDK 状态机外复活连接」
//! 一类问题都是这个形状）。代数由链路状态机分配，监听器带着自己那条连接的代数，分发前比对。

use std::sync::Arc;
use tokio::sync::watch;

/// 当前连接代数（共享）。0 表示还没有建过连接。
#[derive(Debug, Clone)]
pub struct ConnectionEpoch(Arc<watch::Sender<u64>>);

impl Default for ConnectionEpoch {
    fn default() -> Self {
        Self(Arc::new(watch::channel(0).0))
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionLease {
    owner: ConnectionEpoch,
    number: u64,
}

impl PartialEq for ConnectionLease {
    fn eq(&self, other: &Self) -> bool {
        self.number == other.number && Arc::ptr_eq(&self.owner.0, &other.owner.0)
    }
}

impl ConnectionLease {
    pub(crate) async fn while_current<T>(
        &self,
        work: impl std::future::Future<Output = T>,
    ) -> Option<T> {
        let mut rx = self.owner.0.subscribe();
        tokio::select! {
            biased;
            _ = rx.wait_for(|current| *current != self.number || *current == 0) => None,
            result = work => self.owner.is_current(self.number).then_some(result),
        }
    }
}

impl ConnectionEpoch {
    pub fn new() -> Self {
        Self::default()
    }

    /// 作废当前连接、开始下一条：之后只有新代数的帧与回调算数。
    pub fn advance(&self) -> u64 {
        let mut next = 0;
        self.0.send_modify(|current| {
            next = current.wrapping_add(1).max(1);
            *current = next;
        });
        next
    }

    pub fn current(&self) -> u64 {
        *self.0.borrow()
    }

    pub(crate) fn lease(&self, number: u64) -> ConnectionLease {
        ConnectionLease {
            owner: self.clone(),
            number,
        }
    }

    /// `epoch` 是否仍是当前连接。
    pub fn is_current(&self, epoch: u64) -> bool {
        epoch != 0 && self.current() == epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn handover_revokes_a_blocked_old_frame() {
        let epoch = ConnectionEpoch::new();
        let lease = epoch.lease(epoch.advance());
        let (started, wait) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            lease
                .while_current(async {
                    started.send(()).unwrap();
                    std::future::pending::<()>().await
                })
                .await
        });
        wait.await.unwrap();
        epoch.advance();
        assert_eq!(task.await.unwrap(), None);
    }

    #[test]
    fn only_the_latest_epoch_is_current() {
        let epoch = ConnectionEpoch::new();
        assert!(!epoch.is_current(0));
        let first = epoch.advance();
        assert!(epoch.is_current(first));
        let second = epoch.advance();
        assert!(second > first);
        assert!(!epoch.is_current(first), "旧连接的帧要丢弃");
        assert!(epoch.is_current(second));
        let shared = epoch.clone();
        shared.advance();
        assert!(!epoch.is_current(second), "克隆共享同一计数");
    }
}
