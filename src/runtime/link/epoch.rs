//! 连接代数：每条连接一个单调递增的编号，旧连接的帧与回调一律丢弃。
//!
//! 切换线路的瞬间，旧 socket 仍可能交付帧（迟到的推送、迟到的回执）或报一次断开。没有代数过滤时，
//! 这些帧会被重复处理，迟到的断开还会把刚连上的新连接当成断了（历史上「SDK 状态机外复活连接」
//! 一类问题都是这个形状）。代数由链路状态机分配，监听器带着自己那条连接的代数，分发前比对。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// 当前连接代数（共享）。0 表示还没有建过连接。
#[derive(Debug, Clone, Default)]
pub struct ConnectionEpoch(Arc<AtomicU64>);

impl ConnectionEpoch {
    pub fn new() -> Self {
        Self::default()
    }

    /// 作废当前连接、开始下一条：之后只有新代数的帧与回调算数。
    pub fn advance(&self) -> u64 {
        self.0.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn current(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    /// `epoch` 是否仍是当前连接。
    pub fn is_current(&self, epoch: u64) -> bool {
        epoch != 0 && self.current() == epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
