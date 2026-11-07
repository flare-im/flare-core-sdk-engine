//! 平台差异的最小抽象：线程约束与随机源。

use std::collections::VecDeque;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

/// 原生端的端口（存储、探活、拉取）会被 tokio 多线程运行时跨线程调用，必须 `Send + Sync`；
/// 浏览器 wasm 是单线程，宿主对象（JS 回调）天然不是 `Send`，强加约束会让宿主无法实现。
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSendSync: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: ?Sized + Send + Sync> MaybeSendSync for T {}

#[cfg(target_arch = "wasm32")]
pub trait MaybeSendSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSendSync for T {}

/// 装箱的 future：原生要能跨线程，浏览器 wasm 单线程且宿主句柄不是 `Send`。
#[cfg(not(target_arch = "wasm32"))]
pub type BoxedFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
#[cfg(target_arch = "wasm32")]
pub type BoxedFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + 'a>>;

/// 随机源：分层随机选路与熔断冷却抖动都从这里取数。
///
/// 抽成 trait 是为了让黄金向量可以把每一次抽样的值写死，六端回放得到完全相同的选择。
pub trait RandomSource: Send {
    /// 返回 `[0, 1)` 区间的均匀随机数。
    fn next_unit(&mut self) -> f64;
}

/// 默认随机源：系统熵播种的小型 PRNG。选路与抖动不需要密码学强度，只需要各客户端彼此独立。
pub struct DefaultRandom(SmallRng);

impl DefaultRandom {
    pub fn new() -> Self {
        Self(SmallRng::from_entropy())
    }

    /// 固定种子，便于引擎侧的确定性测试。
    pub fn seeded(seed: u64) -> Self {
        Self(SmallRng::seed_from_u64(seed))
    }
}

impl Default for DefaultRandom {
    fn default() -> Self {
        Self::new()
    }
}

impl RandomSource for DefaultRandom {
    fn next_unit(&mut self) -> f64 {
        self.0.r#gen::<f64>()
    }
}

/// 按给定序列吐数的随机源（序列耗尽后返回 0.5）。用于测试与向量回放。
#[derive(Debug, Default, Clone)]
pub struct SequenceRandom(VecDeque<f64>);

impl SequenceRandom {
    pub fn new(values: impl IntoIterator<Item = f64>) -> Self {
        Self(values.into_iter().collect())
    }

    pub fn push(&mut self, value: f64) {
        self.0.push_back(value);
    }
}

impl RandomSource for SequenceRandom {
    fn next_unit(&mut self) -> f64 {
        clamp_unit(self.0.pop_front().unwrap_or(0.5))
    }
}

/// 把任意输入夹到 `[0, 1)`，防止外部注入的随机源越界导致下标溢出。
pub(crate) fn clamp_unit(value: f64) -> f64 {
    if value.is_nan() || value < 0.0 {
        0.0
    } else if value >= 1.0 {
        // 最接近 1 的可表示值，保持右开区间。
        1.0 - f64::EPSILON
    } else {
        value
    }
}
