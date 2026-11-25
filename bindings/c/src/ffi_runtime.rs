//! Flutter / 纯原生主线程等调用 FFI 时，线程上通常 **没有** Tokio runtime；
//! `Handle::try_current()` 会失败。此处提供进程级共享的 multi-thread runtime 供 `spawn` 使用。

use tokio::runtime::{Handle, Runtime};

const MIN_FFI_WORKER_THREADS: usize = 2;

lazy_static::lazy_static! {
    static ref FLARE_FFI_TOKIO: std::result::Result<Runtime, String> =
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(ffi_worker_threads())
            .enable_all()
            .thread_name("flare-ffi")
            // Deeply nested dispatch futures overflow tokio's 2 MiB default in debug builds.
            .thread_stack_size(8 * 1024 * 1024)
            .build()
            .map_err(|error| format!("flare_im_core_sdk_ffi: failed to create Tokio runtime: {error}"));
}

fn recommended_worker_threads(available_parallelism: usize) -> usize {
    (available_parallelism / 2).max(MIN_FFI_WORKER_THREADS)
}

fn ffi_worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| recommended_worker_threads(parallelism.get()))
        .unwrap_or(MIN_FFI_WORKER_THREADS)
}

fn fallback_runtime() -> std::result::Result<&'static Runtime, String> {
    match &*FLARE_FFI_TOKIO {
        Ok(runtime) => Ok(runtime),
        Err(error) => Err(error.clone()),
    }
}

/// 供 `SdkInstance` 使用的 runtime handle：优先复用当前线程已存在的 runtime（如 `#[tokio::test]`），否则使用共享 runtime。
#[inline]
pub fn sdk_runtime_handle() -> std::result::Result<Handle, String> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return Ok(handle);
    }

    fallback_runtime().map(|runtime| runtime.handle().clone())
}

#[cfg(test)]
mod tests {
    use super::{MIN_FFI_WORKER_THREADS, recommended_worker_threads};

    #[test]
    fn recommended_worker_threads_scale_with_host_parallelism() {
        assert_eq!(recommended_worker_threads(1), MIN_FFI_WORKER_THREADS);
        assert_eq!(recommended_worker_threads(2), MIN_FFI_WORKER_THREADS);
        assert_eq!(recommended_worker_threads(4), MIN_FFI_WORKER_THREADS);
        assert_eq!(recommended_worker_threads(8), 4);
        assert_eq!(recommended_worker_threads(16), 8);
    }
}
