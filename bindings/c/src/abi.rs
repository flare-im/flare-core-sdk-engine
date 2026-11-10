//! FFI 边界治理：有界 C 字符串读取、panic 隔离。
//!
//! 原则：不在此模块调用 SDK 业务 API；仅提供跨 C ABI 的机械转换与安全护栏。

use std::ffi::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};

use serde::de::DeserializeOwned;

use crate::error_convert::{
    FLARE_ERR_FFI_PANIC, FLARE_ERR_INVALID_PARAM, FLARE_ERR_JSON_PARSE, FLARE_ERR_NULL_POINTER,
};
use crate::types::{FlareHandle, FlareString, FlareSubscriptionHandle};

/// C 字符串最大扫描长度（含末尾 NUL 之前），防止恶意/损坏指针拖垮扫描。
pub const MAX_CSTRING_BYTES: usize = 4 * 1024 * 1024;

/// 读取以 NUL 结尾的 UTF-8 字符串（有界）。
///
/// # 指针与生命周期
/// - 调用方保证 `ptr` 在函数返回前指向可读内存。
/// - 并发写同一缓冲在 C 侧为 UB；本侧按只读快照解释。
#[inline]
pub fn read_c_str(ptr: *const c_char) -> Result<String, i32> {
    read_c_str_with_limit(ptr, MAX_CSTRING_BYTES)
}

/// A caller-selected bounded scan, reserved for a known operation's larger
/// request contract. Other C strings retain the default four MiB bound.
#[inline]
pub fn read_c_str_with_limit(ptr: *const c_char, scan_limit: usize) -> Result<String, i32> {
    if ptr.is_null() {
        return Err(FLARE_ERR_NULL_POINTER);
    }
    let len = scan_nul_terminated_len(ptr, scan_limit)?;
    // SAFETY: `ptr` 已判非空；`len` 为不超过 MAX 的前缀长度且下一字节为 NUL 或 len 在边界内合法子串。
    // 调用方保证 [ptr, ptr+len) 在调用期间有效且可读；无所有权转移。
    let slice = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) };
    std::str::from_utf8(slice)
        .map(|s| s.to_string())
        .map_err(|_| FLARE_ERR_INVALID_PARAM)
}

/// `NULL` 视为「无字符串」；非空则同 [`read_c_str`]。
#[inline]
pub fn read_c_str_opt(ptr: *const c_char) -> Result<Option<String>, i32> {
    if ptr.is_null() {
        return Ok(None);
    }
    read_c_str(ptr).map(Some)
}

#[inline]
fn scan_nul_terminated_len(ptr: *const c_char, scan_limit: usize) -> Result<usize, i32> {
    // SAFETY: `ptr` 非空；每次仅做单字节只读，直到 NUL 或达到上限。
    unsafe {
        let mut n = 0usize;
        while n < scan_limit {
            let b = *ptr.add(n) as u8;
            if b == 0 {
                return Ok(n);
            }
            n += 1;
        }
        Err(FLARE_ERR_INVALID_PARAM)
    }
}

/// 从 C 侧 UTF-8 JSON 字符串反序列化。
#[inline]
pub fn parse_json<T: DeserializeOwned>(ptr: *const c_char) -> Result<T, i32> {
    let s = read_c_str(ptr)?;
    serde_json::from_str(&s).map_err(|_| FLARE_ERR_JSON_PARSE)
}

// --- Panic 隔离：禁止 Rust panic 穿过 `extern "C"`（否则与 C 互操作未定义行为） ---

/// 在用户提供的 C 回调外包裹 `catch_unwind`，用于异步路径与事件转发（与 `catch_ffi_*` 入口层互补）。
#[inline]
pub(crate) fn invoke_user_c_callback(name: &'static str, f: impl FnOnce()) {
    if catch_unwind(AssertUnwindSafe(f)).is_err() {
        tracing::error!(
            target: "flare_im_ffi",
            "{} panicked (buffer ownership may be ambiguous for caller)",
            name
        );
    }
}

#[inline]
pub fn catch_ffi_i32(f: impl FnOnce() -> i32) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            tracing::error!(target: "flare_im_ffi", "FFI entry panicked (i32 return)");
            FLARE_ERR_FFI_PANIC
        }
    }
}

#[inline]
pub fn catch_ffi_void(f: impl FnOnce()) {
    if catch_unwind(AssertUnwindSafe(f)).is_err() {
        tracing::error!(target: "flare_im_ffi", "FFI entry panicked (void return)");
    }
}

#[inline]
pub fn catch_ffi_bool(f: impl FnOnce() -> bool) -> bool {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            tracing::error!(target: "flare_im_ffi", "FFI entry panicked (bool return)");
            false
        }
    }
}

#[inline]
pub fn catch_ffi_handle(f: impl FnOnce() -> FlareHandle) -> FlareHandle {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            tracing::error!(target: "flare_im_ffi", "FFI entry panicked (handle return)");
            0
        }
    }
}

#[inline]
pub fn catch_ffi_subscription_handle(
    f: impl FnOnce() -> FlareSubscriptionHandle,
) -> FlareSubscriptionHandle {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            tracing::error!(target: "flare_im_ffi", "FFI entry panicked (subscription return)");
            0
        }
    }
}

#[inline]
pub fn catch_ffi_flare_string(f: impl FnOnce() -> FlareString) -> FlareString {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            tracing::error!(target: "flare_im_ffi", "FFI entry panicked (FlareString return)");
            FlareString::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_specific_string_bound_does_not_relax_the_default_scan() {
        let mut bytes = vec![b'1'; MAX_CSTRING_BYTES + 16];
        bytes.push(0);
        let pointer = bytes.as_ptr().cast();
        assert_eq!(read_c_str(pointer), Err(FLARE_ERR_INVALID_PARAM));
        assert_eq!(
            read_c_str_with_limit(pointer, bytes.len()).unwrap().len(),
            bytes.len() - 1
        );
        assert_eq!(
            read_c_str_with_limit(pointer, bytes.len() - 1),
            Err(FLARE_ERR_INVALID_PARAM)
        );
        assert_eq!(
            read_c_str_with_limit(std::ptr::null(), bytes.len()),
            Err(FLARE_ERR_NULL_POINTER)
        );
    }

    #[test]
    fn catch_ffi_i32_converts_panic_to_stable_error_code() {
        let code = catch_ffi_i32(|| panic!("injected FFI panic"));

        assert_eq!(code, FLARE_ERR_FFI_PANIC);
    }

    #[test]
    fn catch_ffi_return_guards_convert_panic_to_safe_defaults() {
        assert!(!catch_ffi_bool(|| panic!("injected bool FFI panic")));
        assert_eq!(catch_ffi_handle(|| panic!("injected handle FFI panic")), 0);
        assert_eq!(
            catch_ffi_subscription_handle(|| panic!("injected subscription FFI panic")),
            0
        );

        let value = catch_ffi_flare_string(|| panic!("injected string FFI panic"));
        assert!(value.ptr.is_null());
        assert_eq!(value.len, 0);
    }
}
