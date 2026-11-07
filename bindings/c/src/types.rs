//! C ABI 类型定义（含与 `flare_*_free` 配对的构造方法）。

use std::ffi::{CString, c_char, c_void};

/// 句柄类型
pub type FlareHandle = u64;

/// 任务句柄
pub type FlareTaskHandle = u64;

/// 订阅句柄
pub type FlareSubscriptionHandle = u64;

/// 输出字符串 (Rust 分配,调用方释放)
#[repr(C)]
pub struct FlareString {
    pub ptr: *mut c_char,
    pub len: usize,
}

/// 输入字符串视图 (调用方持有)
#[repr(C)]
pub struct FlareStringView {
    pub ptr: *const c_char,
    pub len: usize,
}

/// 输出字节 (Rust 分配,调用方释放)
#[repr(C)]
pub struct FlareBytes {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

/// 输入字节视图 (调用方持有)
#[repr(C)]
pub struct FlareBytesView {
    pub ptr: *const u8,
    pub len: usize,
}

/// 错误结构
#[repr(C)]
#[derive(Default)]
pub struct FlareError {
    pub code: i32,
    pub message: FlareString,
    pub details_json: FlareString,
}

/// 结果回调
pub type FlareResultCallback =
    extern "C" fn(context: *mut c_void, error: *const FlareError, result_json: FlareString);

/// 事件回调
pub type FlareEventCallback =
    extern "C" fn(context: *mut c_void, event_type: i32, event_json: FlareString);

/// 批量事件回调
pub type FlareEventBatchCallback =
    extern "C" fn(context: *mut c_void, event_count: usize, events_json: FlareString);

/// 进度回调
pub type FlareProgressCallback = extern "C" fn(context: *mut c_void, current: u64, total: u64);

/// 连接令牌续期请求回调：核心需要新的连接令牌时调用，运行在 SDK 后台线程上。宿主应立即返回，
/// 换到令牌后用 `flare_sdk_complete_connect_token_refresh(handle, request_id, token)` 交回
/// （换不到时 `token` 传空指针）。20 秒内未交回按「没换到」处理。
///
/// 可为空（`NULL` 表示卸下回调），所以别名本身就是 `Option`：cbindgen 把它生成为普通的函数指针
/// typedef，宿主按 `FlareTokenRefreshCallback` 传参；写成 `Option<别名>` 会生成一个不完整的结构体。
pub type FlareTokenRefreshCallback = Option<extern "C" fn(context: *mut c_void, request_id: u64)>;

impl Default for FlareString {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            len: 0,
        }
    }
}

impl FlareString {
    /// Rust 拥有的 UTF-8 字符串 → C 侧可读缓冲；须由 `flare_string_free` 释放（与 `CString::into_raw` 配对）。
    #[inline]
    pub fn from_rust_string(s: String) -> Self {
        if s.is_empty() {
            return Self::default();
        }
        match CString::new(s) {
            Ok(cstr) => {
                let len = cstr.as_bytes().len();
                let ptr = cstr.into_raw();
                Self { ptr, len }
            }
            Err(_) => Self::default(),
        }
    }
}

impl Default for FlareBytes {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        }
    }
}

impl FlareBytes {
    /// `Vec<u8>` 转移给 C ABI；须由 `flare_bytes_free` 释放。
    #[inline]
    pub fn from_vec(mut v: Vec<u8>) -> Self {
        if v.is_empty() {
            return Self::default();
        }
        let len = v.len();
        let cap = v.capacity();
        let ptr = v.as_mut_ptr();
        std::mem::forget(v);
        Self { ptr, len, cap }
    }
}

unsafe impl Send for FlareString {}
unsafe impl Sync for FlareString {}
unsafe impl Send for FlareBytes {}
unsafe impl Sync for FlareBytes {}
unsafe impl Send for FlareBytesView {}
unsafe impl Sync for FlareBytesView {}
unsafe impl Send for FlareError {}
unsafe impl Sync for FlareError {}

#[cfg(test)]
mod tests {
    use super::FlareString;

    #[test]
    fn flare_string_len_excludes_nul_terminator() {
        let s = FlareString::from_rust_string("[]".to_string());

        assert_eq!(s.len, 2);
        assert!(!s.ptr.is_null());

        crate::helpers::flare_string_free(s);
    }
}
