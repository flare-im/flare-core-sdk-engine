//! 辅助工具 - 字符串、字节、JSON 处理（安全侧）；裸指针读取委托 [`crate::abi`]。

use serde::de::DeserializeOwned;
use std::ffi::{CString, c_char};

use flare_im_core_sdk::model::UploadOptions;
use flare_im_core_sdk::prelude::SdkConfigOverlay;
use serde_json::Value;

use crate::abi;
use crate::error_convert::{FLARE_ERR_INVALID_PARAM, FLARE_ERR_JSON_PARSE};
use crate::types::{FlareBytes, FlareError, FlareString};

// ===== 字符串辅助 =====

/// 从 C 侧 NUL 终止 UTF-8 字符串读取（有界扫描，见 [`abi::MAX_CSTRING_BYTES`]）。
#[inline]
pub fn c_str_to_string(ptr: *const c_char) -> Result<String, i32> {
    abi::read_c_str(ptr)
}

/// 从 Rust `String` 创建 [`FlareString`]（见 [`FlareString::from_rust_string`]）。
#[inline]
pub fn string_to_flare(s: String) -> FlareString {
    FlareString::from_rust_string(s)
}

// ===== JSON 辅助 =====

/// 解析 C 侧 UTF-8 JSON 字符串
#[inline]
pub fn parse_json<T: DeserializeOwned>(ptr: *const c_char) -> Result<T, i32> {
    abi::parse_json(ptr)
}

/// 序列化为 JSON String
#[inline]
pub fn to_json_string<T: serde::Serialize>(value: &T) -> Result<String, i32> {
    serde_json::to_string(value).map_err(|_| FLARE_ERR_JSON_PARSE)
}

/// 解析 init 配置：支持裸 [`SdkConfigOverlay`] 或 `{ environment?, sdkConfig? }`。
#[inline]
pub fn parse_init_request(
    ptr: *const c_char,
) -> Result<(Option<String>, Option<SdkConfigOverlay>), i32> {
    let value: Value = parse_json(ptr)?;
    flare_im_core_sdk_bindings_runtime::dispatch_support::init_request(&value)
        .map_err(|_| FLARE_ERR_JSON_PARSE)
}

/// 解析可选上传分片配置（`options_json` 为 null/空时返回 `None`）。
#[inline]
pub fn parse_upload_options(options_json: *const c_char) -> Result<Option<UploadOptions>, i32> {
    let Some(raw) = abi::read_c_str_opt(options_json)? else {
        return Ok(None);
    };
    if raw.trim().is_empty() || raw.trim() == "null" {
        return Ok(None);
    }
    let value: serde_json::Value =
        serde_json::from_str(&raw).map_err(|_| FLARE_ERR_INVALID_PARAM)?;
    if !value.is_object() {
        return Err(FLARE_ERR_INVALID_PARAM);
    }
    let chunk_size = match value.get("chunk_size").or_else(|| value.get("chunkSize")) {
        None | Some(serde_json::Value::Null) => UploadOptions::default().chunk_size,
        Some(value) => usize::try_from(value.as_u64().ok_or(FLARE_ERR_INVALID_PARAM)?)
            .map_err(|_| FLARE_ERR_INVALID_PARAM)?,
    };
    let purpose = match value.get("purpose") {
        None | Some(serde_json::Value::Null) => Default::default(),
        Some(serde_json::Value::String(raw)) => raw.parse().map_err(|_| FLARE_ERR_INVALID_PARAM)?,
        _ => return Err(FLARE_ERR_INVALID_PARAM),
    };
    if chunk_size == 0 {
        return Err(FLARE_ERR_INVALID_PARAM);
    }
    let transfer_id = match value.get("transferId") {
        None | Some(serde_json::Value::Null) => None,
        Some(value) => Some(value.as_str().ok_or(FLARE_ERR_INVALID_PARAM)?.to_owned()),
    };
    Ok(Some(UploadOptions {
        transfer_id,
        chunk_size,
        purpose,
    }))
}

// ===== 内存释放函数（C 调用；内部 panic 隔离） =====

#[unsafe(no_mangle)]
pub extern "C" fn flare_string_free(s: FlareString) {
    abi::catch_ffi_void(|| {
        if s.ptr.is_null() || s.len == 0 {
            return;
        }
        // SAFETY: `ptr` 须来自 [`FlareString::from_rust_string`] / `CString::into_raw`；仅释放一次。
        unsafe {
            let _ = CString::from_raw(s.ptr);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn flare_bytes_free(b: FlareBytes) {
    abi::catch_ffi_void(|| {
        if b.ptr.is_null() || b.len == 0 {
            return;
        }
        // SAFETY: `ptr,len,cap` 须来自 [`FlareBytes::from_vec`]；仅释放一次。
        unsafe {
            let _ = Vec::from_raw_parts(b.ptr, b.len, b.cap);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn flare_error_free(e: FlareError) {
    abi::catch_ffi_void(|| {
        flare_string_free(e.message);
        flare_string_free(e.details_json);
    });
}

/// 释放由 `Box::into_raw` 传入 Dart 回调的 [`FlareError`]（须与 [`crate::executor`] 堆分配错误成对使用）。
///
/// # Safety
/// `ptr` 须为 null，或由本 crate 经 `Box::into_raw` 分配且仅释放一次的合法指针。
#[unsafe(no_mangle)]
pub unsafe extern "C" fn flare_error_heap_free(ptr: *mut FlareError) {
    if ptr.is_null() {
        return;
    }
    abi::catch_ffi_void(|| {
        // SAFETY: 与上方约定一致；`ptr` 非 null 且由 `Box::into_raw` 产出。
        unsafe {
            let FlareError {
                message,
                details_json,
                ..
            } = *Box::from_raw(ptr);
            flare_string_free(message);
            flare_string_free(details_json);
        }
    });
}
