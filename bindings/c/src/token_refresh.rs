//! 宿主托管令牌的续期回调（C ABI）。
//!
//! 令牌由宿主签发、刷新时（例如业务网关连同 IM 连接令牌一起下发），核心在连接被网关按鉴权拒绝后
//! 只能向宿主要新令牌。C 宿主交不出 Rust 异步函数，所以走「通知请求号 → 宿主交回」：
//! [`flare_sdk_set_connect_token_refresher`] 装上通知回调，宿主换到令牌后调用
//! [`flare_sdk_complete_connect_token_refresh`]。配对表在核心的 `HostTokenBroker` 里。

use std::ffi::{c_char, c_void};
use std::sync::atomic::Ordering;

use dashmap::DashMap;
use flare_im_core_sdk::client::HostTokenBroker;

use crate::abi;
use crate::error_convert::{FLARE_ERR_INVALID_HANDLE, FLARE_ERR_INVALID_PARAM, FLARE_OK};
use crate::executor::{CallbackContext, execute_async_unit};
use crate::registry::require_instance;
use crate::types::{FlareHandle, FlareResultCallback, FlareTokenRefreshCallback};

lazy_static::lazy_static! {
    /// 句柄 → 续期桥。交回时按句柄找到桥；句柄释放时一并摘掉。
    static ref TOKEN_BROKERS: DashMap<FlareHandle, HostTokenBroker> = DashMap::new();
}

pub(crate) fn forget_handle(handle: FlareHandle) {
    TOKEN_BROKERS.remove(&handle);
}

pub(crate) fn forget_all() {
    TOKEN_BROKERS.clear();
}

/// 装上（`refresh_callback` 非空）或卸下（为空）连接令牌续期回调；结果经 `callback` 异步返回。
///
/// SDK 托管令牌（初始化配置里有 `auth`，空对象也算）时不需要它，核心自己经 Core API 签发和换新。
#[unsafe(no_mangle)]
pub extern "C" fn flare_sdk_set_connect_token_refresher(
    handle: FlareHandle,
    refresh_context: *mut c_void,
    refresh_callback: FlareTokenRefreshCallback,
    context: *mut c_void,
    callback: FlareResultCallback,
) -> i32 {
    abi::catch_ffi_i32(|| {
        let instance = match require_instance(handle) {
            Ok(instance) => instance,
            Err(code) => return code,
        };
        let refresher = match refresh_callback {
            Some(notify) => {
                // 指针存成 usize 才能跨线程；宿主保证 refresh_context 在卸下回调前一直有效。
                let user_context = refresh_context as usize;
                let enabled = instance.callbacks_enabled.clone();
                let broker = HostTokenBroker::new(move |request_id| {
                    if !enabled.load(Ordering::Acquire) {
                        return;
                    }
                    abi::invoke_user_c_callback("FlareTokenRefreshCallback", || {
                        notify(user_context as *mut c_void, request_id);
                    });
                });
                TOKEN_BROKERS.insert(handle, broker.clone());
                Some(broker.refresher())
            }
            None => {
                TOKEN_BROKERS.remove(&handle);
                None
            }
        };
        let ctx = CallbackContext::new(context, callback);
        let client = instance.client.clone();
        execute_async_unit(instance, ctx, async move {
            client.set_connect_token_refresher(refresher).await;
            Ok(())
        });
        FLARE_OK
    })
}

/// 交回续期请求 `request_id` 的结果：`token` 为新的连接令牌，换不到时传空指针。
///
/// 返回 `FLARE_ERR_INVALID_PARAM` 表示请求号未知或已作废（核心已不再等待），宿主无需重试。
#[unsafe(no_mangle)]
pub extern "C" fn flare_sdk_complete_connect_token_refresh(
    handle: FlareHandle,
    request_id: u64,
    token: *const c_char,
) -> i32 {
    abi::catch_ffi_i32(|| {
        let token = match abi::read_c_str_opt(token) {
            Ok(token) => token.filter(|token| !token.trim().is_empty()),
            Err(code) => return code,
        };
        let Some(broker) = TOKEN_BROKERS
            .get(&handle)
            .map(|entry| entry.value().clone())
        else {
            return FLARE_ERR_INVALID_HANDLE;
        };
        if broker.complete(request_id, token) {
            FLARE_OK
        } else {
            FLARE_ERR_INVALID_PARAM
        }
    })
}
