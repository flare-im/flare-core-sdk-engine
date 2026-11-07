//! 宿主托管令牌的续期回调（Web）。
//!
//! 令牌由页面（业务网关）签发、刷新时，连接被网关按鉴权拒绝后核心向页面要一枚新的：调用页面装上的
//! 函数，它返回新令牌（字符串或 Promise），换不到返回 `null`。核心最多等 20 秒。
//! JS 函数不能跨线程，按运行时 ID 存在线程局部表里，核心持有的只是运行时 ID。

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use flare_im_core_sdk::spi::ConnectTokenRefresher;
use js_sys::{Function, Promise};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;

thread_local! {
    static REFRESH_CALLBACKS: RefCell<HashMap<u64, Rc<Function>>> = RefCell::new(HashMap::new());
}

/// 装上或卸下运行时 `runtime_id` 的续期函数；返回是否装上。
pub fn set_refresh_callback(runtime_id: u64, callback: Option<Function>) -> bool {
    REFRESH_CALLBACKS.with(|slot| {
        let mut callbacks = slot.borrow_mut();
        match callback {
            Some(callback) => {
                callbacks.insert(runtime_id, Rc::new(callback));
                true
            }
            None => {
                callbacks.remove(&runtime_id);
                false
            }
        }
    })
}

pub fn clear_refresh_callback(runtime_id: u64) {
    set_refresh_callback(runtime_id, None);
}

/// 核心持有的续期回调：只记运行时 ID，调用时再取 JS 函数。
pub struct WasmTokenRefresher {
    runtime_id: u64,
}

impl WasmTokenRefresher {
    pub fn new(runtime_id: u64) -> Self {
        Self { runtime_id }
    }
}

#[async_trait::async_trait(?Send)]
impl ConnectTokenRefresher for WasmTokenRefresher {
    async fn refresh_connect_token(&self) -> Option<String> {
        // 先放开 RefCell 借用再调 JS：回调里可能卸下自己。
        let callback =
            REFRESH_CALLBACKS.with(|slot| slot.borrow().get(&self.runtime_id).cloned())?;
        let returned = match callback.call0(&JsValue::NULL) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(?error, "the page's connect token refresher threw");
                return None;
            }
        };
        let value = if returned.is_instance_of::<Promise>() {
            match JsFuture::from(returned.unchecked_into::<Promise>()).await {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(?error, "the page's connect token refresher rejected");
                    return None;
                }
            }
        } else {
            returned
        };
        value.as_string().filter(|token| !token.trim().is_empty())
    }
}
