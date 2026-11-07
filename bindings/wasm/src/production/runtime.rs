//! Production browser runtime backed by real [`IMClient`] + WebSocket transport.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use flare_im_core_sdk::client::lifecycle::LoginDbKind;
use flare_im_core_sdk::event::SharedEventReceiver;
use flare_im_core_sdk::link::flare_access::AccessStore;
use flare_im_core_sdk_bindings_runtime::{
    SessionTaskSlot, binding_response_to_value, boundary_error_js, invoke_api_id_json, sdk_error_js,
};
use js_sys::Function;
use serde::Serialize;
use serde_json::{Value, json};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use super::events::{clear_event_callback, forward_event_rx_to_js, set_event_callback};
use super::session::WasmSdkState;
use super::token_refresh::{WasmTokenRefresher, clear_refresh_callback, set_refresh_callback};
use crate::tokio_runtime;
use flare_im_core_sdk_storage_indexeddb::{
    JsGlobalStorageHost, build_web_store_provider, clear_storage_host, set_storage_host,
    storage_host_configured,
};

fn js_error(code: &str, operation: &str, error: impl ToString) -> JsValue {
    boundary_error_js(code, operation, &error.to_string())
}

/// An SDK failure as an `Error` carrying the structured error (`code`, `reason`,
/// `params`, `retryable`, `operation`).
fn map_sdk_err(operation: &str, error: flare_im_core_sdk::FlareError) -> JsValue {
    sdk_error_js(&error, Some(operation))
}

fn parse_request(request_json: &str) -> Result<Value, JsValue> {
    if request_json.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(request_json).map_err(|error| js_error("invalidParameter", "parse", error))
}

fn spawn_event_bridge(rx: SharedEventReceiver, bridge: SessionTaskSlot, runtime_id: u64) {
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
    bridge.replace(move || {
        let _ = cancel_tx.send(());
    });
    tokio_runtime::spawn_detached(forward_event_rx_to_js(runtime_id, rx, cancel_rx));
}

#[wasm_bindgen]
pub struct FlareImWasmRuntime {
    state: Arc<WasmSdkState>,
}

#[wasm_bindgen(js_name = createWasmRuntime)]
pub fn create_wasm_runtime() -> FlareImWasmRuntime {
    FlareImWasmRuntime::new()
}

impl Default for FlareImWasmRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
impl FlareImWasmRuntime {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            state: Arc::new(WasmSdkState::new()),
        }
    }

    #[wasm_bindgen(js_name = setEventCallback)]
    pub fn set_event_callback(&self, callback: Option<Function>) {
        set_event_callback(self.state.runtime_id, callback);
    }

    /// 页面托管令牌时装上续期函数：连接被网关按鉴权拒绝后核心调用 `callback()`，它返回新的连接令牌
    /// （字符串或 Promise），换不到返回 `null`；传 `undefined` 卸下。SDK 托管令牌（初始化配置里有
    /// `auth`，空对象也算）时不需要。
    #[wasm_bindgen(js_name = setConnectTokenRefresher)]
    pub fn set_connect_token_refresher(&self, callback: Option<Function>) -> js_sys::Promise {
        let runtime_id = self.state.runtime_id;
        let refresher = set_refresh_callback(runtime_id, callback).then(|| {
            Arc::new(WasmTokenRefresher::new(runtime_id))
                as Arc<dyn flare_im_core_sdk::spi::ConnectTokenRefresher>
        });
        let client = self.state.client();
        future_to_promise(async move {
            tokio_runtime::run_sdk(async move {
                client.set_connect_token_refresher(refresher).await;
                Ok(JsValue::UNDEFINED)
            })
            .await
        })
    }

    /// 装上登录前全局存储的宿主（`get` / `commit` / `scan`，见 `flare_im_core_sdk_storage_indexeddb::JsGlobalStorageHost`），在 `sdk.init`
    /// 之前调用；传 `null` / `undefined` 还原默认（内存）。缺方法时同步抛 `INVALID_PARAMETER`。
    #[wasm_bindgen(js_name = setAccessStoreHost)]
    pub fn set_access_store_host(&self, host: JsValue) -> Result<js_sys::Promise, JsValue> {
        let store: Option<Arc<dyn AccessStore>> = if host.is_null() || host.is_undefined() {
            None
        } else {
            let host = JsGlobalStorageHost::from_js(&host)
                .map_err(|error| sdk_error_js(&error, Some("setAccessStoreHost")))?;
            Some(Arc::new(host.into_access_store()))
        };
        let client = self.state.client();
        Ok(future_to_promise(async move {
            tokio_runtime::run_sdk(async move {
                client.set_access_store(store).await;
                Ok(JsValue::UNDEFINED)
            })
            .await
        }))
    }

    /// Install the JS persistence host before `sdk.login`: one object with the
    /// methods `loadSnapshot`, `saveMessage`, `saveConversation`, `saveCursor`,
    /// `savePendingSend`, `deleteMessage`, `deleteConversation` and
    /// `deletePendingSend`. Throws `INVALID_PARAMETER` when one is missing.
    #[wasm_bindgen(js_name = setStorageHost)]
    pub fn set_storage_host(&self, host: JsValue) -> Result<(), JsValue> {
        let _ = self;
        set_storage_host(&host).map_err(|error| sdk_error_js(&error, Some("setStorageHost")))
    }

    #[wasm_bindgen(js_name = clearStorageHost)]
    pub fn clear_storage_host(&self) {
        let _ = self;
        clear_storage_host();
    }

    #[wasm_bindgen(js_name = storageHostConfigured)]
    pub fn storage_host_configured(&self) -> bool {
        let _ = self;
        storage_host_configured()
    }

    /// Sync export returning a JS Promise — avoids nested `block_on` inside wasm-bindgen `async fn`.
    #[wasm_bindgen]
    pub fn invoke(&self, operation: &str, request_json: &str) -> js_sys::Promise {
        let operation = operation.to_string();
        let request_json = request_json.to_string();
        let state = self.state.clone();
        let mut invocation = state.invocations.begin(&operation);
        future_to_promise(async move {
            tokio_runtime::run_sdk(async move {
                if state.disposed.load(Ordering::Acquire) {
                    return Err(js_error("wasm.disposed", &operation, "runtime disposed"));
                }
                tokio::select! {
                    biased;
                    _ = invocation.cancelled() => Err(js_error(
                        "wasm.operation_cancelled", &operation,
                        "Local operation cancelled; remote writes may already have committed",
                    )),
                    result = invoke_impl(state, &operation, &request_json) => result,
                }
            })
            .await
        })
    }

    #[wasm_bindgen(js_name = cancelPendingInvocations)]
    pub fn cancel_pending_invocations(&self) -> bool {
        self.state.invocations.cancel_pending()
    }

    pub fn dispose(&self) -> js_sys::Promise {
        self.state.disposed.store(true, Ordering::Release);
        clear_event_callback(self.state.runtime_id);
        clear_refresh_callback(self.state.runtime_id);
        self.state.clear_event_bridge();
        self.state.invocations.cancel_pending();
        let state = self.state.clone();
        future_to_promise(async move {
            tokio_runtime::run_sdk(async move {
                state
                    .logout()
                    .await
                    .map_err(|e| map_sdk_err("sdk.dispose", e))?;
                Ok(JsValue::UNDEFINED)
            })
            .await
        })
    }
}

async fn invoke_impl(
    state: Arc<WasmSdkState>,
    operation: &str,
    request_json: &str,
) -> Result<JsValue, JsValue> {
    let result = match operation {
        "sdk.create" => Ok(json!({ "handle": 1 })),
        "sdk.init" => {
            let request = parse_request(request_json)?;
            let environment = request
                .get("environment")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let sdk_config = if request.get("sdkConfig").is_some() {
                Some(
                    serde_json::from_value(
                        request.get("sdkConfig").cloned().unwrap_or(Value::Null),
                    )
                    .map_err(|e| js_error("invalidParameter", operation, e))?,
                )
            } else {
                serde_json::from_value(request.clone()).ok()
            };
            state
                .set_config(environment, sdk_config)
                .await
                .map_err(|e| map_sdk_err(operation, e))?;
            Ok(Value::Null)
        }
        "sdk.login" => {
            let request = parse_request(request_json)?;
            let user_id = request
                .get("userId")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| js_error("invalidParameter", operation, "userId is required"))?
                .to_string();
            let token = request
                .get("token")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_default();
            let token = if token.trim().is_empty() {
                None
            } else {
                Some(token.as_str())
            };
            let client = state.client();
            let session_state = state.clone();
            let event_bridge = session_state.event_bridge();
            event_bridge.clear();
            let event_bridge_for_login = event_bridge.clone();
            let runtime_id = state.runtime_id;
            let store_provider = build_web_store_provider(&user_id).await;
            let login_result = client
                .login(
                    &user_id,
                    token,
                    LoginDbKind::IndexedDb(store_provider),
                    move |bus, _| {
                        let rx = bus.subscribe_shared_raw();
                        spawn_event_bridge(rx, event_bridge_for_login.clone(), runtime_id);
                    },
                )
                .await;
            let apis = match login_result {
                Ok(apis) => apis,
                Err(err) => {
                    event_bridge.clear();
                    return Err(map_sdk_err(operation, err));
                }
            };
            session_state.install_session(apis).await;
            Ok(Value::Null)
        }
        "sdk.prepare" => {
            let request = parse_request(request_json)?;
            let user_id = request
                .get("userId")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| js_error("invalidParameter", operation, "userId is required"))?
                .to_string();
            let client = state.client();
            let event_bridge = state.event_bridge();
            event_bridge.clear();
            let store_provider = build_web_store_provider(&user_id).await;
            client
                .prepare(&user_id, LoginDbKind::IndexedDb(store_provider))
                .await
                .map_err(|e| map_sdk_err(operation, e))?;
            // 预热后立即订阅事件总线 → 转发 JS（等价 login 闭包在 connect 前所做）。
            let bus = client.bus().await.map_err(|e| map_sdk_err(operation, e))?;
            let rx = bus.subscribe_shared_raw();
            spawn_event_bridge(rx, event_bridge, state.runtime_id);
            Ok(Value::Null)
        }
        "sdk.connect" => {
            let request = parse_request(request_json)?;
            let user_id = request
                .get("userId")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| js_error("invalidParameter", operation, "userId is required"))?
                .to_string();
            let token = request
                .get("token")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_default();
            let token = if token.trim().is_empty() {
                None
            } else {
                Some(token.as_str())
            };
            let client = state.client();
            let session_state = state.clone();
            let apis = client
                .connect(&user_id, token)
                .await
                .map_err(|e| map_sdk_err(operation, e))?;
            session_state.install_session(apis).await;
            Ok(Value::Null)
        }
        "sdk.logout" => {
            state
                .logout()
                .await
                .map_err(|e| map_sdk_err(operation, e))?;
            Ok(Value::Null)
        }
        "sdk.uninit" => {
            state.clear_event_bridge();
            state.clear_session().await;
            state
                .client()
                .uninit()
                .await
                .map_err(|e| map_sdk_err(operation, e))?;
            Ok(Value::Null)
        }
        "sdk.dispose" | "sdk.hard_reset" => {
            clear_event_callback(state.runtime_id);
            state.clear_event_bridge();
            state.clear_session().await;
            let _ = state.client().logout().await;
            Ok(Value::Null)
        }
        "event.subscribe" => Ok(json!({ "id": 1 })),
        "event.unsubscribe" | "event.unsubscribe_all" => Ok(Value::Null),
        operation => invoke_api_id_json(&*state, operation, request_json)
            .await
            .map(binding_response_to_value)
            .map_err(|e| map_sdk_err(operation, e)),
    }?;

    result
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|error| js_error("wasm.serialize_failed", operation, error))
}
