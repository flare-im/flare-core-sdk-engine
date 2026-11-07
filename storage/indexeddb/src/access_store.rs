//! 登录前全局存储的页面宿主（接入清单 LKG、密钥状态、健康簿）。
//!
//! 页面装上一个对象（平台包用 IndexedDB 实现），提供三个方法，都可以返回 Promise：
//!
//! - `get(key)` → `Uint8Array`（或 `ArrayBuffer`），没有时 `null` / `undefined`；
//! - `commit(ops)`：`ops` 是 `[{ op: "put", key, value: Uint8Array } | { op: "delete", key }]`，
//!   必须在一个事务里全部生效或全部不生效（清单与它的 serial 一起落盘）；
//! - `scan(prefix)` → `[[key, Uint8Array], ...]`，按前缀列出。
//!
//! 核心经 flare-access 的 [`HostAccessStore`] 使用它，键都带 `flare-access/` 前缀，宿主原样存取即可。
//! 宿主方法抛错或拒绝时按存储后端失败处理：健康簿与 LKG 只是加速与兜底，本次进程照常可用。

use flare_im_core_sdk::link::flare_access::store::{
    GlobalStorageHost, HostAccessStore, StoreError, StoreOp,
};
use flare_im_core_sdk::{ErrorCode, FlareError};
use js_sys::{Array, ArrayBuffer, Function, Object, Promise, Reflect, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;

/// 页面提供的 `get` / `commit` / `scan`，装上时按名字取出并绑定到宿主对象。
pub struct JsGlobalStorageHost {
    get: Function,
    commit: Function,
    scan: Function,
}

impl JsGlobalStorageHost {
    pub fn from_js(host: &JsValue) -> Result<Self, FlareError> {
        Ok(Self {
            get: host_method(host, "get")?,
            commit: host_method(host, "commit")?,
            scan: host_method(host, "scan")?,
        })
    }

    /// 包成带命名空间的 [`HostAccessStore`]，交给 `IMClient::set_access_store`。
    pub fn into_access_store(self) -> HostAccessStore<Self> {
        HostAccessStore::new(self)
    }
}

fn host_method(host: &JsValue, name: &str) -> Result<Function, FlareError> {
    Reflect::get(host, &JsValue::from_str(name))
        .ok()
        .and_then(|value| value.dyn_into::<Function>().ok())
        .map(|method| method.bind0(host))
        .ok_or_else(|| {
            FlareError::localized(
                ErrorCode::InvalidParameter,
                format!("access store host has no {name} method"),
            )
        })
}

fn backend(what: &str, error: JsValue) -> StoreError {
    StoreError::Backend(format!("access store host {what} failed: {error:?}"))
}

/// 调用宿主方法并等它完成（返回值可以是 Promise，也可以是普通值）。
async fn call(function: &Function, what: &str, arg: &JsValue) -> Result<JsValue, StoreError> {
    let returned = function
        .call1(&JsValue::NULL, arg)
        .map_err(|error| backend(what, error))?;
    if returned.is_instance_of::<Promise>() {
        JsFuture::from(returned.unchecked_into::<Promise>())
            .await
            .map_err(|error| backend(what, error))
    } else {
        Ok(returned)
    }
}

fn bytes_from_js(value: &JsValue, what: &str) -> Result<Vec<u8>, StoreError> {
    if let Some(bytes) = value.dyn_ref::<Uint8Array>() {
        return Ok(bytes.to_vec());
    }
    if let Some(buffer) = value.dyn_ref::<ArrayBuffer>() {
        return Ok(Uint8Array::new(buffer).to_vec());
    }
    Err(StoreError::Corrupt(format!(
        "access store host {what} returned a value that is not bytes"
    )))
}

fn op_to_js(op: &StoreOp) -> Result<JsValue, StoreError> {
    let entry = Object::new();
    let set = |name: &str, value: &JsValue| {
        Reflect::set(&entry, &JsValue::from_str(name), value)
            .map(|_| ())
            .map_err(|error| backend("commit", error))
    };
    match op {
        StoreOp::Put { key, value } => {
            set("op", &JsValue::from_str("put"))?;
            set("key", &JsValue::from_str(key))?;
            set("value", &Uint8Array::from(value.as_slice()).into())?;
        }
        StoreOp::Delete { key } => {
            set("op", &JsValue::from_str("delete"))?;
            set("key", &JsValue::from_str(key))?;
        }
    }
    Ok(entry.into())
}

#[async_trait::async_trait(?Send)]
impl GlobalStorageHost for JsGlobalStorageHost {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let value = call(&self.get, "get", &JsValue::from_str(key)).await?;
        if value.is_null() || value.is_undefined() {
            return Ok(None);
        }
        bytes_from_js(&value, "get").map(Some)
    }

    async fn commit(&self, ops: &[StoreOp]) -> Result<(), StoreError> {
        if ops.is_empty() {
            return Ok(());
        }
        let batch = Array::new();
        for op in ops {
            batch.push(&op_to_js(op)?);
        }
        call(&self.commit, "commit", &batch.into())
            .await
            .map(|_| ())
    }

    async fn scan(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        let value = call(&self.scan, "scan", &JsValue::from_str(prefix)).await?;
        if value.is_null() || value.is_undefined() {
            return Ok(Vec::new());
        }
        let entries = value.dyn_into::<Array>().map_err(|_| {
            StoreError::Corrupt("access store host scan did not return an array".to_owned())
        })?;
        let mut out = Vec::with_capacity(entries.length() as usize);
        for entry in entries.iter() {
            let pair = entry.dyn_into::<Array>().map_err(|_| {
                StoreError::Corrupt("access store host scan entry is not [key, value]".to_owned())
            })?;
            let key = pair.get(0).as_string().ok_or_else(|| {
                StoreError::Corrupt("access store host scan key is not a string".to_owned())
            })?;
            out.push((key, bytes_from_js(&pair.get(1), "scan")?));
        }
        Ok(out)
    }
}
