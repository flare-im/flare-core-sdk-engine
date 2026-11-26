//! The errors browser runtimes throw (core and business-SDK wasm bindings): real `Error`
//! objects whose properties carry the structured error, so hosts never parse the
//! message.

use flare_im_core_sdk::FlareError;
use flare_im_core_sdk::serde_json::{Value, json};
use serde::Serialize;
use wasm_bindgen::JsValue;

use crate::error::sdk_error_value;

/// A binding boundary failure (bad request, disposed runtime): `code` is the
/// boundary code, `operation` the binding operation.
pub fn boundary_error_js(code: &str, operation: &str, message: &str) -> JsValue {
    error_with(message, &json!({"code": code, "operation": operation}))
}

/// An SDK failure: the structured error (`code`, `reason`, `retryable` and the
/// typed facts) and, when known, `operation` ([`sdk_error_value`]).
pub fn sdk_error_js(error: &FlareError, operation: Option<&str>) -> JsValue {
    error_with(&error.to_string(), &sdk_error_value(error, operation))
}

fn error_with(message: &str, properties: &Value) -> JsValue {
    let error = js_sys::Error::new(message);
    if let Value::Object(properties) = properties {
        let serializer = serde_wasm_bindgen::Serializer::json_compatible();
        for (key, value) in properties {
            if key == "message" {
                continue;
            }
            if let Ok(value) = value.serialize(&serializer) {
                let _ = js_sys::Reflect::set(&error, &JsValue::from_str(key), &value);
            }
        }
    }
    error.into()
}
