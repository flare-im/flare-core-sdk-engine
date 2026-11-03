use serde_json::Value;

/// Operation-level boundary between wasm-bindgen and a concrete core runtime.
///
/// Implementations may be temporary in-memory smoke runtimes or production
/// adapters backed by `flare-core-sdk-engine/src`. The wasm export layer should not
/// know which one it is calling.
pub trait WasmRuntimePort {
    fn invoke_json(&mut self, operation: &str, request: Value) -> Result<Value, WasmRuntimeError>;
    fn dispose(&mut self);
}

#[derive(Debug, Clone)]
pub struct WasmRuntimeError {
    pub code: &'static str,
    pub operation: String,
    pub message: String,
}

impl WasmRuntimeError {
    pub fn new(
        code: &'static str,
        operation: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            operation: operation.into(),
            message: message.into(),
        }
    }
}
