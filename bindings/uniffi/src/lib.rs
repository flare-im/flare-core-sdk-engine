//! UniFFI binding facade over contract-generated metadata.
//!
//! Full UniFFI API surface should be layered on `bindings/shared` dispatch;
//! extend `bindings/contract/*.json` and run `cargo xtask core-codegen`.

pub mod generated;

pub use generated::client_config::{client_config_contract_json, client_init_request_example_json};
pub use generated::contract::BINDING_CONTRACT_VERSION;
pub use generated::events::EVENT_CODE_TABLE;
pub use generated::invoke::invoke_contract_api;
pub use generated::types::{BindingErrorCode, BindingEventId};

pub fn binding_contract_version() -> &'static str {
    BINDING_CONTRACT_VERSION
}

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum UniFfiSdkError {
    #[error("{message}")]
    Sdk { message: String },
}

impl From<flare_im_core_sdk::FlareError> for UniFfiSdkError {
    fn from(value: flare_im_core_sdk::FlareError) -> Self {
        Self::Sdk {
            message: value.to_string(),
        }
    }
}

impl From<serde_json::Error> for UniFfiSdkError {
    fn from(value: serde_json::Error) -> Self {
        Self::Sdk {
            message: value.to_string(),
        }
    }
}

#[derive(uniffi::Object)]
pub struct FlareImCoreClient {
    _client: flare_im_core_sdk::IMClient,
    _runtime: tokio::runtime::Runtime,
}

#[uniffi::export]
impl FlareImCoreClient {
    #[uniffi::constructor]
    pub fn new(config_json: String) -> Result<Self, UniFfiSdkError> {
        let _config: serde_json::Value = serde_json::from_str(&config_json)?;
        let client = flare_im_core_sdk::IMClient::new();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("flare-im-uniffi")
            .build()
            .map_err(|error| UniFfiSdkError::Sdk {
                message: error.to_string(),
            })?;
        Ok(Self {
            _client: client,
            _runtime: runtime,
        })
    }

    pub fn invoke_json(&self, request_json: String) -> String {
        let request = serde_json::from_str::<serde_json::Value>(&request_json)
            .unwrap_or(serde_json::Value::Null);
        let api_id = request
            .get("api_id")
            .or_else(|| request.get("operation"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let payload = request
            .get("payload")
            .or_else(|| request.get("request"))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        match invoke_contract_api(api_id, &payload.to_string()) {
            Ok(value) => {
                serde_json::json!({ "ok": true, "data": value, "error": null }).to_string()
            }
            Err(message) => serde_json::json!({
                "ok": false,
                "data": null,
                "error": {
                    "code": "uniffi.invoke_unavailable",
                    "message": message
                }
            })
            .to_string(),
        }
    }

    pub fn poll_event_json(&self) -> Result<Option<String>, UniFfiSdkError> {
        Ok(None)
    }

    pub fn snapshot_json(&self) -> Result<String, UniFfiSdkError> {
        Err(unsupported("snapshot_json"))
    }

    pub fn restore_snapshot_json(&self, _snapshot_json: String) -> Result<String, UniFfiSdkError> {
        Err(unsupported("restore_snapshot_json"))
    }
}

uniffi::setup_scaffolding!();

fn unsupported(operation: &str) -> UniFfiSdkError {
    UniFfiSdkError::Sdk {
        message: format!("{operation} is not supported by the current typed SDK surface"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    #[test]
    fn uniffi_client_reports_invoke_placeholder() {
        let client = super::FlareImCoreClient::new(
            serde_json::json!({
                "endpoint": "memory://local",
                "tenantId": "tenant_a",
                "userId": "user_a",
                "deviceId": "device_a",
                "accessToken": "test_token",
                "transport": "web_socket",
                "outboundQueueCapacity": 64,
                "eventBufferCapacity": 64
            })
            .to_string(),
        )
        .unwrap();

        let response = client.invoke_json(
            serde_json::json!({
                "api_id": "connection.get_status",
                "payload": {}
            })
            .to_string(),
        );
        let value: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(value["ok"], false);
        assert_eq!(value["error"]["code"], "uniffi.invoke_unavailable");

        assert!(client.poll_event_json().unwrap().is_none());
        assert!(client.snapshot_json().is_err());
        assert!(client.restore_snapshot_json("{}".to_string()).is_err());
    }
}
