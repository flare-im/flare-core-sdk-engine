//! Shared contract runtime for Flare IM Core SDK bindings.
//!
//! This crate is the L1 dispatch boundary used by C, Tauri, UniFFI, and Wasm.
//! IM behavior stays in `flare-im-core-sdk`; this crate owns contract metadata,
//! JSON boundary helpers, operation normalization, and session-aware routing.

use flare_im_core_sdk::serde_json::{Value, json};

pub mod contract;
pub mod dispatch_support;
pub mod error;
pub mod event;
pub mod generated;
pub mod invocation;
pub mod invoke;
#[cfg(target_arch = "wasm32")]
pub mod js_error;
pub mod operation;
pub mod request;
pub mod session;
pub mod task_slot;

pub use contract::{
    API_CONTRACT_VERSION, API_OPERATIONS, BINDING_CONTRACT_VERSION, ERROR_CODES,
    ERROR_CONTRACT_VERSION, EVENT_CONTRACT_VERSION, EVENT_DESCRIPTORS, ErrorCode, EventDescriptor,
    MessageBuildCatalogEntry, find_api_operation, find_error_code, find_event_by_code,
    find_event_by_id,
};
pub use error::{
    BindingBoundaryError, BindingBoundaryResult, binding_invalid_parameter,
    binding_operation_not_supported, sdk_error_details, sdk_error_value,
};
pub use event::{
    platform_event_bridge_resync_marker, sdk_event_batch_json, sdk_event_channel_payload,
    sdk_event_code, sdk_event_json, sdk_event_payload, sdk_event_web_payload,
};
pub use generated::dispatch::{capability, conversation, media, message, message_build};
pub use generated::{
    CAPABILITY_DISPATCH_OPERATIONS, CLIENT_CONFIG_CONTRACT_JSON, CLIENT_INIT_REQUEST_EXAMPLE_JSON,
    CONVERSATION_DISPATCH_OPERATIONS, MEDIA_DISPATCH_OPERATIONS, MESSAGE_BUILD_OPERATIONS,
    MESSAGE_DISPATCH_OPERATIONS,
};
pub use invoke::{InvokeSession, binding_response_to_value, invoke_api_id_json};
#[cfg(target_arch = "wasm32")]
pub use js_error::{boundary_error_js, sdk_error_js};
pub use operation::{NormalizedOperation, message_build_catalog, normalize_operation};
pub use request::{BindingRequest, BindingResponse};
pub use session::SessionSlot;
pub use task_slot::SessionTaskSlot;

pub fn contract_json() -> String {
    let operations = API_OPERATIONS
        .iter()
        .filter(|operation| !operation.dev_only)
        .map(|operation| {
            json!({
                "id": operation.id,
                "module": operation.module,
                "core": operation.core,
                "c_symbol": operation.c_symbol,
                "c_dispatch_op": operation.c_dispatch_op,
                "tauri": operation.tauri,
                "dev_only": operation.dev_only,
            })
        })
        .collect::<Vec<_>>();
    let events = EVENT_DESCRIPTORS
        .iter()
        .map(|event| {
            json!({
                "id": event.id,
                "c_code": event.c_code,
                "c_code_name": event.c_code_name,
                "tauri": event.tauri,
            })
        })
        .collect::<Vec<_>>();
    let errors = ERROR_CODES
        .iter()
        .map(|error| {
            json!({
                "name": error.name,
                "code": error.code,
                "meaning": error.meaning,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "binding_contract_version": BINDING_CONTRACT_VERSION,
        "api_contract_version": API_CONTRACT_VERSION,
        "event_contract_version": EVENT_CONTRACT_VERSION,
        "error_contract_version": ERROR_CONTRACT_VERSION,
        "operations": operations,
        "events": events,
        "errors": errors,
    })
    .to_string()
}

pub fn client_init_request_example_json() -> String {
    CLIENT_INIT_REQUEST_EXAMPLE_JSON.to_string()
}

pub fn client_config_contract_json() -> String {
    CLIENT_CONFIG_CONTRACT_JSON.to_string()
}

pub fn binding_response_to_json_value(response: BindingResponse) -> Value {
    binding_response_to_value(response)
}

#[cfg(test)]
mod tests {
    use flare_im_core_sdk::serde_json::{self, Value};

    #[test]
    fn contract_json_exposes_generated_contract_metadata() {
        let document: Value = serde_json::from_str(&super::contract_json()).unwrap();

        assert_eq!(
            document["binding_contract_version"],
            super::BINDING_CONTRACT_VERSION
        );
        assert!(document["operations"].as_array().unwrap().len() > 100);
        assert!(
            document["operations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["id"] == "message.send")
        );
        assert!(
            document["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["id"] == "message.received")
        );
    }

    #[test]
    fn generate_core_token_is_gone_from_every_binding_surface() {
        // 客户端本地签发 = 签名密钥进客户端。契约里不能再有这条路：token 由网关签发/刷新
        //（SDK 托管）或由应用拿（业务后端）。
        assert!(super::find_api_operation("sdk.generate_core_token").is_none());
        let contract: serde_json::Value = serde_json::from_str(&super::contract_json()).unwrap();
        assert!(
            !contract["operations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["id"] == "sdk.generate_core_token")
        );
        assert!(super::find_api_operation("sdk.update_access_token").is_some());
    }

    #[test]
    fn contract_json_exposes_only_canonical_singular_api_ids() {
        let document: Value = serde_json::from_str(&super::contract_json()).unwrap();
        let operations = document["operations"].as_array().unwrap();

        assert!(operations.iter().any(|item| item["id"] == "message.send"));
        assert!(
            operations
                .iter()
                .any(|item| item["id"] == "message_builder.create_text")
        );
        assert!(
            operations
                .iter()
                .any(|item| item["id"] == "conversation.list")
        );
        assert!(
            operations
                .iter()
                .any(|item| item["id"] == "event.subscribe")
        );
        assert!(operations.iter().any(|item| item["id"] == "media.get_url"));

        for removed in [
            "messages.send",
            "messages.build.text",
            "messages.create_text_direct",
            "conversations.list",
            "events.subscribe",
            "capabilities.list",
            "media.get_file_url",
            "sync.set_conversation_input_state",
        ] {
            assert!(
                !operations.iter().any(|item| item["id"] == removed),
                "removed compatibility API id leaked into contract: {removed}"
            );
        }
    }
}
