//! Shared async JSON dispatch helpers for generated C entrypoints.

use std::ffi::{c_char, c_void};
use std::future::Future;
use std::pin::Pin;

use crate::abi;
use crate::error_convert::FLARE_ERR_JSON_PARSE;
use crate::executor::{CallbackContext, execute_async, execute_async_unit, return_error};
use crate::helpers::{c_str_to_string, to_json_string};
use crate::registry::SdkInstance;
use crate::registry::require_instance;
use crate::types::{FlareHandle, FlareResultCallback};
use flare_im_core_sdk_bindings_runtime::{BindingResponse, InvokeSession, invoke_api_id_json};
use std::sync::Arc;

type DispatchFut = Pin<Box<dyn Future<Output = flare_im_core_sdk::Result<BindingResponse>> + Send>>;

fn validate_json_str(json: &str) -> Result<(), i32> {
    serde_json::from_str::<Box<serde_json::value::RawValue>>(json)
        .map(|_| ())
        .map_err(|_| FLARE_ERR_JSON_PARSE)
}

pub(crate) fn binding_response_to_json(response: BindingResponse) -> Result<String, i32> {
    if response.is_unit {
        Ok(String::new())
    } else {
        to_json_string(&response.payload)
    }
}

impl InvokeSession for SdkInstance {
    fn client(&self) -> flare_im_core_sdk::client::IMClient {
        self.client.clone()
    }

    async fn message_api(
        &self,
    ) -> flare_im_core_sdk::Result<flare_im_core_sdk::client::api::MessageApi> {
        self.message_api().await
    }

    async fn message_build_api(
        &self,
    ) -> flare_im_core_sdk::Result<std::sync::Arc<flare_im_core_sdk::client::api::MessageBuildApi>>
    {
        self.message_build_api().await
    }

    async fn conversation_api(
        &self,
    ) -> flare_im_core_sdk::Result<flare_im_core_sdk::client::api::ConversationApi> {
        self.conversation_api().await
    }

    async fn media_api(
        &self,
    ) -> flare_im_core_sdk::Result<std::sync::Arc<flare_im_core_sdk::client::api::MediaApi>> {
        self.media_api().await
    }

    async fn capability_api(
        &self,
    ) -> flare_im_core_sdk::Result<std::sync::Arc<flare_im_core_sdk::client::api::CapabilityApi>>
    {
        self.capability_api().await
    }

    async fn after_disconnect(&self) {
        self.im_session.clear().await;
    }
}

pub(crate) fn json_dispatch_entry(
    handle: FlareHandle,
    op: *const c_char,
    params_json: *const c_char,
    context: *mut c_void,
    callback: FlareResultCallback,
    run: impl FnOnce(Arc<SdkInstance>, String, String) -> DispatchFut + Send + 'static,
) -> i32 {
    abi::catch_ffi_i32(|| {
        let instance = match require_instance(handle) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let operation = match c_str_to_string(op) {
            Ok(s) => s,
            Err(code) => {
                let ctx = CallbackContext::new(context, callback);
                return_error(&ctx, code, "Invalid op");
                return code;
            }
        };
        let scan_limit =
            flare_im_core_sdk_bindings_runtime::dispatch_support::plugin_file_upload_json_limit(
                &operation,
            )
            .unwrap_or(abi::MAX_CSTRING_BYTES);
        let params_json = match abi::read_c_str_with_limit(params_json, scan_limit) {
            Ok(s) => s,
            Err(code) => {
                let ctx = CallbackContext::new(context, callback);
                return_error(&ctx, code, "Invalid params JSON");
                return code;
            }
        };
        if let Err(code) = validate_json_str(&params_json) {
            let ctx = CallbackContext::new(context, callback);
            return_error(&ctx, code, "Invalid params JSON");
            return code;
        }
        let ctx = CallbackContext::new(context, callback);
        execute_async(
            instance.clone(),
            &operation.clone(),
            ctx,
            async move { run(instance, operation, params_json).await },
            binding_response_to_json,
        );
        0
    })
}

pub(crate) fn message_build_dispatch_entry(
    handle: FlareHandle,
    request_json: *const c_char,
    context: *mut c_void,
    callback: FlareResultCallback,
) -> i32 {
    abi::catch_ffi_i32(|| {
        let instance = match require_instance(handle) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let request_json = match c_str_to_string(request_json) {
            Ok(s) => s,
            Err(code) => {
                let ctx = CallbackContext::new(context, callback);
                return_error(&ctx, code, "Invalid request JSON");
                return code;
            }
        };
        if let Err(code) = validate_json_str(&request_json) {
            let ctx = CallbackContext::new(context, callback);
            return_error(&ctx, code, "Invalid request JSON");
            return code;
        }
        let ctx = CallbackContext::new(context, callback);
        execute_async(
            instance.clone(),
            "message_builder.dispatch",
            ctx,
            async move {
                let api = instance.message_build_api().await?;
                flare_im_core_sdk_bindings_runtime::message_build::dispatch_message_build_json(
                    &api,
                    &request_json,
                )
                .await
            },
            binding_response_to_json,
        );
        0
    })
}

pub(crate) fn typed_invoke_unit(
    instance: Arc<SdkInstance>,
    ctx: CallbackContext,
    api_id: &str,
    params_json: String,
) {
    let inst = instance.clone();
    let api_id = api_id.to_string();
    execute_async_unit(instance, ctx, async move {
        let _ = invoke_api_id_json(inst.as_ref(), &api_id, &params_json).await?;
        Ok(())
    });
}

pub(crate) fn typed_invoke_json(
    instance: Arc<SdkInstance>,
    ctx: CallbackContext,
    api_id: &str,
    params_json: String,
) {
    let inst = instance.clone();
    let api_id = api_id.to_string();
    execute_async(
        instance,
        &api_id.clone(),
        ctx,
        async move { invoke_api_id_json(inst.as_ref(), &api_id, &params_json).await },
        binding_response_to_json,
    );
}

pub(crate) fn typed_invoke_send_ack(
    instance: Arc<SdkInstance>,
    ctx: CallbackContext,
    api_id: &str,
    params_json: String,
) {
    typed_invoke_json(instance, ctx, api_id, params_json);
}

pub(crate) fn invoke_entry(
    handle: FlareHandle,
    api_id: *const c_char,
    params_json: *const c_char,
    context: *mut c_void,
    callback: FlareResultCallback,
) -> i32 {
    abi::catch_ffi_i32(|| {
        let instance = match require_instance(handle) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let api_id = match c_str_to_string(api_id) {
            Ok(s) => s,
            Err(code) => {
                let ctx = CallbackContext::new(context, callback);
                return_error(&ctx, code, "Invalid api_id");
                return code;
            }
        };
        let scan_limit =
            flare_im_core_sdk_bindings_runtime::dispatch_support::plugin_file_upload_json_limit(
                &api_id,
            )
            .unwrap_or(abi::MAX_CSTRING_BYTES);
        let params_json = match abi::read_c_str_with_limit(params_json, scan_limit) {
            Ok(s) => s,
            Err(code) => {
                let ctx = CallbackContext::new(context, callback);
                return_error(&ctx, code, "Invalid params JSON");
                return code;
            }
        };
        if let Err(code) = validate_json_str(&params_json) {
            let ctx = CallbackContext::new(context, callback);
            return_error(&ctx, code, "Invalid params JSON");
            return code;
        }
        let ctx = CallbackContext::new(context, callback);
        execute_async(
            instance.clone(),
            &api_id.clone(),
            ctx,
            async move { invoke_api_id_json(instance.as_ref(), &api_id, &params_json).await },
            binding_response_to_json,
        );
        0
    })
}
