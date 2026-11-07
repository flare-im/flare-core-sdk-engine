//! Unified contract API invoke — used by Tauri `sdk_invoke_json`, C `flare_sdk_invoke_json`, and wasm.

use std::borrow::Cow;

use flare_im_core_sdk::serde_json::{self, Value};

use crate::generated::dispatch::{capability, conversation, media, message, message_build};
use crate::operation::normalize_operation;
use crate::{BindingResponse, binding_operation_not_supported};
use flare_im_core_sdk::Result;
use flare_im_core_sdk::client::IMClient;
use flare_im_core_sdk::client::api::{
    CapabilityApi, ConversationApi, MediaApi, MessageApi, MessageBuildApi,
};
use std::future::Future;
use std::sync::Arc;

/// Session APIs required for contract invoke (implemented by Tauri [`SdkState`] and C [`SdkInstance`]).
pub trait InvokeSession: Send + Sync {
    fn client(&self) -> IMClient;
    fn message_api(&self) -> impl Future<Output = Result<MessageApi>> + Send;
    fn message_build_api(&self) -> impl Future<Output = Result<Arc<MessageBuildApi>>> + Send;
    fn conversation_api(&self) -> impl Future<Output = Result<ConversationApi>> + Send;
    fn media_api(&self) -> impl Future<Output = Result<Arc<MediaApi>>> + Send;
    fn capability_api(&self) -> impl Future<Output = Result<Arc<CapabilityApi>>> + Send;

    /// Clears binding-layer API caches after [`IMClient::disconnect`].
    fn after_disconnect(&self) -> impl Future<Output = ()> + Send;
}

pub async fn invoke_api_id_json(
    session: &impl InvokeSession,
    api_id: &str,
    request_json: &str,
) -> Result<BindingResponse> {
    let normalized = normalize_operation_json(api_id, request_json)?;
    invoke_normalized_json(session, &normalized.name, normalized.request.as_ref()).await
}

struct NormalizedJsonOperation<'a> {
    name: String,
    request: Cow<'a, str>,
}

fn normalize_operation_json<'a>(
    operation: &str,
    request_json: &'a str,
) -> Result<NormalizedJsonOperation<'a>> {
    if operation == "message_builder.dispatch" || operation.starts_with("message_builder.create_") {
        return normalize_operation_json_with_request(operation, request_json);
    }

    match operation {
        "rich_doc_v2.create_message" => {
            normalize_operation_json_with_request(operation, request_json)
        }
        op if op.starts_with("rtc.") => normalize_operation_json_with_request(op, request_json),
        "rich_doc_v2.edit_message" => Ok(NormalizedJsonOperation {
            name: "message.edit_rich_doc_by_message_id".to_string(),
            request: Cow::Borrowed(request_json),
        }),
        _ => Ok(NormalizedJsonOperation {
            name: operation.to_string(),
            request: Cow::Borrowed(request_json),
        }),
    }
}

fn normalize_operation_json_with_request<'a>(
    operation: &str,
    request_json: &'a str,
) -> Result<NormalizedJsonOperation<'a>> {
    let request = crate::dispatch_support::dispatch_params_from_json(request_json)?;
    let normalized = normalize_operation(operation, request);
    let request = serde_json::to_string(&normalized.request)
        .map_err(|e| crate::binding_invalid_parameter(format!("invalid request JSON: {e}")))?;
    Ok(NormalizedJsonOperation {
        name: normalized.name,
        request: Cow::Owned(request),
    })
}

fn message_dispatch_parts(request: Value) -> Result<(String, Value)> {
    let mut object = request.as_object().cloned().ok_or_else(|| {
        crate::binding_invalid_parameter("message.dispatch request must be an object")
    })?;
    let operation = object
        .remove("op")
        .and_then(|value| value.as_str().map(str::trim).map(str::to_owned))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| crate::binding_invalid_parameter("message.dispatch op is required"))?;
    let params = object
        .remove("params")
        .ok_or_else(|| crate::binding_invalid_parameter("message.dispatch params is required"))?;
    if !params.is_object() {
        return Err(crate::binding_invalid_parameter(
            "message.dispatch params must be an object",
        ));
    }
    Ok((operation, params))
}

fn message_dispatch_json_parts(request_json: &str) -> Result<(String, String)> {
    let request: Value = serde_json::from_str(request_json)
        .map_err(|e| crate::binding_invalid_parameter(format!("invalid request JSON: {e}")))?;
    let (operation, params) = message_dispatch_parts(request)?;
    let params_json = serde_json::to_string(&params)
        .map_err(|e| crate::binding_invalid_parameter(format!("invalid request JSON: {e}")))?;
    Ok((operation, params_json))
}

async fn invoke_normalized(
    session: &impl InvokeSession,
    route: &str,
    request: Value,
) -> Result<BindingResponse> {
    if crate::generated::direct_invoke::is_direct_invoke_route(route) {
        return crate::generated::direct_invoke::dispatch_direct(session, route, &request).await;
    }

    match route {
        "message.dispatch" => {
            let (op, params) = message_dispatch_parts(request)?;
            let api = session.message_api().await?;
            message::dispatch_message(&api, &op, params).await
        }
        "message.build" => {
            let api = session.message_build_api().await?;
            message_build::dispatch_message_build(&api, request).await
        }
        // 构建 + 发送合并为一次调用。
        //
        // 分开调用时，媒体消息的大载荷要跨绑定边界**三次**：
        // 宿主→build（带 `data:` 的 base64）、build→宿主（构建好的消息含同一份内容）、
        // 宿主→send（再交回来）。合并后只跨一次。
        //
        // 更要紧的是**乐观物化的时机**：`send_with_media` 一进来就把消息以
        // sending/uploading 落库并发总线（气泡立刻出现）。分开调用时这一步要等
        // build 那一次往返之后才发生；合并后提前到调用最开始。
        //
        // 参数与 `message.build` 完全一致（`op` + 各构建参数），
        // 因此宿主侧不需要为每种消息类型再写一遍发送逻辑。
        "message.build_and_send" => {
            let build_api = session.message_build_api().await?;
            let built = message_build::dispatch_message_build(&build_api, request).await?;
            let message = crate::dispatch_support::message_from_params(&built.payload)?;
            let api = session.message_api().await?;
            crate::dispatch_support::json_send_ack(api.send(message).await?)
        }
        name if name.starts_with("message.") => {
            let op = name.strip_prefix("message.").unwrap_or(name);
            let api = session.message_api().await?;
            message::dispatch_message(&api, op, request).await
        }
        name if name.starts_with("conversation.") => {
            let op = name.strip_prefix("conversation.").unwrap_or(name);
            let api = session.conversation_api().await?;
            conversation::dispatch_conversation(&api, op, request).await
        }
        name if name.starts_with("view.") => {
            let op = name.strip_prefix("view.").unwrap_or(name);
            let api = session.client().view_async().await?;
            dispatch_view(&api, op, request).await
        }
        name if name.starts_with("media.") => {
            let op = name.strip_prefix("media.").unwrap_or(name);
            let api = session.media_api().await?;
            media::dispatch_media(&api, op, request).await
        }
        "capability.list" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability(&api, &session.client(), "capability_list", request)
                .await
        }
        "capability.list_user" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability(
                &api,
                &session.client(),
                "capability_list_user",
                request,
            )
            .await
        }
        "capability.dispatch" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability(&api, &session.client(), "capability_dispatch", request)
                .await
        }
        "capability.grant" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability(&api, &session.client(), "capability_grant", request)
                .await
        }
        "capability.revoke" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability(&api, &session.client(), "capability_revoke", request)
                .await
        }
        name if name.starts_with("capability.") => {
            let api = session.capability_api().await?;
            capability::dispatch_capability(
                &api,
                &session.client(),
                name.strip_prefix("capability.").unwrap_or(name),
                request,
            )
            .await
        }
        _ => Err(binding_operation_not_supported(route)),
    }
}

pub async fn invoke_normalized_json(
    session: &impl InvokeSession,
    route: &str,
    request_json: &str,
) -> Result<BindingResponse> {
    if crate::generated::direct_invoke::is_direct_invoke_route(route) {
        return crate::generated::direct_invoke::dispatch_direct_json(session, route, request_json)
            .await;
    }

    match route {
        "message.dispatch" => {
            let (op, params_json) = message_dispatch_json_parts(request_json)?;
            let api = session.message_api().await?;
            message::dispatch_message_json(&api, &op, &params_json).await
        }
        "message.build" => {
            let api = session.message_build_api().await?;
            message_build::dispatch_message_build_json(&api, request_json).await
        }
        name if name.starts_with("message.") => {
            let op = name.strip_prefix("message.").unwrap_or(name);
            let api = session.message_api().await?;
            message::dispatch_message_json(&api, op, request_json).await
        }
        name if name.starts_with("conversation.") => {
            let op = name.strip_prefix("conversation.").unwrap_or(name);
            let api = session.conversation_api().await?;
            conversation::dispatch_conversation_json(&api, op, request_json).await
        }
        name if name.starts_with("view.") => {
            let op = name.strip_prefix("view.").unwrap_or(name);
            let api = session.client().view_async().await?;
            let request = crate::dispatch_support::dispatch_params_from_json(request_json)?;
            dispatch_view(&api, op, request).await
        }
        name if name.starts_with("media.") => {
            let op = name.strip_prefix("media.").unwrap_or(name);
            let api = session.media_api().await?;
            media::dispatch_media_json(&api, op, request_json).await
        }
        "capability.list" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability_json(
                &api,
                &session.client(),
                "capability_list",
                request_json,
            )
            .await
        }
        "capability.list_user" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability_json(
                &api,
                &session.client(),
                "capability_list_user",
                request_json,
            )
            .await
        }
        "capability.dispatch" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability_json(
                &api,
                &session.client(),
                "capability_dispatch",
                request_json,
            )
            .await
        }
        "capability.grant" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability_json(
                &api,
                &session.client(),
                "capability_grant",
                request_json,
            )
            .await
        }
        "capability.revoke" => {
            let api = session.capability_api().await?;
            capability::dispatch_capability_json(
                &api,
                &session.client(),
                "capability_revoke",
                request_json,
            )
            .await
        }
        name if name.starts_with("capability.") => {
            let api = session.capability_api().await?;
            capability::dispatch_capability_json(
                &api,
                &session.client(),
                name.strip_prefix("capability.").unwrap_or(name),
                request_json,
            )
            .await
        }
        _ => {
            let request = crate::dispatch_support::dispatch_params_from_json(request_json)?;
            invoke_normalized(session, route, request).await
        }
    }
}

async fn dispatch_view(
    api: &Arc<flare_im_core_sdk::client::api::ViewApi>,
    operation: &str,
    request: Value,
) -> Result<BindingResponse> {
    match operation {
        "timeline.open" => {
            let request = crate::dispatch_support::from_value::<
                flare_im_core_sdk::model::OpenTimelineViewRequest,
            >(request, "open timeline view request")?;
            crate::dispatch_support::json(api.open_timeline(request).await?)
        }
        "timeline.load_older" => {
            let request = crate::dispatch_support::from_value::<
                flare_im_core_sdk::model::LoadOlderTimelineViewRequest,
            >(request, "load older timeline view request")?;
            crate::dispatch_support::json(api.load_older_timeline(request).await?)
        }
        "timeline.load_newer" => {
            let request = crate::dispatch_support::from_value::<
                flare_im_core_sdk::model::LoadNewerTimelineViewRequest,
            >(request, "load newer timeline view request")?;
            crate::dispatch_support::json(api.load_newer_timeline(request).await?)
        }
        "timeline.jump_to_latest" => {
            let request = crate::dispatch_support::from_value::<
                flare_im_core_sdk::model::TimelineViewRequest,
            >(request, "jump to latest timeline view request")?;
            crate::dispatch_support::json(api.jump_to_latest_timeline(request).await?)
        }
        "timeline.snapshot" => {
            let request = crate::dispatch_support::from_value::<
                flare_im_core_sdk::model::TimelineViewRequest,
            >(request, "timeline view snapshot request")?;
            crate::dispatch_support::json(api.timeline_snapshot(request).await?)
        }
        "conversation_list.open" => {
            let request = crate::dispatch_support::from_value::<
                flare_im_core_sdk::model::OpenConversationListViewRequest,
            >(request, "open conversation list view request")?;
            crate::dispatch_support::json(api.open_conversation_list(request).await?)
        }
        "timeline.close" | "conversation_list.close" | "close" => {
            let request = crate::dispatch_support::from_value::<
                flare_im_core_sdk::model::CloseViewRequest,
            >(request, "close view request")?;
            crate::dispatch_support::json(api.close(request).await?)
        }
        _ => Err(binding_operation_not_supported(format!("view.{operation}"))),
    }
}

pub fn binding_response_to_value(response: BindingResponse) -> Value {
    if response.is_unit {
        Value::Null
    } else {
        response.payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_core_sdk::client::IMClient;
    use flare_im_core_sdk::client::api::{
        CapabilityApi, ConversationApi, MediaApi, MessageApi, MessageBuildApi,
    };
    use std::sync::Arc;

    struct DirectOnlySession {
        client: IMClient,
    }

    impl DirectOnlySession {
        fn new() -> Self {
            Self {
                client: IMClient::new(),
            }
        }
    }

    impl InvokeSession for DirectOnlySession {
        fn client(&self) -> IMClient {
            self.client.clone()
        }

        async fn message_api(&self) -> Result<MessageApi> {
            unreachable!("direct route tests must not require message api")
        }

        async fn message_build_api(&self) -> Result<Arc<MessageBuildApi>> {
            unreachable!("direct route tests must not require message build api")
        }

        async fn conversation_api(&self) -> Result<ConversationApi> {
            unreachable!("direct route tests must not require conversation api")
        }

        async fn media_api(&self) -> Result<Arc<MediaApi>> {
            unreachable!("direct route tests must not require media api")
        }

        async fn capability_api(&self) -> Result<Arc<CapabilityApi>> {
            unreachable!("direct route tests must not require capability api")
        }

        async fn after_disconnect(&self) {}
    }

    #[test]
    fn normalizes_message_builder_typed_dispatch_json_to_message_build() {
        let normalized = normalize_operation_json(
            "message_builder.dispatch",
            r#"{"op":"create_text","conversationId":"c1","text":"hello"}"#,
        )
        .expect("message builder dispatch should normalize");

        assert_eq!(normalized.name, "message.build");
        assert_eq!(
            normalized.request.as_ref(),
            r#"{"conversationId":"c1","op":"create_text","text":"hello"}"#
        );
    }

    #[test]
    fn extracts_message_dispatch_operation_and_params() {
        let request = serde_json::json!({
            "op": "search_in_conversation",
            "params": {
                "conversationId": "c1",
                "keyword": "hello"
            }
        });

        let (operation, params) =
            message_dispatch_parts(request).expect("dispatch request should split");

        assert_eq!(operation, "search_in_conversation");
        assert_eq!(params["conversationId"], "c1");
        assert_eq!(params["keyword"], "hello");
    }

    #[test]
    fn rejects_message_dispatch_without_params() {
        let err = message_dispatch_parts(serde_json::json!({ "op": "search" }))
            .expect_err("params is part of the public message.dispatch contract");

        assert!(
            err.to_string()
                .contains("message.dispatch params is required")
        );
    }

    /// `message.build_and_send` 必须**先**走构建 API，而不是落到通用 `message.*`
    /// 分支去要 message api——后者意味着路由没生效，参数会被当成一条已构建好的
    /// 消息去解析，报的是"缺少 message"这类完全误导的错。
    ///
    /// 两个 mock API 都会 panic，用 panic 信息区分走了哪条路。
    #[tokio::test]
    #[should_panic(expected = "message build api")]
    async fn build_and_send_routes_to_the_builder_first() {
        let session = DirectOnlySession::new();
        let _ = invoke_normalized(
            &session,
            "message.build_and_send",
            serde_json::json!({ "op": "create_text", "conversationId": "c1", "text": "hi" }),
        )
        .await;
    }

    #[tokio::test]
    async fn native_push_routes_are_available_before_login_and_strictly_typed() {
        let session = DirectOnlySession::new();
        for (id, body, expected) in [
            (
                "sdk.update_push_token",
                r#"{"provider":"fcm","platform":"android","token":"os-fixture"}"#,
                "disabled",
            ),
            (
                "sdk.set_push_permission",
                r#"{"allowed":true}"#,
                "waitingForSession",
            ),
            ("sdk.reconcile_push", "{}", "waitingForSession"),
            (
                "sdk.clear_push_token",
                r#"{"provider":"fcm"}"#,
                "waitingForSession",
            ),
            ("sdk.push_status", "{}", "waitingForSession"),
        ] {
            let response = invoke_api_id_json(&session, id, body).await.unwrap();
            let value = binding_response_to_value(response);
            assert_eq!(value["state"], expected);
            assert_eq!(value["registeredProviders"], 0);
            assert!(value.get("token").is_none());
        }
        for body in [
            r#"{"allowed":"true"}"#,
            r#"{"allowed":true,"userId":"other"}"#,
        ] {
            assert!(
                invoke_api_id_json(&session, "sdk.set_push_permission", body)
                    .await
                    .is_err()
            );
        }
    }

    /// 没有会话时状态是 `idle`，且每个字段都出现（宿主整体替换，不做合并）。
    #[tokio::test]
    async fn direct_connection_status_without_a_session_is_idle() {
        let session = DirectOnlySession::new();

        let response = invoke_api_id_json(&session, "connection.get_status", "{}")
            .await
            .expect("connection status");
        let value = binding_response_to_value(response);

        assert_eq!(value["phase"], "idle");
        assert_eq!(value["attempt"], 0);
        assert!(value.get("accessPointId").is_some_and(Value::is_null));
        assert!(value.get("nextRetryAt").is_some_and(Value::is_null));
    }

    /// Hosts read `{ offsetMs, estimated }`; without a session nothing is estimated yet, so the
    /// host keeps its own clock rather than treating the offset as a measured zero.
    #[tokio::test]
    async fn direct_server_clock_returns_offset_and_whether_it_is_estimated() {
        let session = DirectOnlySession::new();

        let value = binding_response_to_value(
            invoke_api_id_json(&session, "connection.server_clock", "{}")
                .await
                .expect("server clock"),
        );

        assert_eq!(
            value,
            serde_json::json!({ "offsetMs": 0, "estimated": false })
        );
    }

    #[tokio::test]
    async fn direct_boolean_routes_return_bare_booleans() {
        let session = DirectOnlySession::new();

        let connected = binding_response_to_value(
            invoke_api_id_json(&session, "sdk.is_connected", "{}")
                .await
                .expect("is connected"),
        );
        let active = binding_response_to_value(
            invoke_api_id_json(&session, "sdk.session_active", "{}")
                .await
                .expect("session active"),
        );

        assert_eq!(connected, serde_json::json!(false));
        assert_eq!(active, serde_json::json!(false));
    }

    #[tokio::test]
    async fn direct_rich_doc_normalization_runs_without_module_api() {
        let session = DirectOnlySession::new();
        let doc_json = r#"{
            "type": "doc",
            "version": 2,
            "children": [{
                "type": "paragraph",
                "children": [{"type": "text", "text": "hello flare"}]
            }]
        }"#;
        let request = serde_json::json!({
            "docJson": doc_json,
        });

        let response = invoke_api_id_json(
            &session,
            "rich_doc_v2.normalize_from_doc_json",
            &request.to_string(),
        )
        .await
        .expect("rich doc direct normalize");
        let value = binding_response_to_value(response);

        assert_eq!(value["plainText"], "hello flare");
        assert_eq!(value["contentSchema"], "rich_doc");
    }
}
