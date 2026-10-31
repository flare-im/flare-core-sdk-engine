//! Shared helpers for generated JSON dispatch modules.

use std::collections::HashMap;

use flare_im_core_sdk::serde::{self, Deserialize};
use flare_im_core_sdk::serde_json::{self, Value};

use crate::{BindingResponse, binding_invalid_parameter};
use flare_im_core_sdk::Result;
use flare_im_core_sdk::client::HeartbeatAppState;
use flare_im_core_sdk::client::api::{
    CreateLocationRequest, CreateRichDocRequest, CreateStickerRequest, EditRichDocRequest,
};
use flare_im_core_sdk::content::content_builder::BuiltContent;
use flare_im_core_sdk::content::message_elem::{Elem, elem_to_message_content};
use flare_im_core_sdk::model::conversation::ConversationType;
use flare_im_core_sdk::model::media::UploadOptions;
use flare_im_core_sdk::model::message::{
    IMMessage, ImageFormat, ImageInfo, MarkType, MessageType, SendAck, send_ack,
};
#[cfg(not(target_arch = "wasm32"))]
use flare_im_core_sdk::{ErrorCode, FlareError};

#[derive(Debug, flare_im_core_sdk::serde::Deserialize)]
#[serde(crate = "flare_im_core_sdk::serde", rename_all = "camelCase")]
struct ImageGroupBuildItem {
    image_id: String,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    mime_type: Option<String>,
    #[serde(default)]
    size: Option<i64>,
    #[serde(default)]
    width: Option<i32>,
    #[serde(default)]
    height: Option<i32>,
    #[serde(default)]
    format: Option<i32>,
    #[serde(default)]
    animated: Option<bool>,
    #[serde(default)]
    blurhash: Option<String>,
}

#[derive(Debug, flare_im_core_sdk::serde::Deserialize)]
#[serde(crate = "flare_im_core_sdk::serde")]
struct DispatchOperationRequest {
    op: String,
}

pub fn json<T: serde::Serialize>(value: T) -> Result<BindingResponse> {
    serde_json::to_value(value)
        .map(BindingResponse::json)
        .map_err(|e| {
            binding_invalid_parameter(format!("failed to serialize binding response: {e}"))
        })
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn run_cpu_bound<T, F>(label: &'static str, work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work).await.map_err(|error| {
        FlareError::localized(
            ErrorCode::InternalError,
            format!("{label} blocking worker failed: {error}"),
        )
    })?
}

#[cfg(target_arch = "wasm32")]
pub async fn run_cpu_bound<T, F>(_label: &'static str, work: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    work()
}

fn canonical_send_ack_id(ack: &SendAck) -> &str {
    ack.ack_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(ack.client_msg_id.as_str())
}

pub fn json_send_ack(ack: SendAck) -> Result<BindingResponse> {
    let ack_id = canonical_send_ack_id(&ack).to_string();
    let (server_msg_id, seq, timestamp, success, error_code, error_message) =
        match ack.result.as_ref() {
            Some(send_ack::Result::Accepted(accepted)) => (
                accepted.server_msg_id.clone(),
                accepted.conversation_seq,
                accepted.server_time,
                true,
                0,
                String::new(),
            ),
            Some(send_ack::Result::Error(error)) => (
                String::new(),
                0,
                0,
                false,
                error.code,
                error.message.clone(),
            ),
            // Reliable queue sends resolve once the message is accepted into the local
            // queue; the durable SendAck arrives later through EventBus. Treat the empty
            // result as an accepted enqueue, not as a send failure.
            None => (String::new(), 0, 0, true, 0, String::new()),
        };
    Ok(BindingResponse::json(serde_json::json!({
        "clientMsgId": ack.client_msg_id,
        "serverId": server_msg_id,
        "seq": seq,
        "conversationId": ack.conversation_id,
        "ackId": ack_id,
        "timestamp": timestamp,
        "success": success,
        "errorCode": error_code,
        "errorMessage": error_message,
    })))
}

pub fn conversation_id(value: &Value) -> Result<String> {
    json_string(value, "conversationId")
}

pub fn json_string(value: &Value, key: &str) -> Result<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| binding_invalid_parameter(format!("missing or invalid JSON field: {key}")))
}

pub fn optional_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

pub fn string_any(value: &Value, keys: &[&str]) -> Result<String> {
    keys.iter()
        .find_map(|key| {
            value
                .get(*key)
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .ok_or_else(|| {
            binding_invalid_parameter(format!("missing or invalid JSON field: {}", keys.join("/")))
        })
}

pub fn optional_string_any(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    })
}

pub fn json_u64(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(|x| {
            x.as_u64()
                .or_else(|| x.as_i64().and_then(|i| u64::try_from(i).ok()))
        })
        .ok_or_else(|| binding_invalid_parameter(format!("missing or invalid JSON field: {key}")))
}

pub fn json_i64(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| binding_invalid_parameter(format!("missing or invalid JSON field: {key}")))
}

pub fn json_i32(value: &Value, key: &str) -> Result<i32> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .and_then(|i| i32::try_from(i).ok())
        .ok_or_else(|| binding_invalid_parameter(format!("missing or invalid JSON field: {key}")))
}

pub fn json_f64(value: &Value, key: &str) -> Result<f64> {
    value
        .get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| binding_invalid_parameter(format!("missing or invalid JSON field: {key}")))
}

pub fn json_bool(value: &Value, key: &str) -> Result<bool> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| binding_invalid_parameter(format!("missing or invalid JSON field: {key}")))
}

pub fn optional_bool(value: &Value, key: &str) -> Result<Option<bool>> {
    let Some(raw) = value.get(key) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    raw.as_bool()
        .map(Some)
        .ok_or_else(|| binding_invalid_parameter(format!("invalid JSON bool field: {key}")))
}

pub fn optional_upload_options(value: &Value, key: &str) -> Result<Option<UploadOptions>> {
    let Some(raw) = value.get(key) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    let Some(chunk_size) = raw.get("chunkSize").and_then(|v| v.as_u64()) else {
        return Ok(None);
    };
    let chunk_size = usize::try_from(chunk_size).map_err(|_| {
        binding_invalid_parameter(format!("invalid upload options field: {key}.chunkSize"))
    })?;
    if chunk_size == 0 {
        return Err(binding_invalid_parameter(format!(
            "invalid upload options field: {key}.chunkSize"
        )));
    }
    Ok(Some(UploadOptions { chunk_size }))
}

pub fn json_bytes_vec(value: &Value, key: &str) -> Result<Vec<u8>> {
    let arr = value.get(key).and_then(Value::as_array).ok_or_else(|| {
        binding_invalid_parameter(format!("missing or invalid JSON field: {key}"))
    })?;
    arr.iter()
        .map(|v| {
            v.as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| {
                    binding_invalid_parameter(format!("invalid JSON bytes field: {key}"))
                })
        })
        .collect()
}

pub fn json_vec_string(value: &Value, key: &str) -> Result<Vec<String>> {
    let arr = value.get(key).and_then(Value::as_array).ok_or_else(|| {
        binding_invalid_parameter(format!("missing or invalid JSON field: {key}"))
    })?;
    arr.iter()
        .map(|v| {
            v.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                binding_invalid_parameter(format!("invalid JSON array field: {key}"))
            })
        })
        .collect()
}

pub fn json_vec_message(value: &Value, key: &str) -> Result<Vec<IMMessage>> {
    let arr = value.get(key).and_then(Value::as_array).ok_or_else(|| {
        binding_invalid_parameter(format!("missing or invalid JSON field: {key}"))
    })?;
    arr.iter()
        .map(|v| {
            IMMessage::from_json_value(v.clone())
                .map_err(|e| binding_invalid_parameter(format!("invalid {key}: {e}")))
        })
        .collect()
}

fn non_negative_i64(value: Option<i64>, label: &str) -> Result<i64> {
    match value {
        Some(v) if v < 0 => Err(binding_invalid_parameter(format!(
            "{label} must be greater than or equal to zero"
        ))),
        Some(v) => Ok(v),
        None => Ok(0),
    }
}

fn non_negative_i32(value: Option<i32>, label: &str) -> Result<i32> {
    match value {
        Some(v) if v < 0 => Err(binding_invalid_parameter(format!(
            "{label} must be greater than or equal to zero"
        ))),
        Some(v) => Ok(v),
        None => Ok(0),
    }
}

fn image_format_or_default(value: Option<i32>) -> Result<i32> {
    let format = value.unwrap_or(ImageFormat::Unspecified as i32);
    ImageFormat::try_from(format).map(|_| format).map_err(|_| {
        binding_invalid_parameter(format!("invalid payload.images[].format: {format}"))
    })
}

pub fn image_group_images(value: &Value) -> Result<Vec<ImageInfo>> {
    let payload = value.get("payload").ok_or_else(|| {
        binding_invalid_parameter("missing payload for image group message build")
    })?;
    let items = payload
        .get("images")
        .cloned()
        .ok_or_else(|| binding_invalid_parameter("missing payload.images for image group"))?;
    let items: Vec<ImageGroupBuildItem> = from_value(items, "payload.images")?;
    items
        .into_iter()
        .map(|item| {
            let image_id = item.image_id.trim().to_string();
            if image_id.is_empty() {
                return Err(binding_invalid_parameter(
                    "payload.images[].imageId must not be empty",
                ));
            }
            Ok(ImageInfo {
                uuid: image_id.clone(),
                image_id,
                url: item.url.unwrap_or_default(),
                mime_type: item.mime_type.unwrap_or_default(),
                size: non_negative_i64(item.size, "payload.images[].size")?,
                width: non_negative_i32(item.width, "payload.images[].width")?,
                height: non_negative_i32(item.height, "payload.images[].height")?,
                format: image_format_or_default(item.format)?,
                animated: item.animated.unwrap_or_default(),
                blurhash: item.blurhash.unwrap_or_default(),
            })
        })
        .collect()
}

pub fn image_group_description(value: &Value) -> String {
    value
        .get("payload")
        .and_then(|payload| {
            payload
                .get("description")
                .or_else(|| payload.get("title"))
                .and_then(Value::as_str)
        })
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .unwrap_or_default()
        .to_string()
}

pub fn image_group_metadata(value: &Value) -> Result<HashMap<String, String>> {
    let Some(payload) = value.get("payload") else {
        return Ok(HashMap::new());
    };
    payload
        .get("metadata")
        .or_else(|| payload.get("attributes"))
        .cloned()
        .map(|metadata| from_value(metadata, "payload.metadata"))
        .transpose()
        .map(|metadata| metadata.unwrap_or_default())
}

pub fn optional_value<T: for<'de> Deserialize<'de>>(value: &Value, key: &str) -> Result<Option<T>> {
    value
        .get(key)
        .cloned()
        .map(|v| from_value(v, key))
        .transpose()
}

pub fn optional_u32(value: &Value, key: &str) -> Result<Option<u32>> {
    let Some(raw) = value.get(key) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    raw.as_u64()
        .and_then(|v| u32::try_from(v).ok())
        .map(Some)
        .ok_or_else(|| binding_invalid_parameter(format!("invalid JSON u32 field: {key}")))
}

pub fn optional_i32(value: &Value, key: &str) -> Result<Option<i32>> {
    let Some(raw) = value.get(key) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    raw.as_i64()
        .and_then(|v| i32::try_from(v).ok())
        .map(Some)
        .ok_or_else(|| binding_invalid_parameter(format!("invalid JSON i32 field: {key}")))
}

fn optional_u8_strict(value: &Value, key: &str) -> Result<Option<u8>> {
    let Some(raw) = value.get(key) else {
        return Ok(None);
    };
    if raw.is_null() {
        return Ok(None);
    }
    raw.as_u64()
        .and_then(|v| u8::try_from(v).ok())
        .map(Some)
        .ok_or_else(|| binding_invalid_parameter(format!("invalid JSON u8 field: {key}")))
}

pub fn heartbeat_app_state(value: &Value, key: &str) -> Result<HeartbeatAppState> {
    let raw = value.get(key).ok_or_else(|| {
        binding_invalid_parameter(format!("missing or invalid JSON field: {key}"))
    })?;

    if let Some(text) = raw.as_str() {
        return match text {
            "foreground" => Ok(HeartbeatAppState::Foreground),
            "background" => Ok(HeartbeatAppState::Background),
            other => Err(binding_invalid_parameter(format!(
                "invalid app_state: {other}"
            ))),
        };
    }

    if let Some(index) = raw.as_u64() {
        return match index {
            0 => Ok(HeartbeatAppState::Foreground),
            1 => Ok(HeartbeatAppState::Background),
            other => Err(binding_invalid_parameter(format!(
                "invalid app_state index: {other}"
            ))),
        };
    }

    Err(binding_invalid_parameter(
        "app_state must be a string or enum index",
    ))
}

pub fn message_from_params(params: &Value) -> Result<IMMessage> {
    match IMMessage::from_json_value(params.clone()) {
        Ok(message) => Ok(message),
        Err(top_level_error) => {
            let Some(message_value) = params.get("message").cloned() else {
                return Err(binding_invalid_parameter(format!(
                    "missing or invalid JSON message: {top_level_error}; missing message"
                )));
            };
            IMMessage::from_json_value(message_value).map_err(|wrapped_error| {
                binding_invalid_parameter(format!(
                    "missing or invalid JSON message: {wrapped_error}; wrapper: {top_level_error}"
                ))
            })
        }
    }
}

pub fn message_from_json_str(params_json: &str) -> Result<IMMessage> {
    match IMMessage::from_json_str(params_json) {
        Ok(message) => Ok(message),
        Err(top_level_error) => {
            let params = serde_json::from_str::<Value>(params_json).map_err(|params_error| {
                binding_invalid_parameter(format!(
                    "missing or invalid JSON message: {top_level_error}; invalid params JSON: {params_error}"
                ))
            })?;
            let Some(message_value) = params.get("message").cloned() else {
                return Err(binding_invalid_parameter(format!(
                    "missing or invalid JSON message: {top_level_error}; missing message"
                )));
            };
            IMMessage::from_json_value(message_value).map_err(|wrapped_error| {
                binding_invalid_parameter(format!(
                    "missing or invalid JSON message: {wrapped_error}; wrapper: {top_level_error}"
                ))
            })
        }
    }
}

pub fn from_value<T: for<'de> Deserialize<'de>>(value: Value, label: &str) -> Result<T> {
    serde_json::from_value(value)
        .map_err(|e| binding_invalid_parameter(format!("invalid {label}: {e}")))
}

pub fn from_json_str<T: for<'de> Deserialize<'de>>(json: &str, label: &str) -> Result<T> {
    serde_json::from_str(json)
        .map_err(|e| binding_invalid_parameter(format!("invalid {label}: {e}")))
}

pub fn dispatch_params_from_json(json: &str) -> Result<Value> {
    serde_json::from_str(json)
        .map_err(|e| binding_invalid_parameter(format!("invalid dispatch params JSON: {e}")))
}

pub fn dispatch_operation_from_json(json: &str) -> Result<String> {
    serde_json::from_str::<DispatchOperationRequest>(json)
        .map(|request| request.op)
        .map_err(|e| binding_invalid_parameter(format!("invalid dispatch op: {e}")))
}

pub fn parse_mark_type(value: i32) -> MarkType {
    match value {
        1 => MarkType::Important,
        2 => MarkType::Todo,
        3 => MarkType::Done,
        _ => MarkType::Custom,
    }
}

pub fn conversation_type(value: &Value) -> Result<ConversationType> {
    let Some(raw) = value.get("conversationType") else {
        return Err(binding_invalid_parameter(
            "missing or invalid JSON field: conversationType",
        ));
    };
    if let Some(v) = raw.as_i64() {
        return i32::try_from(v)
            .map(ConversationType::from_proto_int)
            .map_err(|_| binding_invalid_parameter("invalid conversationType enum index"));
    }
    if let Some(v) = raw.as_str() {
        return Ok(ConversationType::from(v));
    }
    Err(binding_invalid_parameter(
        "missing or invalid JSON field: conversationType",
    ))
}

pub fn built_content_from_value(value: &Value) -> Result<BuiltContent> {
    let elem = Elem::from_json_value(value.clone())
        .map_err(|e| binding_invalid_parameter(format!("invalid built content: {e}")))?;
    let message_type = message_type_for_elem(&elem);
    Ok(BuiltContent::new(
        message_type,
        elem_to_message_content(&elem),
    ))
}

fn message_type_for_elem(elem: &Elem) -> MessageType {
    match elem {
        Elem::Text(_) => MessageType::Text,
        Elem::Image(_) => MessageType::Image,
        Elem::Video(_) => MessageType::Video,
        Elem::Audio(_) => MessageType::Audio,
        Elem::File(_) => MessageType::File,
        Elem::Location(_) => MessageType::Location,
        Elem::Card(_) => MessageType::Card,
        Elem::Sticker(_) => MessageType::Sticker,
        Elem::Emoji(_) => MessageType::Emoji,
        Elem::Quote(_) => MessageType::Quote,
        Elem::LinkCard(_) => MessageType::LinkCard,
        Elem::Forward(_) => MessageType::Forward,
        Elem::Thread(_) => MessageType::Thread,
        Elem::MiniProgram(_)
        | Elem::Vote(_)
        | Elem::Task(_)
        | Elem::Schedule(_)
        | Elem::Announcement(_) => MessageType::AppCard,
        Elem::RichText(_) => MessageType::RichText,
        Elem::ImageGroup(_) => MessageType::ImageGroup,
        Elem::System(_) => MessageType::System,
        Elem::Notification(_) => MessageType::Notification,
        Elem::Custom(_) => MessageType::Custom,
        Elem::Placeholder(_) => MessageType::Placeholder,
    }
}

/// 「保存到本机」请求：接受各端历史上用过的键名（`fileId`/`remoteFileId`、`fileName`/`displayFileName`、
/// `sourceUrl`/`sourceHttpUrl`），`downloadKey` 与文件名缺省时由核心推导。
#[cfg(not(target_arch = "wasm32"))]
pub fn build_user_download_request(
    params: Value,
) -> Result<flare_im_core_sdk::prelude::UserFileDownloadRequest> {
    // 第一个非空的键：宿主可能同时带上空的旧键名。
    let pick = |keys: &[&str]| {
        keys.iter().find_map(|key| {
            optional_string(&params, key)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
    };
    Ok(flare_im_core_sdk::prelude::UserFileDownloadRequest {
        download_key: pick(&["downloadKey"]).unwrap_or_default(),
        display_file_name: pick(&["fileName", "displayFileName", "name"]).unwrap_or_default(),
        source_path: pick(&["sourcePath", "localPath"]),
        source_http_url: pick(&["sourceUrl", "sourceHttpUrl"]),
        remote_file_id: pick(&["fileId", "remoteFileId", "mediaId"]),
        expires_in: optional_i32(&params, "expiresIn")?.unwrap_or(3600),
        on_progress: None,
    })
}

pub fn build_create_location_request(params: Value) -> Result<CreateLocationRequest> {
    Ok(CreateLocationRequest {
        conversation_id: conversation_id(&params)?,
        longitude: json_f64(&params, "longitude")?,
        latitude: json_f64(&params, "latitude")?,
        address: optional_string(&params, "address").unwrap_or_default(),
        title: optional_string(&params, "title").unwrap_or_default(),
        zoom: optional_u8_strict(&params, "zoom")?,
        snapshot_url: optional_string(&params, "snapshotUrl"),
        snapshot_local_path: optional_string(&params, "snapshotLocalPath"),
    })
}

pub fn build_create_sticker_request(params: Value) -> Result<CreateStickerRequest> {
    Ok(CreateStickerRequest {
        conversation_id: conversation_id(&params)?,
        sticker_id: json_string(&params, "stickerId")?,
        package_id: optional_string(&params, "packageId"),
        url: optional_string(&params, "url"),
        width: optional_i32(&params, "width")?,
        height: optional_i32(&params, "height")?,
        sticker_format: optional_string(&params, "stickerFormat"),
    })
}

#[derive(flare_im_core_sdk::serde::Deserialize)]
#[serde(crate = "flare_im_core_sdk::serde", rename_all = "camelCase")]
pub struct EditRichDocJson {
    message_id: String,
    doc_json: String,
    content_schema: String,
    plain_text: String,
    input_format: Option<String>,
    input_format_version: Option<i32>,
    source_payload: Option<HashMap<String, String>>,
    title: Option<String>,
    search_text: Option<String>,
    render_hints_json: Option<String>,
}

impl From<EditRichDocJson> for EditRichDocRequest {
    fn from(value: EditRichDocJson) -> Self {
        Self {
            message_id: value.message_id,
            doc_json: value.doc_json,
            content_schema: value.content_schema,
            plain_text: value.plain_text,
            input_format: value.input_format,
            input_format_version: value.input_format_version,
            source_payload: value.source_payload,
            title: value.title,
            search_text: value.search_text,
            render_hints_json: value.render_hints_json,
        }
    }
}

#[derive(flare_im_core_sdk::serde::Deserialize)]
#[serde(crate = "flare_im_core_sdk::serde", rename_all = "camelCase")]
pub struct CreateRichDocJson {
    conversation_id: String,
    doc_json: String,
    content_schema: String,
    plain_text: String,
    input_format: Option<String>,
    input_format_version: Option<i32>,
    source_payload: Option<HashMap<String, String>>,
    title: Option<String>,
    search_text: Option<String>,
    render_hints_json: Option<String>,
}

impl From<CreateRichDocJson> for CreateRichDocRequest {
    fn from(value: CreateRichDocJson) -> Self {
        Self {
            conversation_id: value.conversation_id,
            doc_json: value.doc_json,
            content_schema: value.content_schema,
            plain_text: value.plain_text,
            input_format: value.input_format,
            input_format_version: value.input_format_version,
            source_payload: value.source_payload,
            title: value.title,
            search_text: value.search_text,
            render_hints_json: value.render_hints_json,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_core_sdk::model::{
        CloseViewRequest, CloseViewResponse, ConversationTimelineSnapshot, Elem,
        HomeTimelineSnapshot, IMMessage, LoadNewerTimelineViewRequest,
        LoadOlderTimelineViewRequest, OpenConversationListViewRequest, OpenTimelineViewRequest,
        TimelineViewPageResponse, TimelineViewRequest, TimelineViewSnapshot, ViewOpenResponse,
        ViewUpdate,
    };
    use flare_im_core_sdk::serde;
    use flare_im_core_sdk::serde::de::DeserializeOwned;
    use flare_im_core_sdk::serde_json::json;

    fn assert_binding_wire_type<T>()
    where
        T: serde::Serialize + DeserializeOwned,
    {
    }

    #[test]
    fn core_sdk_binding_wire_types_implement_canonical_serde_contract() {
        assert_binding_wire_type::<IMMessage>();
        assert_binding_wire_type::<Elem>();
        assert_binding_wire_type::<OpenTimelineViewRequest>();
        assert_binding_wire_type::<LoadOlderTimelineViewRequest>();
        assert_binding_wire_type::<LoadNewerTimelineViewRequest>();
        assert_binding_wire_type::<TimelineViewRequest>();
        assert_binding_wire_type::<TimelineViewSnapshot>();
        assert_binding_wire_type::<OpenConversationListViewRequest>();
        assert_binding_wire_type::<CloseViewRequest>();
        assert_binding_wire_type::<HomeTimelineSnapshot>();
        assert_binding_wire_type::<ConversationTimelineSnapshot>();
        assert_binding_wire_type::<ViewOpenResponse>();
        assert_binding_wire_type::<TimelineViewPageResponse>();
        assert_binding_wire_type::<ViewUpdate>();
        assert_binding_wire_type::<CloseViewResponse>();
    }

    #[test]
    fn send_ack_response_uses_client_msg_id_when_ack_id_is_absent() {
        let response = json_send_ack(SendAck {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conversation-1".to_string(),
            ack_id: None,
            result: None,
        })
        .expect("send ack JSON should serialize");

        assert_eq!(response.payload["ackId"], "client-1");
        assert_ne!(response.payload["ackId"], serde_json::Value::Null);
    }

    #[test]
    fn send_ack_response_treats_empty_result_as_queued_success() {
        let response = json_send_ack(SendAck {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conversation-1".to_string(),
            ack_id: None,
            result: None,
        })
        .expect("queued send ack JSON should serialize");

        assert_eq!(response.payload["serverId"], "");
        assert_eq!(response.payload["seq"], 0);
        assert_eq!(response.payload["timestamp"], 0);
        assert_eq!(response.payload["success"], true);
        assert_eq!(response.payload["errorCode"], 0);
        assert_eq!(response.payload["errorMessage"], "");
    }

    #[test]
    fn send_ack_response_serializes_accepted_result() {
        let mut accepted = match send_ack::Result::Accepted(Default::default()) {
            send_ack::Result::Accepted(accepted) => accepted,
            send_ack::Result::Error(_) => unreachable!(),
        };
        accepted.server_msg_id = "server-1".to_string();
        accepted.conversation_seq = 42;
        accepted.server_time = 123_456;

        let response = json_send_ack(SendAck {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conversation-1".to_string(),
            ack_id: Some("ack-1".to_string()),
            result: Some(send_ack::Result::Accepted(accepted)),
        })
        .expect("accepted send ack JSON should serialize");

        assert_eq!(response.payload["serverId"], "server-1");
        assert_eq!(response.payload["seq"], 42);
        assert_eq!(response.payload["timestamp"], 123_456);
        assert_eq!(response.payload["ackId"], "ack-1");
        assert_eq!(response.payload["success"], true);
        assert_eq!(response.payload["errorCode"], 0);
        assert_eq!(response.payload["errorMessage"], "");
    }

    #[test]
    fn send_ack_response_serializes_error_result() {
        let mut error = match send_ack::Result::Error(Default::default()) {
            send_ack::Result::Accepted(_) => unreachable!(),
            send_ack::Result::Error(error) => error,
        };
        error.code = 13;
        error.message = "pre_send rejected: SOCIAL_BLOCKED".to_string();

        let response = json_send_ack(SendAck {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conversation-1".to_string(),
            ack_id: Some("ack-1".to_string()),
            result: Some(send_ack::Result::Error(error)),
        })
        .expect("error send ack JSON should serialize");

        assert_eq!(response.payload["serverId"], "");
        assert_eq!(response.payload["seq"], 0);
        assert_eq!(response.payload["timestamp"], 0);
        assert_eq!(response.payload["ackId"], "ack-1");
        assert_eq!(response.payload["success"], false);
        assert_eq!(response.payload["errorCode"], 13);
        assert_eq!(
            response.payload["errorMessage"],
            "pre_send rejected: SOCIAL_BLOCKED"
        );
    }

    #[test]
    fn heartbeat_app_state_accepts_string_wire_values() {
        let foreground = heartbeat_app_state(&json!({ "appState": "foreground" }), "appState")
            .expect("foreground should parse");
        let background = heartbeat_app_state(&json!({ "appState": "background" }), "appState")
            .expect("background should parse");

        assert_eq!(foreground, HeartbeatAppState::Foreground);
        assert_eq!(background, HeartbeatAppState::Background);
    }

    #[test]
    fn heartbeat_app_state_accepts_platform_enum_indices() {
        let foreground = heartbeat_app_state(&json!({ "appState": 0 }), "appState")
            .expect("foreground index should parse");
        let background = heartbeat_app_state(&json!({ "appState": 1 }), "appState")
            .expect("background index should parse");

        assert_eq!(foreground, HeartbeatAppState::Foreground);
        assert_eq!(background, HeartbeatAppState::Background);
    }

    #[test]
    fn heartbeat_app_state_rejects_unknown_values() {
        assert!(heartbeat_app_state(&json!({ "appState": "inactive" }), "appState").is_err());
        assert!(heartbeat_app_state(&json!({ "appState": 2 }), "appState").is_err());
        assert!(heartbeat_app_state(&json!({ "appState": true }), "appState").is_err());
    }

    #[test]
    fn json_u64_rejects_negative_numbers() {
        let input = json!({ "readSeq": -1 });

        assert!(json_u64(&input, "readSeq").is_err());
    }

    #[test]
    fn json_u64_accepts_valid_unsigned_numbers() {
        let input = json!({ "readSeq": 42 });

        assert_eq!(json_u64(&input, "readSeq").unwrap(), 42);
    }

    #[test]
    fn json_i32_rejects_out_of_range_numbers() {
        let too_large = json!({ "markType": i64::from(i32::MAX) + 1 });
        let too_small = json!({ "markType": i64::from(i32::MIN) - 1 });

        assert!(json_i32(&too_large, "markType").is_err());
        assert!(json_i32(&too_small, "markType").is_err());
    }

    #[test]
    fn json_i32_accepts_valid_signed_numbers() {
        let input = json!({ "markType": -1 });

        assert_eq!(json_i32(&input, "markType").unwrap(), -1);
    }

    #[test]
    fn conversation_type_rejects_out_of_range_enum_index() {
        let input = json!({ "conversationType": i64::from(i32::MAX) + 1 });

        assert!(conversation_type(&input).is_err());
    }

    #[test]
    fn build_create_location_request_rejects_out_of_range_zoom() {
        let input = json!({
            "conversationId": "conv-1",
            "longitude": 120.0,
            "latitude": 30.0,
            "zoom": 256,
        });

        assert!(build_create_location_request(input).is_err());
    }

    #[test]
    fn build_create_sticker_request_rejects_out_of_range_dimensions() {
        let input = json!({
            "conversationId": "conv-1",
            "stickerId": "sticker-1",
            "width": i64::from(i32::MAX) + 1,
        });

        assert!(build_create_sticker_request(input).is_err());
    }

    #[test]
    fn optional_u32_rejects_invalid_present_value() {
        let input = json!({ "limit": -1 });

        assert!(optional_u32(&input, "limit").is_err());
    }

    #[test]
    fn optional_bool_rejects_invalid_present_value() {
        let input = json!({ "merge": "false" });

        assert!(optional_bool(&input, "merge").is_err());
    }

    #[test]
    fn image_group_images_rejects_unknown_format() {
        let input = json!({
            "payload": {
                "images": [{
                    "imageId": "img-1",
                    "format": 9999
                }]
            }
        });

        assert!(image_group_images(&input).is_err());
    }

    #[test]
    fn image_group_images_rejects_negative_dimensions_and_size() {
        for image in [
            json!({ "imageId": "img-1", "size": -1 }),
            json!({ "imageId": "img-1", "width": -1 }),
            json!({ "imageId": "img-1", "height": -1 }),
        ] {
            let input = json!({ "payload": { "images": [image] } });
            assert!(image_group_images(&input).is_err());
        }
    }

    #[test]
    fn image_group_images_accepts_valid_image_metadata() {
        let input = json!({
            "payload": {
                "images": [{
                    "imageId": "img-1",
                    "size": 123,
                    "width": 640,
                    "height": 480,
                    "format": ImageFormat::Png as i32,
                    "animated": false
                }]
            }
        });

        let images = image_group_images(&input).unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].image_id, "img-1");
        assert_eq!(images[0].format, ImageFormat::Png as i32);
    }
}
