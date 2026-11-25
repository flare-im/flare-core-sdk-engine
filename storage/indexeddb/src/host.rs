//! JS IndexedDB host bridge for WASM production storage.

use std::cell::RefCell;
use std::rc::Rc;

use flare_im_core_sdk::model::Conversation;
use flare_im_core_sdk::model::IMMessage;
use flare_im_core_sdk::storage::{PendingSendVo, StoredMessage, StoredPendingSend};
use flare_im_core_sdk::{ErrorCode, FlareError, Result};
use js_sys::Function;
use serde::{Deserialize, Serialize};
use serde_wasm_bindgen::Serializer;
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;

thread_local! {
    static STORAGE_HOST: RefCell<Option<Rc<StorageHost>>> = const { RefCell::new(None) };
}

/// The JS storage host's methods, read by name and bound to the host object when it
/// is installed, so a host never depends on the order of its callbacks.
#[derive(Clone)]
struct StorageHost {
    load_snapshot: Function,
    read_messages: Option<Function>,
    commit_projection: Option<Function>,
    commit_outgoing: Function,
    save_message: Function,
    save_messages: Option<Function>,
    save_conversation: Function,
    save_cursor: Function,
    save_pending_send: Function,
    delete_message: Function,
    delete_conversation: Function,
    delete_pending_send: Function,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct SnapshotPayload {
    #[serde(default)]
    pub messages: Vec<StoredMessage>,
    #[serde(default, rename = "previewRepairs")]
    pub preview_repairs: Vec<StoredMessage>,
    #[serde(default)]
    pub conversations: Vec<Conversation>,
    #[serde(default)]
    pub cursors: std::collections::HashMap<String, String>,
    #[serde(default, rename = "pendingSends")]
    pub pending_sends: Vec<StoredPendingSend>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistMessageArgs<'a> {
    user_id: &'a str,
    message: &'a StoredMessage,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistMessagesArgs<'a> {
    user_id: &'a str,
    messages: &'a [StoredMessage],
    previews: Vec<Conversation>,
    redacted_ids: Vec<(String, String)>,
}

#[derive(Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum MessageRead {
    Ids {
        ids: Vec<String>,
    },
    Timeline {
        conversation_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        before_seq: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        after_seq: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        oldest_seq: Option<f64>,
    },
    All,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadMessagesArgs<'a> {
    user_id: &'a str,
    #[serde(flatten)]
    query: MessageRead,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistConversationArgs<'a> {
    user_id: &'a str,
    conversation: &'a Conversation,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistCursorArgs<'a> {
    user_id: &'a str,
    key: &'a str,
    value: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistPendingSendArgs<'a> {
    user_id: &'a str,
    entry: &'a StoredPendingSend,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeleteIdArgs<'a> {
    user_id: &'a str,
    id: &'a str,
}

async fn call_host_void(function: &Function, payload: &JsValue) -> Result<()> {
    let promise = function
        .call1(&JsValue::NULL, payload)
        .map_err(|e| FlareError::system(format!("storage host call failed: {e:?}")))?;
    if promise.is_instance_of::<js_sys::Promise>() {
        JsFuture::from(js_sys::Promise::from(promise))
            .await
            .map_err(|e| FlareError::system(format!("storage host promise failed: {e:?}")))?;
    }
    Ok(())
}

async fn call_host_string(function: &Function, payload: &JsValue) -> Result<String> {
    let promise = function
        .call1(&JsValue::NULL, payload)
        .map_err(|e| FlareError::system(format!("storage host call failed: {e:?}")))?;
    let value = if promise.is_instance_of::<js_sys::Promise>() {
        JsFuture::from(js_sys::Promise::from(promise))
            .await
            .map_err(|e| FlareError::system(format!("storage host promise failed: {e:?}")))?
    } else {
        promise
    };
    if value.is_null() || value.is_undefined() {
        return Ok(String::new());
    }
    value
        .as_string()
        .ok_or_else(|| FlareError::system("storage host returned non-string payload"))
}

fn to_host_value<T: Serialize>(payload: &T, operation: &str) -> Result<JsValue> {
    payload
        .serialize(&Serializer::json_compatible())
        .map_err(|e| FlareError::system(format!("encode {operation} payload failed: {e}")))
}

impl StorageHost {
    fn from_js(host: &JsValue) -> Result<Self> {
        Ok(Self {
            load_snapshot: host_method(host, "loadSnapshot")?,
            read_messages: optional_host_method(host, "readMessages"),
            commit_projection: optional_host_method(host, "commitProjection"),
            commit_outgoing: host_method(host, "commitOutgoing")?,
            save_message: host_method(host, "saveMessage")?,
            save_messages: optional_host_method(host, "saveMessages"),
            save_conversation: host_method(host, "saveConversation")?,
            save_cursor: host_method(host, "saveCursor")?,
            save_pending_send: host_method(host, "savePendingSend")?,
            delete_message: host_method(host, "deleteMessage")?,
            delete_conversation: host_method(host, "deleteConversation")?,
            delete_pending_send: host_method(host, "deletePendingSend")?,
        })
    }
}

/// `host[name]` bound to `host`; an invalid parameter when the host lacks it.
fn host_method(host: &JsValue, name: &str) -> Result<Function> {
    optional_host_method(host, name).ok_or_else(|| {
        FlareError::localized(
            ErrorCode::InvalidParameter,
            format!("storage host has no {name} method"),
        )
    })
}

fn optional_host_method(host: &JsValue, name: &str) -> Option<Function> {
    js_sys::Reflect::get(host, &JsValue::from_str(name))
        .ok()
        .and_then(|value| value.dyn_into::<Function>().ok())
        .map(|method| method.bind0(host))
}

/// Installs the JS storage host: one object with the methods `loadSnapshot`,
/// `saveMessage`, `saveConversation`, `saveCursor`, `savePendingSend`,
/// `deleteMessage`, `deleteConversation` and `deletePendingSend`. A host missing
/// any of them is refused and the installed one stays. The optional
/// `saveMessages` method resolves after committing a batch of message rows.
/// When `readMessages` is provided, `loadSnapshot` receives `metadataOnly: true`
/// and the host reads message windows or IDs separately on demand.
pub fn set_storage_host(host: &JsValue) -> Result<()> {
    let host = StorageHost::from_js(host)?;
    STORAGE_HOST.with(|slot| {
        *slot.borrow_mut() = Some(Rc::new(host));
    });
    Ok(())
}

pub fn clear_storage_host() {
    STORAGE_HOST.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

pub fn storage_host_configured() -> bool {
    STORAGE_HOST.with(|slot| slot.borrow().is_some())
}

pub(crate) fn loads_messages_on_demand() -> bool {
    STORAGE_HOST.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|host| host.read_messages.is_some())
    })
}

pub async fn load_snapshot(user_id: &str) -> Result<SnapshotPayload> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(host) = host else {
        return Ok(SnapshotPayload::default());
    };
    let payload = to_host_value(
        &serde_json::json!({ "userId": user_id, "metadataOnly": host.read_messages.is_some() }),
        "storage load",
    )?;
    let raw = call_host_string(&host.load_snapshot, &payload).await?;
    if raw.trim().is_empty() {
        return Ok(SnapshotPayload::default());
    }
    flare_im_core_sdk::json::from_str(&raw)
        .map_err(|e| FlareError::system(format!("decode storage snapshot failed: {e}")))
}

pub(crate) async fn read_messages(user_id: &str, query: MessageRead) -> Result<Vec<IMMessage>> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(read) = host.as_ref().and_then(|host| host.read_messages.as_ref()) else {
        return Ok(Vec::new());
    };
    let payload = to_host_value(&ReadMessagesArgs { user_id, query }, "read messages")?;
    let raw = call_host_string(read, &payload).await?;
    let rows: Vec<StoredMessage> = flare_im_core_sdk::json::from_str(&raw)
        .map_err(|e| FlareError::system(format!("decode stored message rows failed: {e}")))?;
    rows.into_iter().map(TryInto::try_into).collect()
}

pub async fn persist_message(user_id: &str, message: &IMMessage) -> Result<()> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(host) = host else {
        return Ok(());
    };
    let stored = StoredMessage::from(message.clone());
    let payload = to_host_value(
        &PersistMessageArgs {
            user_id,
            message: &stored,
        },
        "persist message",
    )?;
    call_host_void(&host.save_message, &payload).await
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OutgoingArgs<'a> {
    user_id: &'a str,
    message: StoredMessage,
    conversation: Conversation,
    entry: Option<StoredPendingSend>,
}

pub(crate) async fn commit_outgoing(
    user_id: &str,
    message: IMMessage,
    conversation: Conversation,
    entry: Option<PendingSendVo>,
) -> Result<()> {
    let host = STORAGE_HOST
        .with(|slot| slot.borrow().clone())
        .ok_or_else(|| {
            flare_im_core_sdk::FlareError::system("storage host removed during outgoing commit")
        })?;
    let payload = to_host_value(
        &OutgoingArgs {
            user_id,
            message: message.into(),
            conversation,
            entry: entry.map(Into::into),
        },
        "commit outgoing send",
    )?;
    call_host_void(&host.commit_outgoing, &payload).await
}

pub(crate) async fn persist_messages(
    user_id: &str,
    messages: Vec<IMMessage>,
    previews: Vec<Conversation>,
) -> Result<Option<Vec<Conversation>>> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(host) = host else {
        return Ok(None);
    };
    if let Some(save_messages) = &host.save_messages {
        let redacted: Vec<_> = messages
            .iter()
            .filter(|m| {
                m.retention_state
                    .as_ref()
                    .is_some_and(flare_im_core_sdk::storage::retention_hides_content)
            })
            .map(|m| {
                (
                    m.conversation_id.clone(),
                    if m.server_id.is_empty() {
                        m.client_msg_id.clone()
                    } else {
                        m.server_id.clone()
                    },
                )
            })
            .collect();
        let stored: Vec<_> = messages.into_iter().map(StoredMessage::from).collect();
        let payload = to_host_value(
            &PersistMessagesArgs {
                user_id,
                messages: &stored,
                previews,
                redacted_ids: redacted,
            },
            "persist messages",
        )?;
        let committed = call_host_string(save_messages, &payload).await?;
        if committed.is_empty() {
            return Ok(None);
        }
        return serde_json::from_str(&committed)
            .map(Some)
            .map_err(|e| FlareError::system(format!("decode staged conversations: {e}")));
    }
    for message in &messages {
        persist_message(user_id, message).await?;
    }
    Ok(None)
}

#[derive(Deserialize)]
pub(crate) struct ProjectionResult {
    pub committed: bool,
    pub conversation: Option<Conversation>,
}

pub(crate) async fn commit_projection(
    user_id: &str,
    expected: &Conversation,
    conversation: &Conversation,
) -> Result<Option<ProjectionResult>> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Request<'a> {
        user_id: &'a str,
        expected: &'a Conversation,
        conversation: &'a Conversation,
    }
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(method) = host.as_ref().and_then(|h| h.commit_projection.as_ref()) else {
        return Ok(None);
    };
    let request = to_host_value(
        &Request {
            user_id,
            expected,
            conversation,
        },
        "compare conversation projection",
    )?;
    let response = call_host_string(method, &request).await?;
    serde_json::from_str(&response)
        .map(Some)
        .map_err(|e| FlareError::system(format!("invalid projection reply: {e}")))
}

pub async fn persist_conversation(user_id: &str, conversation: &Conversation) -> Result<()> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(host) = host else {
        return Ok(());
    };
    let payload = to_host_value(
        &PersistConversationArgs {
            user_id,
            conversation,
        },
        "persist conversation",
    )?;
    call_host_void(&host.save_conversation, &payload).await
}

pub async fn persist_cursor(user_id: &str, key: &str, value: &str) -> Result<()> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(host) = host else {
        return Ok(());
    };
    let payload = to_host_value(
        &PersistCursorArgs {
            user_id,
            key,
            value,
        },
        "persist cursor",
    )?;
    call_host_void(&host.save_cursor, &payload).await
}

pub async fn persist_pending_send(user_id: &str, entry: &PendingSendVo) -> Result<()> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(host) = host else {
        return Ok(());
    };
    let stored = StoredPendingSend::from(entry.clone());
    let payload = to_host_value(
        &PersistPendingSendArgs {
            user_id,
            entry: &stored,
        },
        "persist pending send",
    )?;
    call_host_void(&host.save_pending_send, &payload).await
}

pub async fn delete_message(user_id: &str, message_id: &str) -> Result<()> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(host) = host else {
        return Ok(());
    };
    let payload = to_host_value(
        &DeleteIdArgs {
            user_id,
            id: message_id,
        },
        "delete message",
    )?;
    call_host_void(&host.delete_message, &payload).await
}

pub async fn delete_conversation(user_id: &str, conversation_id: &str) -> Result<()> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(host) = host else {
        return Ok(());
    };
    let payload = to_host_value(
        &DeleteIdArgs {
            user_id,
            id: conversation_id,
        },
        "delete conversation",
    )?;
    call_host_void(&host.delete_conversation, &payload).await
}

pub async fn delete_pending_send(user_id: &str, client_msg_id: &str) -> Result<()> {
    let host = STORAGE_HOST.with(|slot| slot.borrow().clone());
    let Some(host) = host else {
        return Ok(());
    };
    let payload = to_host_value(
        &DeleteIdArgs {
            user_id,
            id: client_msg_id,
        },
        "delete pending send",
    )?;
    call_host_void(&host.delete_pending_send, &payload).await
}
