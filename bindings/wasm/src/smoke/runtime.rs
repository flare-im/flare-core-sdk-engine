// Temporary in-memory browser runtime used only for Web SDK smoke tests.
//
// This module intentionally lives behind `lib.rs` so the wasm binding entry
// stays thin. Do not add durable IM behavior here. Add shared behavior under
// `flare-core-sdk-engine/src`, then route the wasm binding to that core facade.

use flare_im_core_sdk::content::message_elem::{
    CustomElem, Elem, EmojiElem, MentionElem, StickerElem, TextElem,
};
use flare_im_core_sdk::model::conversation::{Conversation, ConversationType};
use flare_im_core_sdk::model::message::{IMMessage, ReactionEntry};
use flare_im_core_sdk::prelude::{
    ConnectionPhase, ConnectionStatus, LinkQuality, SdkConfigOverlay,
};
use flare_im_core_sdk::spi::{
    extract_conversation_type as extract_cid_conversation_type, generate_group_conversation_id,
    generate_single_chat_conversation_id, generate_system_conversation_id,
};
use serde::Serialize;
use serde_json::{Value, json};
use wasm_bindgen::prelude::*;

use super::web_model::{
    content_text, conversation_to_json, conversations_to_json, message_to_json, messages_to_json,
};
use flare_im_core_sdk_bindings_runtime::{message_build_catalog, normalize_operation};
use flare_proto::common::MentionType;

const CONTRACT_VERSION: &str = "flare-im-ffi/v2";

#[derive(Debug, Default, Clone)]
struct SmokeInitState {
    overlay: SdkConfigOverlay,
}

#[wasm_bindgen]
pub struct FlareImWasmRuntime {
    initialized: bool,
    connected: bool,
    current_user_id: Option<String>,
    init: Option<SmokeInitState>,
    conversations: Vec<Conversation>,
    messages: Vec<IMMessage>,
    event_subscription_ids: Vec<u64>,
    next_subscription_id: u64,
    next_seq: u64,
    next_message_id: u64,
}

#[wasm_bindgen(js_name = createWasmRuntime)]
pub fn create_wasm_runtime() -> FlareImWasmRuntime {
    FlareImWasmRuntime::new()
}

#[wasm_bindgen]
impl FlareImWasmRuntime {
    #[wasm_bindgen(constructor)]
    pub fn new() -> FlareImWasmRuntime {
        FlareImWasmRuntime {
            initialized: false,
            connected: false,
            current_user_id: None,
            init: None,
            conversations: Vec::new(),
            messages: Vec::new(),
            event_subscription_ids: Vec::new(),
            next_subscription_id: 1,
            next_seq: 1,
            next_message_id: 1,
        }
    }

    pub fn invoke(&mut self, operation: &str, request_json: &str) -> Result<JsValue, JsValue> {
        let request = parse_request(request_json)?;
        let result = self.invoke_json(operation, request)?;
        result
            .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
            .map_err(|error| js_error("wasm.serialize_failed", error))
    }

    pub fn dispose(&mut self) {
        self.initialized = false;
        self.connected = false;
        self.current_user_id = None;
        self.conversations.clear();
        self.messages.clear();
        self.event_subscription_ids.clear();
    }
}

impl FlareImWasmRuntime {
    fn invoke_json(&mut self, operation: &str, request: Value) -> Result<Value, JsValue> {
        let normalized = normalize_operation(operation, request);
        let operation = normalized.name.as_str();
        let request = normalized.request;
        match operation {
            "sdk.create" => Ok(json!({ "handle": 1 })),
            "sdk.init" => {
                self.init = Some(parse_init_request(request));
                self.initialized = true;
                Ok(Value::Null)
            }
            "sdk.uninit" => {
                self.initialized = false;
                self.connected = false;
                Ok(Value::Null)
            }
            "sdk.login" => self.login(request),
            "sdk.logout" | "connection.disconnect" => {
                self.connected = false;
                Ok(Value::Null)
            }
            "sdk.dispose" => {
                self.dispose();
                Ok(Value::Null)
            }
            "sdk.hard_reset" => {
                self.dispose();
                Ok(Value::Null)
            }
            "sdk.current_user_id" => {
                Ok(json!({ "userId": self.current_user_id.clone().unwrap_or_default() }))
            }
            "sdk.is_connected" | "sdk.session_active" => Ok(json!(self.connected)),
            "sdk.update_access_token" => Ok(Value::Null),
            "connection.get_status" => {
                let mut status = ConnectionStatus::idle(false);
                if self.connected {
                    status.phase = ConnectionPhase::Online;
                    status.quality = Some(LinkQuality::Good);
                }
                serde_json::to_value(status)
                    .map_err(|error| js_error("wasm.serialize_failed", error))
            }
            "conversation.list"
            | "conversation.list_raw"
            | "conversation.list_including_archived"
            | "conversation.list_paginated"
            | "conversation.list_by_query"
            | "conversation.get_multiple" => {
                Ok(json!({ "conversations": conversations_to_json(&self.conversations) }))
            }
            "conversation.total_unread" => Ok(json!({
                "totalUnread": flare_im_core_sdk::model::total_unread(&self.conversations)
            })),
            "conversation.bootstrap_home" => {
                let limit = request
                    .get("conversationLimit")
                    .and_then(Value::as_u64)
                    .unwrap_or(100) as usize;
                let total_unread = flare_im_core_sdk::model::total_unread(&self.conversations);
                let conversations = self
                    .conversations
                    .iter()
                    .take(limit)
                    .cloned()
                    .collect::<Vec<_>>();
                Ok(json!({
                    "conversations": conversations_to_json(&conversations),
                    "totalUnread": total_unread,
                    "syncState": "localReady"
                }))
            }
            "sync.bootstrap_startup_home" => {
                let limit = request
                    .get("conversationLimit")
                    .and_then(Value::as_u64)
                    .unwrap_or(100) as usize;
                let total_unread = flare_im_core_sdk::model::total_unread(&self.conversations);
                let conversations = self
                    .conversations
                    .iter()
                    .take(limit)
                    .cloned()
                    .collect::<Vec<_>>();
                Ok(json!({
                    "snapshot": {
                        "conversations": conversations_to_json(&conversations),
                        "totalUnread": total_unread,
                        "syncState": "localReady"
                    },
                    "servedFromLocal": !conversations.is_empty(),
                    "coldSyncPerformed": false,
                    "backgroundConvergenceStarted": true
                }))
            }
            "conversation.get" | "conversation.get_one" | "conversation.get_group_by_user_ids" => {
                let conversation = self.resolve_conversation(&request);
                Ok(conversation_to_json(&conversation))
            }
            "conversation.mark_read" => {
                if let Some(id) = conversation_id(&request) {
                    let read_seq = request
                        .get("readSeq")
                        .and_then(Value::as_u64)
                        .filter(|seq| *seq > 0)
                        .ok_or_else(|| JsValue::from_str("readSeq must be greater than 0"))?;
                    if let Some(conversation) = self
                        .conversations
                        .iter_mut()
                        .find(|item| item.conversation_id == id)
                    {
                        conversation.unread_count = 0;
                        conversation.last_read_seq = read_seq;
                    }
                }
                Ok(Value::Null)
            }
            "conversation.mark_unread" => {
                if let Some(id) = conversation_id(&request) {
                    if let Some(conversation) = self
                        .conversations
                        .iter_mut()
                        .find(|item| item.conversation_id == id)
                    {
                        conversation.unread_count = conversation.unread_count.max(1);
                        conversation.updated_at = now_ms();
                        conversation.version += 1;
                        return Ok(conversation_to_json(conversation));
                    }
                }
                Ok(Value::Null)
            }
            "conversation.set_pinned" | "conversation.set_muted" | "conversation.set_archived" => {
                self.update_conversation_flag(operation, &request);
                Ok(Value::Null)
            }
            "conversation.update_draft" => {
                if let Some(id) = conversation_id(&request) {
                    let draft = string_field(&request, "draft").unwrap_or_default();
                    if let Some(conversation) = self
                        .conversations
                        .iter_mut()
                        .find(|item| item.conversation_id == id)
                    {
                        conversation.draft = if draft.is_empty() { None } else { Some(draft) };
                        conversation.updated_at = now_ms();
                        conversation.version += 1;
                    }
                }
                Ok(Value::Null)
            }
            "conversation.clear_local_chat_history"
            | "conversation.clear_chat_history_for_self" => {
                if let Some(id) = conversation_id(&request) {
                    self.messages.retain(|item| item.conversation_id != id);
                    if let Some(conversation) = self
                        .conversations
                        .iter_mut()
                        .find(|item| item.conversation_id == id)
                    {
                        conversation.last_message_id = None;
                        conversation.last_message_at = None;
                        conversation.last_message_preview = None;
                        conversation.max_seq = 0;
                        conversation.unread_count = 0;
                    }
                }
                Ok(Value::Null)
            }
            "conversation.delete" => {
                if let Some(id) = conversation_id(&request) {
                    self.conversations.retain(|item| item.conversation_id != id);
                    self.messages.retain(|item| item.conversation_id != id);
                }
                Ok(Value::Null)
            }
            "message_builder.list_catalog" => Ok(json!({ "entries": message_build_catalog() })),
            "message.build" => Ok(message_to_json(&self.build_message(&request))),
            "message.create_text" => {
                let message = self.build_text_message(&request);
                Ok(message_to_json(&message))
            }
            "message.send" | "message.send_no_oss" => self.send_message(request),
            "message.list" => {
                let id = conversation_id(&request).unwrap_or_default();
                let mut messages = self
                    .messages
                    .iter()
                    .filter(|item| item.conversation_id == id)
                    .cloned()
                    .collect::<Vec<_>>();
                messages.sort_by(IMMessage::compare_for_timeline_asc);
                Ok(json!({ "messages": messages_to_json(&messages) }))
            }
            "message.dispatch" => self.dispatch_message(request),
            "message.typing" => Ok(json!({ "typing": true })),
            "sync.conversation" | "sync.messages" => {
                Ok(json!({ "synced": true, "syncedAt": now_ms() }))
            }
            "message.get" | "message.get_raw" => {
                let id = string_field(&request, "messageId").unwrap_or_default();
                Ok(self
                    .messages
                    .iter()
                    .find(|item| item.server_id == id || item.client_msg_id == id)
                    .map(message_to_json)
                    .unwrap_or(Value::Null))
            }
            "message.search" | "message.search_by_query" | "message.search_in_conversation" => {
                let keyword = string_field(&request, "keyword").unwrap_or_default();
                let messages = self
                    .messages
                    .iter()
                    .filter(|item| message_text(item).contains(&keyword))
                    .cloned()
                    .collect::<Vec<_>>();
                Ok(json!({ "messages": messages_to_json(&messages) }))
            }
            "message.recall"
            | "message.delete"
            | "message.delete_for_self"
            | "message.delete_for_everyone" => {
                if let Some(id) = string_field(&request, "messageId") {
                    self.messages
                        .retain(|item| item.server_id != id && item.client_msg_id != id);
                }
                Ok(Value::Null)
            }
            "message.edit_text_by_message_id"
            | "message.mark_read_and_burn"
            | "message.add_reaction"
            | "message.remove_reaction"
            | "message.pin"
            | "message.unpin"
            | "message.pin_by_message_id"
            | "message.unpin_by_message_id"
            | "message.mark"
            | "message.mark_with_color"
            | "message.unmark"
            | "message.mark_by_message_id"
            | "message.unmark_by_message_id"
            | "message.edit_rich_doc_by_message_id" => Ok(Value::Null),
            "presence.get" => {
                let user_id = string_field(&request, "userId").unwrap_or_default();
                Ok(json!({
                    "userId": user_id,
                    "status": "unknown",
                    "available": false,
                    "lastSeenAt": Value::Null
                }))
            }
            "presence.batch_get" => {
                let user_ids = request
                    .get("userIds")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let items = user_ids
                    .into_iter()
                    .filter_map(|value| value.as_str().map(str::to_string))
                    .map(|user_id| json!({ "userId": user_id, "status": "unknown", "available": false }))
                    .collect::<Vec<_>>();
                Ok(json!({ "items": items }))
            }
            "presence.subscribe" => Ok(json!({ "subscribed": true })),
            "capability.list" => Ok(json!({ "capabilities": [] })),
            "capability.dispatch" => Err(js_message(
                "capabilityUnavailable",
                operation,
                "WASM capability runtime is not connected to a host plugin adapter",
            )),
            "rich_doc_v2.normalize_from_markdown"
            | "rich_doc_v2.normalize_from_html"
            | "rich_doc_v2.normalize_from_doc_json" => {
                Ok(self.normalize_rich_doc(operation, &request))
            }
            op if op.starts_with("media.") => Err(js_message(
                "capabilityUnavailable",
                operation,
                "WASM media runtime requires a browser media host adapter",
            )),
            "event.subscribe" => {
                let id = self.next_subscription_id;
                self.next_subscription_id += 1;
                self.event_subscription_ids.push(id);
                Ok(json!({ "id": id }))
            }
            "event.unsubscribe" => {
                if let Some(id) = request.get("id").and_then(Value::as_u64) {
                    self.event_subscription_ids.retain(|item| *item != id);
                }
                Ok(Value::Null)
            }
            "event.unsubscribe_all" => {
                self.event_subscription_ids.clear();
                Ok(Value::Null)
            }
            "diagnostics.sdk_version" => {
                Ok(json!({ "version": env!("CARGO_PKG_VERSION"), "runtime": "wasm" }))
            }
            "diagnostics.ffi_contract_version" => Ok(json!({ "version": CONTRACT_VERSION })),
            "diagnostics.data_root" => {
                let overlay = self
                    .init
                    .as_ref()
                    .map(|s| s.overlay.clone())
                    .unwrap_or_default();
                Ok(json!({
                    "dataUrl": overlay.data_url.clone().unwrap_or_else(|| "memory://flare-im-core-sdk-wasm".to_string()),
                    "developmentMode": overlay
                        .access
                        .as_ref()
                        .is_some_and(|access| access.development_endpoints.is_some()),
                    "tenantId": overlay.tenant_id
                }))
            }
            "diagnostics.runtime_health" => {
                let (state, state_code) = if self.connected {
                    ("ready", 3)
                } else {
                    ("disconnected", 0)
                };
                Ok(json!({
                    "metricsEnabled": false,
                    "state": state,
                    "stateCode": state_code,
                    "sessionGeneration": 0,
                    "rawSubscriberDroppedTotal": 0,
                    "metricsJson": "{}"
                }))
            }
            _ => Err(js_message(
                "invalidParameter",
                operation,
                "Unsupported WASM SDK operation",
            )),
        }
    }

    fn login(&mut self, request: Value) -> Result<Value, JsValue> {
        if !self.initialized {
            self.initialized = true;
        }
        let user_id = string_field(&request, "userId")
            .ok_or_else(|| js_message("invalidParameter", "sdk.login", "userId is required"))?;
        self.current_user_id = Some(user_id.clone());
        self.connected = true;
        self.ensure_seed_conversation(&user_id);
        Ok(Value::Null)
    }

    fn current_user_or_default(&self) -> String {
        self.current_user_id
            .clone()
            .unwrap_or_else(|| "web-user".to_string())
    }

    fn generate_conversation_id(
        &self,
        conversation_type: ConversationType,
        source_id: &str,
    ) -> String {
        let me = self.current_user_or_default();
        match conversation_type {
            ConversationType::Single => generate_single_chat_conversation_id(&me, source_id),
            ConversationType::Group => generate_group_conversation_id(source_id),
            ConversationType::System => generate_system_conversation_id(source_id, None),
            _ => generate_single_chat_conversation_id(&me, source_id),
        }
    }

    fn channel_id_for_conversation(&self, conversation_id: &str) -> String {
        self.conversations
            .iter()
            .find(|item| item.conversation_id == conversation_id)
            .map(|item| item.channel_id.clone())
            .unwrap_or_default()
    }

    fn ensure_seed_conversation(&mut self, user_id: &str) {
        if !self.conversations.is_empty() {
            return;
        }
        if user_id == "hugo" {
            self.seed_hugo_workspace();
            return;
        }
        let peer = if user_id == "alice" { "bob" } else { "alice" };
        self.create_seed_conversation(
            ConversationType::Single,
            peer,
            format!("{} (WASM)", peer),
            false,
            0,
            None,
        );
    }

    fn seed_hugo_workspace(&mut self) {
        let support = self.create_seed_conversation(
            ConversationType::Single,
            "alice",
            "Alice 产品支持",
            true,
            2,
            Some("明天上午我会把 SDK 接入 checklist 发你。"),
        );
        let ops = self.create_seed_conversation(
            ConversationType::Single,
            "bob",
            "Bob 后端联调",
            false,
            1,
            Some("wasm runtime 的会话列表已经可以拉取。"),
        );
        let group = self.create_seed_conversation(
            ConversationType::Group,
            "core-sdk",
            "Core SDK 联调群",
            true,
            2,
            Some("消息发送 ack 需要回写 clientMsgId 和 serverId。"),
        );
        let system = self.create_seed_conversation(
            ConversationType::System,
            "release",
            "发布通知",
            false,
            0,
            Some("wasm 包已生成，可进入 Web 示例验证。"),
        );
        self.push_seed_message(
            &support,
            "alice",
            "明天上午我会把 SDK 接入 checklist 发你。",
            1,
            true,
        );
        self.push_seed_message(
            &support,
            "hugo",
            "收到，我先把 Web 端会话同步状态补齐。",
            2,
            false,
        );
        self.push_seed_message(
            &ops,
            "bob",
            "wasm runtime 的会话列表已经可以拉取。",
            1,
            true,
        );
        self.push_seed_message(
            &group,
            "alice",
            "请确认 Web、Android、Flutter 的字段契约一致。",
            1,
            true,
        );
        self.push_seed_message(
            &group,
            "bob",
            "消息发送 ack 需要回写 clientMsgId 和 serverId。",
            2,
            true,
        );
        self.push_seed_message(
            &system,
            "system",
            "wasm 包已生成，可进入 Web 示例验证。",
            1,
            false,
        );
    }

    fn create_seed_conversation(
        &mut self,
        conversation_type: ConversationType,
        channel_id: &str,
        display_name: impl Into<String>,
        pinned: bool,
        unread_count: u32,
        preview: Option<&str>,
    ) -> String {
        let id = self.generate_conversation_id(conversation_type, channel_id);
        let now = now_ms();
        let max_seq = u64::from(unread_count);
        self.conversations.push(Conversation {
            conversation_id: id.clone(),
            conversation_type,
            business_type: conversation_type.as_str().to_string(),
            channel_id: channel_id.to_string(),
            members_count: if conversation_type == ConversationType::Group {
                8
            } else {
                2
            },
            display_name: display_name.into(),
            avatar_url: String::new(),
            last_message_id: preview.map(|_| format!("seed-{}", self.next_message_id)),
            last_sender_id: None,
            last_message_at: preview.map(|_| now),
            last_message_preview: preview.map(ToString::to_string),
            last_sender_nickname: String::new(),
            last_sender_avatar_url: String::new(),
            unread_count,
            last_read_seq: max_seq.saturating_sub(u64::from(unread_count)),
            peer_read_seq: 0,
            max_seq,
            visible_after_seq: 0,
            is_pinned: pinned,
            is_muted: false,
            is_archived: false,
            version: 1,
            updated_at: now,
            created_at: now,
            participant_version: 1,
            draft: None,
            mention_count: 0,
            mention_me: false,
            ..Default::default()
        });
        id
    }

    fn push_seed_message(
        &mut self,
        conversation_id: &str,
        sender_id: &str,
        text: &str,
        seq: u64,
        unread: bool,
    ) {
        let now = now_ms() - (10_000 - seq * 1_000);
        let server_id = format!("seed-{seq}-{}", self.next_message_id);
        self.next_message_id += 1;
        let conversation_type = self.conversation_type_for(conversation_id);
        let channel_id = self.channel_id_for_conversation(conversation_id);
        self.messages.push(IMMessage {
            server_id: server_id.clone(),
            client_msg_id: format!("seed-local-{seq}"),
            conversation_id: conversation_id.to_string(),
            conversation_type: conversation_type.to_proto_int(),
            channel_id,
            sender_id: sender_id.to_string(),
            source: if sender_id == self.current_user_id.as_deref().unwrap_or("") {
                1
            } else {
                2
            },
            conversation_seq: seq,
            created_at: now,
            client_created_at: now,
            message_type: 0,
            content: Some(text_elem(text)),
            encoded_content: Vec::new(),
            text_preview: text.to_string(),
            sender_name: sender_id.to_string(),
            sender_avatar: String::new(),
            sender_display_name: sender_id.to_string(),
            status: 2,
            is_read: !unread,
            is_recalled: false,
            is_edited: false,
            retention_policy: None,
            retention_state: None,
            mention_users: Vec::new(),
            mention_all: false,
            offline_push_info: None,
            reply_to: None,
            quote_preview: None,
            attributes: Default::default(),
            extensions: Default::default(),
            reactions: Vec::new(),
            version: 1,
            updated_at: now,
            local_state: Default::default(),
        });
        if let Some(conversation) = self
            .conversations
            .iter_mut()
            .find(|item| item.conversation_id == conversation_id)
        {
            conversation.last_message_id = Some(server_id);
            conversation.last_sender_id = Some(sender_id.to_string());
            conversation.last_message_at = Some(now);
            conversation.last_message_preview = Some(text.to_string());
            conversation.last_sender_nickname = sender_id.to_string();
            conversation.max_seq = conversation.max_seq.max(seq);
            conversation.updated_at = now;
        }
        self.next_seq = self.next_seq.max(seq + 1);
    }

    fn resolve_conversation(&mut self, request: &Value) -> Conversation {
        if let Some(existing_id) = conversation_id(request) {
            if let Some(conversation) = self
                .conversations
                .iter()
                .find(|item| item.conversation_id == existing_id)
            {
                return conversation.clone();
            }
            let conversation_type = extract_cid_conversation_type(&existing_id)
                .map(conversation_type_from_cid)
                .unwrap_or_else(|| conversation_type_from_request(request));
            let channel_id = source_id_field(request)
                .or_else(|| string_field(request, "channelId"))
                .unwrap_or_default();
            let display_name = if channel_id.is_empty() {
                existing_id.clone()
            } else {
                channel_id.clone()
            };
            return self.insert_conversation(
                existing_id,
                conversation_type,
                channel_id,
                display_name,
            );
        }

        if let Some(source_id) = source_id_field(request) {
            let conversation_type = conversation_type_from_request(request);
            let id = self.generate_conversation_id(conversation_type, &source_id);
            if let Some(conversation) = self
                .conversations
                .iter()
                .find(|item| item.conversation_id == id)
            {
                return conversation.clone();
            }
            return self.insert_conversation(id, conversation_type, source_id.clone(), source_id);
        }

        if let Some(conversation) = self.conversations.first() {
            return conversation.clone();
        }

        let peer = "alice";
        let conversation_type = ConversationType::Single;
        let id = self.generate_conversation_id(conversation_type, peer);
        self.insert_conversation(id, conversation_type, peer.to_string(), peer.to_string())
    }

    fn insert_conversation(
        &mut self,
        conversation_id: String,
        conversation_type: ConversationType,
        channel_id: String,
        display_name: String,
    ) -> Conversation {
        let now = now_ms();
        let members_count = if conversation_type == ConversationType::Group {
            8
        } else {
            2
        };
        let conversation = Conversation {
            conversation_id,
            conversation_type,
            business_type: conversation_type.as_str().to_string(),
            channel_id,
            members_count,
            display_name,
            avatar_url: String::new(),
            last_message_id: None,
            last_sender_id: None,
            last_message_at: None,
            last_message_preview: None,
            last_sender_nickname: String::new(),
            last_sender_avatar_url: String::new(),
            unread_count: 0,
            last_read_seq: 0,
            peer_read_seq: 0,
            max_seq: 0,
            visible_after_seq: 0,
            is_pinned: false,
            is_muted: false,
            is_archived: false,
            version: 1,
            updated_at: now,
            created_at: now,
            participant_version: 1,
            draft: None,
            mention_count: 0,
            mention_me: false,
            ..Default::default()
        };
        self.conversations.push(conversation.clone());
        conversation
    }

    fn update_conversation_flag(&mut self, operation: &str, request: &Value) {
        let Some(id) = conversation_id(request) else {
            return;
        };
        let value = bool_field(request, "pinned")
            .or_else(|| bool_field(request, "muted"))
            .or_else(|| bool_field(request, "archived"))
            .unwrap_or(false);
        if let Some(conversation) = self
            .conversations
            .iter_mut()
            .find(|item| item.conversation_id == id)
        {
            match operation {
                "conversation.set_pinned" => conversation.is_pinned = value,
                "conversation.set_muted" => conversation.is_muted = value,
                "conversation.set_archived" => conversation.is_archived = value,
                _ => {}
            }
        }
    }

    fn build_message(&mut self, request: &Value) -> IMMessage {
        let op = string_field(request, "op").unwrap_or_else(|| "create_text".to_string());
        match op.as_str() {
            "create_text" => self.build_text_message(request),
            "create_emoji" => self.build_content_message(
                request,
                8,
                Elem::Emoji(EmojiElem {
                    emoji: string_field(request, "emoji").unwrap_or_else(|| "🙂".to_string()),
                    description: String::new(),
                    extra: Default::default(),
                }),
            ),
            "create_sticker" => self.build_content_message(
                request,
                7,
                Elem::Sticker(StickerElem {
                    sticker_id: string_field(request, "stickerId")
                        .unwrap_or_else(|| "001".to_string()),
                    package_id: string_field(request, "packageId")
                        .unwrap_or_else(|| "default".to_string()),
                    url: String::new(),
                    width: 0,
                    height: 0,
                    format: String::new(),
                    extra: Default::default(),
                }),
            ),
            _ => self.build_content_message(
                request,
                22,
                Elem::Custom(CustomElem {
                    r#type: op,
                    payload: serde_json::to_vec(request).unwrap_or_default(),
                    description: String::new(),
                    metadata: Default::default(),
                }),
            ),
        }
    }

    fn build_text_message(&mut self, request: &Value) -> IMMessage {
        let text = string_field(request, "text")
            .or_else(|| string_field(request, "body"))
            .unwrap_or_default();
        let mention_users = string_vec_field(request, "mentionUsers");
        let mention_all = bool_field(request, "mentionAll").unwrap_or(false);
        let mut message = self.build_content_message(
            request,
            0,
            text_elem_with_mentions(&text, &mention_users, mention_all),
        );
        message.mention_users = mention_users;
        message.mention_all = mention_all;
        message
    }

    fn build_content_message(
        &mut self,
        request: &Value,
        message_type: i32,
        content: Elem,
    ) -> IMMessage {
        let conversation_id = conversation_id(request).unwrap_or_else(|| {
            self.conversations
                .first()
                .map(|item| item.conversation_id.clone())
                .unwrap_or_else(|| self.generate_conversation_id(ConversationType::Single, "alice"))
        });
        let sender_id = self
            .current_user_id
            .clone()
            .unwrap_or_else(|| "web-user".to_string());
        let now = now_ms();
        let client_msg_id = format!("wasm-local-{}", self.next_message_id);
        self.next_message_id += 1;
        let conversation_type = self
            .conversations
            .iter()
            .find(|item| item.conversation_id == conversation_id)
            .map(|item| item.conversation_type.to_proto_int())
            .unwrap_or(ConversationType::Single.to_proto_int());
        let text_preview = content_text(Some(&content));
        IMMessage {
            server_id: String::new(),
            client_msg_id,
            conversation_id: conversation_id.clone(),
            conversation_type,
            channel_id: string_field(request, "channelId")
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| self.channel_id_for_conversation(&conversation_id)),
            sender_id: sender_id.clone(),
            source: 1,
            conversation_seq: 0,
            created_at: now,
            client_created_at: now,
            message_type,
            content: Some(content),
            encoded_content: Vec::new(),
            text_preview,
            sender_name: sender_id.clone(),
            sender_avatar: String::new(),
            sender_display_name: sender_id,
            status: 1,
            is_read: false,
            is_recalled: false,
            is_edited: false,
            retention_policy: None,
            retention_state: None,
            mention_users: Vec::new(),
            mention_all: false,
            offline_push_info: None,
            reply_to: None,
            quote_preview: None,
            attributes: Default::default(),
            extensions: Default::default(),
            reactions: Vec::new(),
            version: 1,
            updated_at: now,
            local_state: Default::default(),
        }
    }

    fn send_message(&mut self, request: Value) -> Result<Value, JsValue> {
        let mut message = self.message_from_request(request.get("message").unwrap_or(&request))?;
        let seq = self
            .messages
            .iter()
            .filter(|item| item.conversation_id == message.conversation_id)
            .map(|item| item.conversation_seq)
            .max()
            .unwrap_or(0)
            + 1;
        self.next_seq = self.next_seq.max(seq + 1);
        let now = now_ms();
        message.conversation_seq = seq;
        message.created_at = now;
        message.updated_at = now;
        if message.server_id.is_empty() {
            message.server_id = format!("wasm-server-{seq}");
        }
        message.status = 2;
        self.upsert_conversation_from_message(&message);
        self.messages.push(message.clone());
        Ok(json!({
            "serverId": message.server_id,
            "serverMsgId": message.server_id,
            "clientMsgId": message.client_msg_id,
            "conversationId": message.conversation_id,
            "conversationSeq": message.conversation_seq,
            "createdAt": message.created_at
        }))
    }

    fn dispatch_message(&mut self, request: Value) -> Result<Value, JsValue> {
        let op = string_field(&request, "op").unwrap_or_default();
        let params = request
            .get("params")
            .cloned()
            .unwrap_or_else(|| request.clone());
        let id = string_field(&params, "messageId")
            .or_else(|| string_field(&params, "clientMsgId"))
            .unwrap_or_default();
        let now = now_ms();
        if op.contains("delete") {
            self.messages
                .retain(|item| item.server_id != id && item.client_msg_id != id);
            return Ok(json!({ "deleted": true, "messageId": id }));
        }
        if let Some(message) = self
            .messages
            .iter_mut()
            .find(|item| item.server_id == id || item.client_msg_id == id)
        {
            if op.contains("edit") {
                let text = string_field(&params, "text").unwrap_or_default();
                message.content = Some(text_elem(&text));
                message.is_edited = true;
                message.updated_at = now;
                return Ok(json!({ "edited": true, "messageId": id }));
            }
            if op.contains("pin") {
                message
                    .attributes
                    .insert("pinned".to_string(), "true".to_string());
                message.updated_at = now;
                return Ok(json!({ "pinned": true, "messageId": id }));
            }
            if op.contains("reaction") {
                let emoji = string_field(&params, "emoji").unwrap_or_else(|| "👍".to_string());
                message.reactions.push(ReactionEntry {
                    emoji,
                    user_ids: vec![self.current_user_id.clone().unwrap_or_default()],
                    count: 1,
                });
                message.updated_at = now;
                return Ok(json!({ "reacted": true, "messageId": id }));
            }
        }
        Ok(json!({ "handled": true, "op": op, "messageId": id }))
    }

    fn normalize_rich_doc(&self, operation: &str, request: &Value) -> Value {
        let raw = string_field(request, "markdown")
            .or_else(|| string_field(request, "html"))
            .or_else(|| string_field(request, "docJson"))
            .unwrap_or_else(|| request.to_string());
        json!({
            "operation": operation,
            "doc": {
                "type": "richDocV2",
                "text": raw,
                "blocks": []
            },
            "plainText": raw
        })
    }

    fn message_from_request(&mut self, value: &Value) -> Result<IMMessage, JsValue> {
        let conversation_id = string_field(value, "conversationId").ok_or_else(|| {
            js_message(
                "invalidParameter",
                "message.send",
                "message.conversationId is required",
            )
        })?;
        let sender_id = string_field(value, "senderId")
            .or_else(|| self.current_user_id.clone())
            .unwrap_or_else(|| "web-user".to_string());
        let message_type = value
            .get("messageType")
            .and_then(Value::as_i64)
            .unwrap_or(0) as i32;
        let now = now_ms();
        let content = parse_web_content(value.get("content"), message_type);
        let text_preview = content_text(content.as_ref());
        let conversation_seq = u64_field_any(value, &["conversationSeq"]).unwrap_or_default();
        let created_at = u64_field_any(value, &["createdAt"]).unwrap_or(now);
        let client_created_at = u64_field_any(value, &["clientCreatedAt"]).unwrap_or(created_at);
        let mut message = IMMessage {
            server_id: string_field(value, "serverId").unwrap_or_default(),
            client_msg_id: string_field(value, "clientMsgId").unwrap_or_else(|| {
                let id = format!("wasm-local-{}", self.next_message_id);
                self.next_message_id += 1;
                id
            }),
            conversation_id: conversation_id.clone(),
            conversation_type: value
                .get("conversationType")
                .and_then(Value::as_i64)
                .map(|value| value as i32)
                .unwrap_or_else(|| self.conversation_type_for(&conversation_id).to_proto_int()),
            channel_id: string_field(value, "channelId")
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| self.channel_id_for_conversation(&conversation_id)),
            sender_id: sender_id.clone(),
            source: value.get("source").and_then(Value::as_i64).unwrap_or(1) as i32,
            conversation_seq,
            created_at,
            client_created_at,
            message_type,
            content,
            encoded_content: Vec::new(),
            text_preview,
            sender_name: string_field(value, "senderName").unwrap_or_else(|| sender_id.clone()),
            sender_avatar: string_field(value, "senderAvatar").unwrap_or_default(),
            sender_display_name: string_field(value, "senderDisplayName")
                .unwrap_or_else(|| sender_id.clone()),
            reply_to: None,
            quote_preview: None,
            status: value.get("status").and_then(Value::as_i64).unwrap_or(1) as i32,
            is_read: value
                .get("isRead")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            is_recalled: value
                .get("isRecalled")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            is_edited: value
                .get("isEdited")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            retention_policy: None,
            retention_state: None,
            mention_users: Vec::new(),
            mention_all: false,
            offline_push_info: None,
            attributes: Default::default(),
            extensions: Default::default(),
            reactions: Vec::new(),
            version: value.get("version").and_then(Value::as_u64).unwrap_or(1),
            updated_at: value
                .get("updatedAt")
                .and_then(Value::as_u64)
                .unwrap_or(now),
            local_state: Default::default(),
        };
        if message.content.is_none() {
            message.content = Some(text_elem(""));
        }
        Ok(message)
    }

    fn conversation_type_for(&self, conversation_id: &str) -> ConversationType {
        self.conversations
            .iter()
            .find(|item| item.conversation_id == conversation_id)
            .map(|item| item.conversation_type)
            .or_else(|| {
                extract_cid_conversation_type(conversation_id).map(conversation_type_from_cid)
            })
            .unwrap_or(ConversationType::Single)
    }

    fn upsert_conversation_from_message(&mut self, message: &IMMessage) {
        let preview = message_text(message);
        let now = message.created_at;
        if let Some(conversation) = self
            .conversations
            .iter_mut()
            .find(|item| item.conversation_id == message.conversation_id)
        {
            conversation.last_message_id = Some(message.server_id.clone());
            conversation.last_sender_id = Some(message.sender_id.clone());
            conversation.last_message_at = Some(now);
            conversation.last_message_preview = Some(preview);
            conversation.last_sender_nickname = message.sender_display_name.clone();
            conversation.max_seq = message.conversation_seq;
            conversation.updated_at = now;
            conversation.version += 1;
            return;
        }
        let mut conversation = self.resolve_conversation(&json!({
            "conversationId": message.conversation_id,
            "channelId": message.channel_id,
            "conversationType": message.conversation_type
        }));
        conversation.last_message_id = Some(message.server_id.clone());
        conversation.last_sender_id = Some(message.sender_id.clone());
        conversation.last_message_at = Some(now);
        conversation.last_message_preview = Some(preview);
        conversation.max_seq = message.conversation_seq;
        conversation.updated_at = now;
        if let Some(stored) = self
            .conversations
            .iter_mut()
            .find(|item| item.conversation_id == conversation.conversation_id)
        {
            *stored = conversation;
        }
    }
}

fn parse_request(request_json: &str) -> Result<Value, JsValue> {
    if request_json.trim().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_str(request_json).map_err(|error| js_error("wasm.invalid_json", error))
}

fn parse_init_request(value: Value) -> SmokeInitState {
    if value.get("sdkConfig").is_some() || value.get("environment").is_some() {
        let overlay = value
            .get("sdkConfig")
            .cloned()
            .and_then(|v| serde_json::from_value::<SdkConfigOverlay>(v).ok())
            .unwrap_or_default();
        return SmokeInitState { overlay };
    }
    SmokeInitState {
        overlay: serde_json::from_value(value).unwrap_or_default(),
    }
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn u64_field_any(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_u64))
}

fn source_id_field(value: &Value) -> Option<String> {
    string_field(value, "sourceId")
}

fn conversation_type_from_request(value: &Value) -> ConversationType {
    let raw = value.get("conversationType");
    if let Some(raw) = raw {
        if let Some(v) = raw.as_i64() {
            return ConversationType::from_proto_int(v as i32);
        }
        if let Some(v) = raw.as_str() {
            return ConversationType::from(v);
        }
    }
    ConversationType::Single
}

fn conversation_type_from_cid(
    conversation_type: flare_im_core_sdk::spi::CidConversationType,
) -> ConversationType {
    ConversationType::from_proto_int(conversation_type as i32)
}

fn bool_field(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool)
}

fn string_vec_field(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn conversation_id(value: &Value) -> Option<String> {
    string_field(value, "conversationId").or_else(|| {
        value
            .get("message")
            .and_then(|message| conversation_id(message))
    })
}

fn message_text(message: &IMMessage) -> String {
    content_text(message.content.as_ref())
}

fn parse_web_content(value: Option<&Value>, message_type: i32) -> Option<Elem> {
    let content = value?;
    let data = content.get("data").unwrap_or(content);
    let content_type = content
        .get("contentType")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let content_type_index = content
        .get("contentType")
        .and_then(Value::as_i64)
        .unwrap_or(i64::from(message_type));
    if content_type == "emoji" || content_type_index == 8 || message_type == 8 {
        return Some(Elem::Emoji(EmojiElem {
            emoji: string_field(data, "emoji").unwrap_or_default(),
            description: string_field(data, "description").unwrap_or_default(),
            extra: Default::default(),
        }));
    }
    if content_type == "sticker" || content_type_index == 7 || message_type == 7 {
        return Some(Elem::Sticker(StickerElem {
            sticker_id: string_field(data, "stickerId").unwrap_or_default(),
            package_id: string_field(data, "packageId").unwrap_or_default(),
            url: string_field(data, "url").unwrap_or_default(),
            width: data.get("width").and_then(Value::as_i64).unwrap_or(0) as i32,
            height: data.get("height").and_then(Value::as_i64).unwrap_or(0) as i32,
            format: string_field(data, "format").unwrap_or_default(),
            extra: Default::default(),
        }));
    }
    Some(text_elem(&string_field(data, "text").unwrap_or_default()))
}

fn text_elem(text: &str) -> Elem {
    text_elem_with_mentions(text, &[], false)
}

fn text_elem_with_mentions(text: &str, mention_users: &[String], mention_all: bool) -> Elem {
    let mut mentions = Vec::<MentionElem>::new();
    if mention_all {
        let length = text.chars().count().min(i32::MAX as usize) as i32;
        mentions.push(MentionElem {
            r#type: MentionType::All as i32,
            user_id: String::new(),
            user_ids: Vec::new(),
            role_id: String::new(),
            start: 0,
            length: length.max(1),
        });
    }
    for user_id in mention_users {
        let token = format!("@{}", user_id.trim());
        let Some(start_byte) = text.find(&token) else {
            continue;
        };
        mentions.push(MentionElem {
            r#type: MentionType::User as i32,
            user_id: user_id.trim().to_string(),
            user_ids: Vec::new(),
            role_id: String::new(),
            start: text[..start_byte].chars().count().min(i32::MAX as usize) as i32,
            length: token.chars().count().min(i32::MAX as usize) as i32,
        });
    }
    Elem::Text(TextElem {
        text: text.to_string(),
        mentions,
    })
}

fn now_ms() -> u64 {
    js_sys::Date::now() as u64
}

fn js_error(code: &str, error: impl std::fmt::Display) -> JsValue {
    js_message(code, "wasm.invoke", &error.to_string())
}

fn js_message(code: &str, operation: &str, message: &str) -> JsValue {
    let error = js_sys::Error::new(message);
    let _ = js_sys::Reflect::set(&error, &JsValue::from_str("code"), &JsValue::from_str(code));
    let _ = js_sys::Reflect::set(
        &error,
        &JsValue::from_str("operation"),
        &JsValue::from_str(operation),
    );
    error.into()
}
