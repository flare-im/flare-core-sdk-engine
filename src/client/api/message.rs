//! 消息 Facade — 直接编排消息写侧/读侧 usecase；发送前用 [`super::MessageBuildApi`] 构建 [`crate::model::message::IMMessage`] 再调用 [`MessageApi::send`]。
//!
//! **上层约定**：所有对上层暴露的 `message_id` 均为 **client_msg_id**；server_msg_id 仅用于内部与服务端交互。

use std::sync::Arc;

use super::media::UploadProgressCallback;
use crate::application::usecases::{
    MessageMutationUseCase, MessageSendUseCase, MessageViewAssembler,
};
use crate::content::{BuiltContent, ContentBuilder};
use crate::kernel::CurrentUserIdStore;
use crate::model::MessageSearchQuery;
use crate::model::message::{IMMessage, MarkType, SendAck};
use crate::shared::error::{ErrorCode, FlareError, Result};

#[derive(Clone, Debug)]
pub struct EditRichDocRequest {
    pub message_id: String,
    pub doc_json: String,
    pub content_schema: String,
    pub plain_text: String,
    pub input_format: Option<String>,
    pub input_format_version: Option<i32>,
    pub source_payload: Option<std::collections::HashMap<String, String>>,
    pub title: Option<String>,
    pub search_text: Option<String>,
    pub render_hints_json: Option<String>,
}

/// 消息命令与查询入口（直接委托 application usecases）。
#[derive(Clone)]
pub struct MessageApi {
    send_use_case: Arc<MessageSendUseCase>,
    mutation_use_case: Arc<MessageMutationUseCase>,
    view_assembler: Arc<MessageViewAssembler>,
    session_guard: super::session_guard::SessionGuard,
    /// Plugin card rules that need the authorized catalog (forwardability).
    plugin_cards: Option<Arc<super::CapabilityApi>>,
    /// Group read receipts: summaries on view messages and the member lists.
    read_receipts: Option<Arc<crate::application::services::ReadSummaryService>>,
}

impl MessageApi {
    pub(crate) fn for_session(mut self, watch: crate::client::im_client::SessionEndWatch) -> Self {
        self.session_guard.bind(watch);
        self
    }

    pub fn new(
        send_use_case: Arc<MessageSendUseCase>,
        mutation_use_case: Arc<MessageMutationUseCase>,
        view_assembler: Arc<MessageViewAssembler>,
        current_user_id: CurrentUserIdStore,
    ) -> Self {
        Self {
            send_use_case,
            mutation_use_case,
            view_assembler,
            session_guard: super::session_guard::SessionGuard::new(current_user_id, "message"),
            plugin_cards: None,
            read_receipts: None,
        }
    }

    /// Enables [`Self::read_receipt_members`] (the group read receipt sheet).
    pub(crate) fn with_read_receipts(
        mut self,
        read_receipts: Arc<crate::application::services::ReadSummaryService>,
    ) -> Self {
        self.read_receipts = Some(read_receipts);
        self
    }

    /// One page of who read, or has not yet read, one of the viewer's own
    /// messages in a group: `{conversationId, serverMsgId, state: "read" |
    /// "unread", cursor?, limit?}` (limit 1..=200, default 50) in,
    /// `{conversationId, serverMsgId, available, summary?, userIds, nextCursor?,
    /// hasMore}` out, user ids ascending. `available: false` (no list) for a
    /// conversation that is not a group or is above the large-group threshold.
    /// Core answers another sender's message the same as one that is not in the
    /// conversation, recalled or deleted (`NOT_FOUND`), so the answer does not
    /// tell whether such a message exists. A summary in the page refreshes the
    /// message's `receipt.readSummary` in open views.
    pub async fn read_receipt_members(
        &self,
        request: serde_json::Value,
    ) -> Result<serde_json::Value> {
        self.session_guard.run(async {
            let read_receipts = self.read_receipts.as_ref().ok_or_else(|| {
                FlareError::localized(
                    ErrorCode::OperationNotSupported,
                    "sdk.read_receipt.unavailable",
                )
            })?;
            let request: crate::application::services::read_summaries::ReadReceiptMembersRequest =
                crate::shared::util::json::from_value(request).map_err(|_| {
                    FlareError::localized(
                        ErrorCode::InvalidParameter,
                        "sdk.read_receipt.invalid_request",
                    )
                })?;
            let page = read_receipts.members(request).await?;
            serde_json::to_value(page).map_err(|error| FlareError::system(error.to_string()))
        }).await
    }

    /// Lets [`Self::action_availability`] offer "forward" on a plugin card only
    /// when its contribution declares `forwardable`. Without it, plugin cards
    /// never offer forward.
    pub fn with_plugin_cards(mut self, capability: Arc<super::CapabilityApi>) -> Self {
        self.plugin_cards = Some(capability);
        self
    }

    pub async fn current_user_id(&self) -> Result<String> {
        self.session_guard
            .run(self.send_use_case.current_user_id())
            .await
    }

    /// 默认发送：若检测到本地媒体路径则先上传 OSS，再发送消息。
    ///
    /// 有可靠队列时，返回表示消息与待发队列已在本机提交（`result` 为空），不代表服务端
    /// 已收到；服务端结果用 [`Self::await_confirmation`] 等待，或订阅 `SendAck` / `SendFailed`。
    pub async fn send(&self, message: IMMessage) -> Result<SendAck> {
        self.session_guard
            .run(async { self.send_use_case.send_with_media(message, None).await })
            .await
    }
    /// Native compositions can fence the build/send boundary against account
    /// switches. The API holds the session's own current-user store; a retired
    /// API cannot become the next session's sender.
    pub async fn send_bound(
        &self,
        message: IMMessage,
        expected_user_id: Option<&str>,
    ) -> Result<SendAck> {
        if expected_user_id.is_some_and(|expected| expected.is_empty()) {
            return Err(FlareError::localized(
                ErrorCode::NotConnected,
                "sdk.message.session_changed",
            ));
        }
        if let Some(expected) = expected_user_id {
            let actual = self.current_user_id().await?;
            if actual != expected {
                return Err(FlareError::localized(
                    ErrorCode::NotConnected,
                    "sdk.message.session_changed",
                ));
            }
        }
        self.send(message).await
    }

    /// 带上传进度回调的发送：检测到本地媒体路径时，回调会上报上传阶段与字节进度。
    pub async fn send_with_media_progress(
        &self,
        message: IMMessage,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<SendAck> {
        self.session_guard
            .run(async {
                self.send_use_case
                    .send_with_media(message, self.session_guard.guard_callback(on_progress))
                    .await
            })
            .await
    }

    /// 原样发送：不做 OSS 上传预处理。
    pub async fn send_no_oss(&self, message: IMMessage) -> Result<SendAck> {
        self.session_guard
            .run(async { self.send_use_case.send(message).await })
            .await
    }

    /// 撤回消息。message_id 为 client_msg_id。
    pub async fn recall(&self, message_id: &str) -> Result<()> {
        self.session_guard
            .run(async { self.mutation_use_case.recall(message_id).await })
            .await
    }

    /// 编辑消息内容。message_id 为 client_msg_id。
    pub async fn edit(
        &self,
        conversation_id: &str,
        message_id: &str,
        new_content: Vec<u8>,
    ) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .edit(conversation_id, message_id, new_content)
                    .await
            })
            .await
    }

    /// 编辑消息（已构建内容）。message_id 为 client_msg_id。
    pub async fn edit_content(
        &self,
        conversation_id: &str,
        message_id: &str,
        content: BuiltContent,
    ) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .edit(conversation_id, message_id, content.encode())
                    .await
            })
            .await
    }

    /// 按 message_id（client_msg_id）编辑为纯文本。
    pub async fn edit_text_by_message_id(&self, message_id: &str, text: &str) -> Result<()> {
        self.session_guard
            .run(async {
                let (conversation_id, _) = self
                    .mutation_use_case
                    .resolve_message_id(message_id)
                    .await?;
                self.edit_content(
                    &conversation_id,
                    message_id,
                    crate::content::ContentBuilder::text(text).build(),
                )
                .await
            })
            .await
    }

    /// 按 message_id 编辑为 Rich Doc（与 [`ContentBuilder::try_rich_doc`] 规则一致）。
    pub async fn edit_rich_doc_by_message_id(&self, request: EditRichDocRequest) -> Result<()> {
        self.session_guard
            .run(async {
                let (conversation_id, _) = self
                    .mutation_use_case
                    .resolve_message_id(&request.message_id)
                    .await?;
                let mut cb = ContentBuilder::try_rich_doc(
                    request.doc_json,
                    request.content_schema,
                    request.plain_text,
                )
                .map_err(|e| {
                    FlareError::localized(
                        ErrorCode::InvalidParameter,
                        format!("sdk.message.rich_doc_v2.invalid: {e}"),
                    )
                })?;
                if let Some(f) = request.input_format {
                    cb = cb.rich_text_input_format(f);
                }
                if let Some(v) = request.input_format_version {
                    cb = cb.rich_text_input_format_version(v);
                }
                if let Some(map) = request.source_payload {
                    for (k, v) in map {
                        cb = cb.rich_text_source_payload_entry(k, v);
                    }
                }
                cb = cb.rich_text_title(request.title);
                cb = cb.rich_text_search_text(request.search_text);
                cb = cb.rich_text_render_hints_json(request.render_hints_json);
                let built = cb.build();
                self.edit_content(&conversation_id, &request.message_id, built)
                    .await
            })
            .await
    }

    /// 删除消息。message_id 为 client_msg_id。
    pub async fn delete(&self, message_id: &str) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .delete_for_self(message_id, None)
                    .await
            })
            .await
    }

    /// 仅删除自己可见（多端同步）。
    pub async fn delete_for_self(&self, message_id: &str, reason: Option<String>) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .delete_for_self(message_id, reason)
                    .await
            })
            .await
    }

    /// 删除所有人可见（仅发送者）。
    pub async fn delete_for_everyone(
        &self,
        message_id: &str,
        reason: Option<String>,
    ) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .delete_for_everyone(message_id, reason)
                    .await
            })
            .await
    }

    /// 标记单条消息已读并触发阅后即焚。message_id 为 client_msg_id 或 server_msg_id。
    pub async fn mark_read_and_burn(&self, message_id: &str) -> Result<()> {
        self.session_guard
            .run(async { self.mutation_use_case.mark_read_and_burn(message_id).await })
            .await
    }

    pub async fn typing(&self, conversation_id: &str, typing: bool) -> Result<()> {
        self.session_guard
            .run(async { self.mutation_use_case.typing(conversation_id, typing).await })
            .await
    }

    /// 按 message_id（client_msg_id）添加反应。
    pub async fn add_reaction(&self, message_id: &str, emoji: &str) -> Result<()> {
        self.session_guard
            .run(async { self.mutation_use_case.add_reaction(message_id, emoji).await })
            .await
    }

    /// 按 message_id（client_msg_id）移除反应。
    pub async fn remove_reaction(&self, message_id: &str, emoji: &str) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .remove_reaction(message_id, emoji)
                    .await
            })
            .await
    }

    /// 置顶消息。message_id 为 client_msg_id；scope 使用 core wire enum：0=会话可见，1=仅自己可见。
    pub async fn pin(&self, conversation_id: &str, message_id: &str, scope: i32) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .pin(conversation_id, message_id, scope)
                    .await
            })
            .await
    }

    /// 取消置顶。message_id 为 client_msg_id；scope 使用 core wire enum：0=会话可见，1=仅自己可见。
    pub async fn unpin(&self, conversation_id: &str, message_id: &str, scope: i32) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .unpin(conversation_id, message_id, scope)
                    .await
            })
            .await
    }

    /// 按 message_id（client_msg_id）置顶。
    pub async fn pin_by_message_id(&self, message_id: &str, scope: i32) -> Result<()> {
        self.session_guard
            .run(async {
                let (conversation_id, _) = self
                    .mutation_use_case
                    .resolve_message_id(message_id)
                    .await?;
                self.pin(&conversation_id, message_id, scope).await
            })
            .await
    }

    /// 按 message_id（client_msg_id）取消置顶。
    pub async fn unpin_by_message_id(&self, message_id: &str, scope: i32) -> Result<()> {
        self.session_guard
            .run(async {
                let (conversation_id, _) = self
                    .mutation_use_case
                    .resolve_message_id(message_id)
                    .await?;
                self.unpin(&conversation_id, message_id, scope).await
            })
            .await
    }

    /// 标记消息。message_id 为 client_msg_id。
    pub async fn mark(
        &self,
        conversation_id: &str,
        message_id: &str,
        mark_type: MarkType,
    ) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .mark(conversation_id, message_id, mark_type, "")
                    .await
            })
            .await
    }

    /// 带颜色的标记。message_id 为 client_msg_id。
    pub async fn mark_with_color(
        &self,
        conversation_id: &str,
        message_id: &str,
        mark_type: MarkType,
        color: &str,
    ) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .mark(conversation_id, message_id, mark_type, color)
                    .await
            })
            .await
    }

    /// 取消标记。message_id 为 client_msg_id。
    pub async fn unmark(
        &self,
        conversation_id: &str,
        message_id: &str,
        mark_type: MarkType,
    ) -> Result<()> {
        self.session_guard
            .run(async {
                self.mutation_use_case
                    .unmark(conversation_id, message_id, mark_type)
                    .await
            })
            .await
    }

    /// 按 message_id（client_msg_id）标记。
    pub async fn mark_by_message_id(
        &self,
        message_id: &str,
        mark_type: MarkType,
        color: &str,
    ) -> Result<()> {
        self.session_guard
            .run(async {
                let (conversation_id, _) = self
                    .mutation_use_case
                    .resolve_message_id(message_id)
                    .await?;
                self.mutation_use_case
                    .mark(&conversation_id, message_id, mark_type, color)
                    .await
            })
            .await
    }

    /// 按 message_id（client_msg_id）取消标记。
    pub async fn unmark_by_message_id(&self, message_id: &str, mark_type: MarkType) -> Result<()> {
        self.session_guard
            .run(async {
                let (conversation_id, _) = self
                    .mutation_use_case
                    .resolve_message_id(message_id)
                    .await?;
                self.mutation_use_case
                    .unmark(&conversation_id, message_id, mark_type)
                    .await
            })
            .await
    }

    /// 按 message_id（client_msg_id）查询单条消息。
    /// 一条消息此刻可用的动作（能不能撤回/编辑/复制/置顶……）。
    ///
    /// 规则在核心（`domain::message_actions`），各端只消费结果——
    /// 同一条消息在任何端都必须给出同一个答案。situational 的入参
    /// （多选模式、待发/失败、是否已置顶、是否在线）由调用方给，
    /// 因为那是此刻的 UI 状态，核心看不到。
    pub async fn action_availability(
        &self,
        message_id: &str,
        multi_select_mode: bool,
        is_pending: bool,
        is_failed: bool,
        is_pinned: bool,
        is_connected: bool,
    ) -> Result<serde_json::Value> {
        let Some(message) = self.get(message_id).await? else {
            return Ok(serde_json::json!({}));
        };
        // Plugin cards are bound to their conversation unless the catalog says otherwise.
        let plugin_forwardable = match (message.plugin_message(), &self.plugin_cards) {
            (Some(card), Some(plugin_cards)) => plugin_cards.plugin_card_forwardable(&card).await,
            _ => false,
        };
        let ctx = crate::domain::message_actions::MessageActionContext {
            current_user_id: self.current_user_id().await.unwrap_or_default(),
            is_connected,
            multi_select_mode,
            is_pending,
            is_failed,
            is_pinned,
            plugin_forwardable,
        };
        let availability =
            crate::domain::message_actions::message_action_availability(&message, &ctx);
        Ok(serde_json::to_value(availability).unwrap_or_else(|_| serde_json::json!({})))
    }

    pub async fn get(&self, message_id: &str) -> Result<Option<IMMessage>> {
        self.session_guard
            .run(async { self.view_assembler.get(message_id).await })
            .await
    }

    /// 等待 `send` 已在本机入队的消息得到服务端持久确认或终态失败。超时返回
    /// `OperationTimeout`，消息仍留在队列中继续重试。
    pub async fn await_confirmation(
        &self,
        request: crate::model::ConfirmationRequest,
    ) -> Result<crate::model::SendConfirmation> {
        let key = &request.key;
        if [&key.conversation_id, &key.sender_id, &key.client_msg_id]
            .iter()
            .any(|id| id.trim().is_empty())
            || request.timeout_ms == 0
        {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "incomplete confirmation request",
            ));
        }
        let timeout = std::time::Duration::from_millis(request.timeout_ms.min(600_000));
        self.session_guard
            .run(self.send_use_case.await_confirmation(key, timeout))
            .await
    }

    /// 按 message_id（client_msg_id）查询原始消息（不填充发送者资料）。
    pub async fn get_by_key(&self, key: crate::model::MessageKey) -> Result<Option<IMMessage>> {
        if [&key.conversation_id, &key.sender_id, &key.client_msg_id]
            .iter()
            .any(|id| id.trim().is_empty())
        {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "incomplete message key",
            ));
        }
        self.session_guard
            .run(self.view_assembler.get_by_key(&key))
            .await
    }

    pub async fn get_raw(&self, message_id: &str) -> Result<Option<IMMessage>> {
        self.session_guard
            .run(async { self.view_assembler.get_raw(message_id).await })
            .await
    }

    /// `before_seq == 0`：会话首屏（本地最新一页）；否则 `seq < before_seq` 分页更早消息。
    pub async fn list(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.session_guard
            .run(async {
                self.view_assembler
                    .list(conversation_id, before_seq, limit)
                    .await
            })
            .await
    }

    pub async fn search(&self, keyword: &str, limit: u32) -> Result<Vec<IMMessage>> {
        self.session_guard
            .run(async { self.view_assembler.search(keyword, limit).await })
            .await
    }

    /// 飞书式高级搜索：SDK 统一处理关键词、会话、发送人、时间、媒体类型等筛选。
    pub async fn search_by_query(&self, query: MessageSearchQuery) -> Result<Vec<IMMessage>> {
        self.session_guard
            .run(async { self.view_assembler.search_by_query(&query).await })
            .await
    }

    pub async fn search_in_conversation(
        &self,
        conversation_id: &str,
        keyword: &str,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        self.session_guard
            .run(async {
                self.view_assembler
                    .search_in_conversation(conversation_id, keyword, limit)
                    .await
            })
            .await
    }
}
