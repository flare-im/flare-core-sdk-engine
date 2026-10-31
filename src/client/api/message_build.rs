//! 消息构建 Facade — 所有 `create_*` 必须传入 `conversation_id`；当前用户与 channel 由 SDK/会话内部解析，产出 [`crate::model::message::IMMessage`]。
//! 与 [`super::MessageApi::send`] 配合：先构建再发送。实现委托 [`crate::application::MessageBuilderService`]。
//!
//! 发送前根据本地 [`Conversation`] 记录直接写入消息的 `channel_id` 与 `conversation_type`（映射为 proto 枚举），供 Orchestrator 构建推送目标。

use std::sync::Arc;

use crate::application::{
    BuildCardRequest, BuildLinkCardRequest, BuildLocationRequest, BuildMiniProgramRequest,
    BuildRichDocRequest, BuildScheduleRequest, BuildStickerRequest, MessageBuilderService,
};
use crate::content::BuiltContent;
use crate::domain::conversation::id as conversation_id;
use crate::domain::{ConversationIdentityService, ConversationStore, UserReader};
use crate::kernel::CurrentUserIdStore;
use crate::model::message::IMMessage;
use crate::shared::error::{ErrorCode, FlareError, Result};
use flare_proto::common::ImageInfo;

#[derive(Clone, Debug)]
pub struct CreateLocationRequest {
    pub conversation_id: String,
    pub longitude: f64,
    pub latitude: f64,
    pub address: String,
    pub title: String,
    pub zoom: Option<u8>,
    pub snapshot_url: Option<String>,
    pub snapshot_local_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CreateStickerRequest {
    pub conversation_id: String,
    pub sticker_id: String,
    pub package_id: Option<String>,
    pub url: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub sticker_format: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CreateRichDocRequest {
    pub conversation_id: String,
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

/// 是否需要随消息携带整张成员表(`group_member_ids`)。
///
/// ad-hoc 用户集合群(`ext["group_source"]=="user_ids"`)的成员集合**恒定**(conversation_id=hash(成员集),
/// 加减成员即是另一个 conversation_id),服务端首条消息即建群并永久持有成员。故仅在**尚未建群**
/// (`max_seq==0` —— 还没有任何带服务端 seq 的消息落地;乐观消息 seq=0 不计)时随消息带成员表用于建群;
/// 一旦建立(`max_seq>0`)即省略,使每条消息回到 O(1):避免 `group_member_ids` 整表膨胀穿 WAL/NATS/投递,
/// 并让 ingest 的 ensure 缓存生效(participants 仅发送者 → 命中缓存,跳过 O(成员) 的 CreateConversation)。
/// 命名/可变成员群不在此优化内,仍按原样携带。同发送者消息经可靠队列**有序**到达,首条(带成员表)必先于
/// 后续(省略)被服务端处理,且失败重试沿用首条原始载荷,故省略不会丢成员。
fn needs_member_roster(conv: &crate::model::Conversation) -> bool {
    let is_user_set_group = conv
        .ext
        .get("group_source")
        .map(|source| source == "user_ids")
        .unwrap_or(false);
    // 已建群的判据(任一即可):① 显式建群 RPC 成功(get_group_by_user_ids 置 group_server_established=1)——
    // 这样**首条消息**就不带成员表;② 已有带服务端 seq 的消息落地(max_seq>0)——RPC 失败兜底路径建群后亦不再带。
    let server_established = conv
        .ext
        .get("group_server_established")
        .map(|v| v == "1")
        .unwrap_or(false)
        || conv.max_seq > 0;
    !(is_user_set_group && server_established)
}

/// 插件卡片能不能转发：它在当前授权目录里的 message-content 贡献声明了 `forwardable`。
#[async_trait::async_trait]
pub(crate) trait ForwardableCards: Send + Sync {
    async fn forwardable(&self, card: &flare_plugin_protocol::ui::PluginMessage) -> bool;
}

#[async_trait::async_trait]
impl ForwardableCards for super::CapabilityApi {
    async fn forwardable(&self, card: &flare_plugin_protocol::ui::PluginMessage) -> bool {
        self.plugin_card_forwardable(card).await
    }
}

/// 多类型消息的构建入口（不负责发送）。
pub struct MessageBuildApi {
    current_user_id: CurrentUserIdStore,
    conversations: Arc<dyn ConversationStore>,
    /// 身份缓存：转发记录给源消息的发送者补名字。
    profiles: Arc<dyn UserReader>,
    /// 插件卡片的可转发判定（授权目录）；没有时插件卡片一律不能转发。
    plugin_cards: Option<Arc<dyn ForwardableCards>>,
}

impl MessageBuildApi {
    pub fn new(
        current_user_id: CurrentUserIdStore,
        conversations: Arc<dyn ConversationStore>,
        profiles: Arc<dyn UserReader>,
    ) -> Self {
        Self {
            current_user_id,
            conversations,
            profiles,
            plugin_cards: None,
        }
    }

    /// 让转发记录收下声明了 `forwardable` 的插件卡片（与菜单的「转发」同一份目录判定）；
    /// 不接的话插件卡片一律不能转发。
    pub fn with_plugin_cards(self, capability: Arc<super::CapabilityApi>) -> Self {
        self.with_forwardable_cards(capability)
    }

    pub(crate) fn with_forwardable_cards(mut self, cards: Arc<dyn ForwardableCards>) -> Self {
        self.plugin_cards = Some(cards);
        self
    }

    /// 转发的每条源消息都必须能转发（[`crate::domain::message_actions::message_forwardable`]，菜单的
    /// 「转发」读的是同一条规则）：有一条不能就整个拒掉，并按 id（服务端 id，没有时客户端 id，每条一次）
    /// 点名在 `refusedMessageIds` 里，不静默跳过——跳过的话，选了几条、记录里少几条，用户无从知道。
    async fn refuse_unforwardable(&self, sources: &[IMMessage]) -> Result<()> {
        let mut refused = Vec::new();
        for source in sources {
            let plugin_forwardable = match (source.plugin_message(), &self.plugin_cards) {
                (Some(card), Some(cards)) => cards.forwardable(&card).await,
                _ => false,
            };
            if !crate::domain::message_actions::message_forwardable(source, plugin_forwardable) {
                let server_id = source.server_id.trim();
                let id = if server_id.is_empty() {
                    source.client_msg_id.trim()
                } else {
                    server_id
                };
                if !refused.iter().any(|seen| seen == id) {
                    refused.push(id.to_owned());
                }
            }
        }
        if refused.is_empty() {
            return Ok(());
        }
        let refusal = FlareError::localized(
            ErrorCode::InvalidParameter,
            format!(
                "sdk.message.forward.not_forwardable: {}",
                refused.join(", ")
            ),
        );
        Err(crate::shared::error::refusing_messages(refusal, &refused))
    }

    /// 宿主交来的源消息缺发送者名时（没经过时间线读取的身份填充，或读取时缓存里还没有这个人），按 sender_id
    /// 查身份缓存补上，与时间线展示同一个名字来源。
    async fn name_forward_sources(&self, sources: &mut [IMMessage]) -> Result<()> {
        let mut missing: Vec<String> = sources
            .iter()
            .filter(|source| MessageBuilderService::forward_sender_name(source).is_none())
            .map(|source| source.sender_id.trim().to_owned())
            .filter(|id| !id.is_empty())
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        missing.sort();
        missing.dedup();
        let profiles = self.profiles.get_many(&missing).await?;
        for source in sources.iter_mut() {
            if MessageBuilderService::forward_sender_name(source).is_some() {
                continue;
            }
            // 人名解析链（别名、资料名）；没有名字就不写，展示端按无名渲染，不把 id 当名字。
            if let Some(profile) = profiles.get(source.sender_id.trim())
                && let Some(name) = crate::domain::resolve_person_name(
                    &source.sender_id,
                    &profile.name_candidates(),
                    None,
                )
                .text()
            {
                source.sender_display_name = name.to_owned();
            }
        }
        Ok(())
    }

    async fn current_sender_id(&self) -> Result<String> {
        let current_user_id = self.current_user_id.read().await.clone();
        if current_user_id.is_empty() {
            return Err(FlareError::localized(ErrorCode::NotConnected, "未连接"));
        }
        Ok(current_user_id)
    }

    /// 用本地会话行的 `channel_id`、`conversation_type` 覆盖消息对应字段（SDK 类型映射为 proto）。
    async fn apply_conversation_routing(
        &self,
        conversation_id: &str,
        mut msg: IMMessage,
    ) -> Result<IMMessage> {
        let Some(mut conv) = self.conversations.get(conversation_id).await? else {
            if conversation_id::is_single_chat_conversation(conversation_id) {
                return Err(FlareError::localized(
                    ErrorCode::InvalidParameter,
                    "单聊会话未在本地落库，请先调用 conversation.get_one(对端 user_id, Single)",
                ));
            }
            return Ok(msg);
        };

        if conv.conversation_type.is_single_chat_conversation() {
            let current_user_id = self.current_sender_id().await?;
            let peer_hint = conv
                .participants
                .iter()
                .chain(conv.member_preview.iter())
                .map(|p| p.user_id.trim())
                .find(|id| !id.is_empty() && *id != current_user_id.as_str())
                .map(ToOwned::to_owned);
            if ConversationIdentityService::repair_single_chat_channel(
                &mut conv,
                &current_user_id,
                peer_hint.as_deref(),
            ) {
                self.conversations.save_one(&conv).await?;
            }
        }

        msg.channel_id = conv.channel_id.clone();
        msg.conversation_type = conv.conversation_type.to_proto_int();
        if conv.conversation_type.is_group_chat_conversation() && needs_member_roster(&conv) {
            let participant_ids = conv
                .participants
                .iter()
                .map(|p| p.user_id.trim())
                .filter(|id| !id.is_empty())
                .collect::<Vec<_>>()
                .join(",");
            let member_ids = if participant_ids.is_empty() {
                conv.ext
                    .get("group_member_ids")
                    .cloned()
                    .unwrap_or_default()
            } else {
                participant_ids
            };
            if !member_ids.trim().is_empty() {
                msg.attributes
                    .entry("group_member_ids".to_string())
                    .or_insert(member_ids);
            }
        }

        if conversation_id::is_single_chat_conversation(conversation_id)
            && msg.channel_id.is_empty()
        {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "单聊缺少对方 user_id（channel_id）。请先调用 conversation.get_one(对端, Single) 以补全会话行",
            ));
        }

        Ok(msg)
    }

    pub async fn create_text(
        &self,
        conversation_id: &str,
        text: &str,
        mention_all: bool,
        mention_user_ids: &[String],
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        // 提及从**正文**解析（`@全员` / `@某人`）：同一段文字在任何端都必须产出
        // 同一条消息，所以规则只有核心这一份。入参是「显式追加」——
        // 比如 UI 的 @全员 开关或选人器已经确定的人，与解析结果取并集。
        let parsed = self
            .parse_text_mentions(conversation_id, text, &sender_id)
            .await;
        let mut merged: Vec<String> = mention_user_ids.to_vec();
        for id in parsed.user_ids {
            if !merged.iter().any(|existing| existing == &id) {
                merged.push(id);
            }
        }
        let msg = MessageBuilderService::build_text(
            conversation_id,
            &sender_id,
            text,
            None,
            mention_all || parsed.mention_all,
            &merged,
        )?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    /// 按会话成员名册解析正文里的提及。取不到会话时只解析 `@全员`
    /// （它不依赖名册），不因为名册缺失而整体失效。
    async fn parse_text_mentions(
        &self,
        conversation_id: &str,
        text: &str,
        sender_id: &str,
    ) -> crate::content::ParsedMentions {
        // 正文里没有 '@' 就什么都不用做 —— 绝大多数消息都是这样。
        //
        // 这里原本无条件读一次会话存储再组名册：在 web 上那是一次 IndexedDB 读，
        // 还要排进 WASM 的单槽 invoke 队列，实测把「点击→上屏」从 92ms 推到 601ms，
        // 而发送→上屏的预算是 **16ms**（下一帧）。热路径上不能有这种无条件 I/O。
        if !text.contains('@') {
            return crate::content::ParsedMentions::default();
        }

        let mut candidates: Vec<crate::content::MentionCandidate> = Vec::new();
        if let Ok(Some(conv)) = self.conversations.get(conversation_id).await {
            // participants 与 member_preview 同型，合并成一份名册；
            // 大群里 participants 可能被截断，member_preview 常是补充来源。
            for p in conv.participants.iter().chain(conv.member_preview.iter()) {
                let id = p.user_id.trim();
                // 自己不算被提及。
                if id.is_empty() || id == sender_id {
                    continue;
                }
                let nickname = p.nickname.trim();
                match candidates.iter_mut().find(|c| c.user_id == id) {
                    Some(existing) => {
                        if !nickname.is_empty()
                            && !existing.display_names.iter().any(|n| n == nickname)
                        {
                            existing.display_names.push(nickname.to_string());
                        }
                    }
                    None => candidates.push(crate::content::MentionCandidate {
                        user_id: id.to_string(),
                        display_names: if nickname.is_empty() {
                            Vec::new()
                        } else {
                            vec![nickname.to_string()]
                        },
                    }),
                }
            }
        }
        crate::content::parse_mentions(text, &candidates)
    }

    pub async fn create_quote(
        &self,
        conversation_id: &str,
        quoted_message_id: &str,
        text: &str,
        quoted_sender_id: Option<&str>,
        quoted_text_preview: Option<&str>,
        quoted_content: Option<BuiltContent>,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_quote(
            conversation_id,
            &sender_id,
            quoted_message_id,
            text,
            quoted_sender_id,
            quoted_text_preview,
            quoted_content,
        )?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_thread_reply(
        &self,
        conversation_id: &str,
        thread_id: &str,
        text: &str,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_thread_reply(
            conversation_id,
            &sender_id,
            thread_id,
            text,
        )?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    /// 源消息里有不能转发的（撤回或删除的、没声明 `forwardable` 的插件卡片）就整个拒绝：
    /// `INVALID_PARAMETER`，被拒的源消息 id 放在结构化的 `refusedMessageIds`
    /// （[`crate::refused_message_ids`]）；原因 `sdk.message.forward.not_forwardable: <id>, …` 只给人看。
    pub async fn create_forward(
        &self,
        conversation_id: &str,
        merge: bool,
        title: &str,
        mut sources: Vec<IMMessage>,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        self.refuse_unforwardable(&sources).await?;
        self.name_forward_sources(&mut sources).await?;
        let msg = MessageBuilderService::build_forward(
            conversation_id,
            &sender_id,
            merge,
            title,
            &sources,
        )?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_with_content(
        &self,
        conversation_id: &str,
        content: BuiltContent,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg =
            MessageBuilderService::build_with_content(conversation_id, &sender_id, content, None)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_image(&self, conversation_id: &str, image_id: &str) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_image(conversation_id, &sender_id, image_id, None)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_image_with_thumbnail(
        &self,
        conversation_id: &str,
        source_image_id: &str,
        thumbnail_image_id: &str,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_image_with_thumbnail(
            conversation_id,
            &sender_id,
            source_image_id,
            thumbnail_image_id,
            None,
        )?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_image_group(
        &self,
        conversation_id: &str,
        images: Vec<ImageInfo>,
        description: impl Into<String>,
        metadata: std::collections::HashMap<String, String>,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_image_group(
            conversation_id,
            &sender_id,
            images,
            description,
            metadata,
            None,
        )?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_video(&self, conversation_id: &str, video_id: &str) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_video(conversation_id, &sender_id, video_id, None)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_audio(&self, conversation_id: &str, audio_id: &str) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_audio(conversation_id, &sender_id, audio_id, None)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_file(&self, conversation_id: &str, file_id: &str) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_file(conversation_id, &sender_id, file_id, None)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_location(&self, request: CreateLocationRequest) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let conversation_id = request.conversation_id.clone();
        let msg = MessageBuilderService::build_location(BuildLocationRequest {
            conversation_id: request.conversation_id,
            sender_id,
            longitude: request.longitude,
            latitude: request.latitude,
            address: request.address,
            title: request.title,
            zoom: request.zoom,
            snapshot_url: request.snapshot_url,
            snapshot_local_path: request.snapshot_local_path,
            channel_id: None,
        })?;
        self.apply_conversation_routing(&conversation_id, msg).await
    }

    /// A card message. `attributes` become the card elem's `extra`
    /// (`CardContent.attributes`), such as a shared person's `referrerId`.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_card(
        &self,
        conversation_id: &str,
        id: &str,
        card_type: Option<&str>,
        title: Option<&str>,
        subtitle: Option<&str>,
        avatar: Option<&str>,
        attributes: Option<std::collections::HashMap<String, String>>,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let ct = match card_type {
            Some(s) if !s.is_empty() => s,
            _ => "user",
        };
        let msg = MessageBuilderService::build_card(BuildCardRequest {
            conversation_id: conversation_id.to_string(),
            sender_id,
            id: id.to_string(),
            card_type: ct.to_string(),
            title: title.unwrap_or("").to_string(),
            subtitle: subtitle.unwrap_or("").to_string(),
            avatar: avatar.unwrap_or("").to_string(),
            channel_id: None,
            attributes,
        })?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_sticker(&self, request: CreateStickerRequest) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let conversation_id = request.conversation_id.clone();
        let msg = MessageBuilderService::build_sticker_with(BuildStickerRequest {
            conversation_id: request.conversation_id,
            sender_id,
            sticker_id: request.sticker_id,
            channel_id: None,
            package_id: request.package_id,
            url: request.url,
            width: request.width.unwrap_or(0),
            height: request.height.unwrap_or(0),
            sticker_format: request.sticker_format,
        })?;
        self.apply_conversation_routing(&conversation_id, msg).await
    }

    pub async fn create_emoji(&self, conversation_id: &str, emoji: &str) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_emoji(conversation_id, &sender_id, emoji, None)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_link_card(
        &self,
        conversation_id: &str,
        url: &str,
        title: Option<&str>,
        description: Option<&str>,
        thumbnail_url: Option<&str>,
        site_name: Option<&str>,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_link_card(BuildLinkCardRequest {
            conversation_id: conversation_id.to_string(),
            sender_id,
            url: url.to_string(),
            channel_id: None,
            title: title.map(ToOwned::to_owned),
            description: description.map(ToOwned::to_owned),
            thumbnail_url: thumbnail_url.map(ToOwned::to_owned),
            site_name: site_name.map(ToOwned::to_owned),
        })?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_mini_program(
        &self,
        conversation_id: &str,
        app_id: &str,
        title: Option<&str>,
        page_path: Option<&str>,
        thumbnail_url: Option<&str>,
        extra: Option<std::collections::HashMap<String, String>>,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_mini_program(BuildMiniProgramRequest {
            conversation_id: conversation_id.to_string(),
            sender_id,
            app_id: app_id.to_string(),
            channel_id: None,
            title: title.map(ToOwned::to_owned),
            page_path: page_path.map(ToOwned::to_owned),
            thumbnail_url: thumbnail_url.map(ToOwned::to_owned),
            extra,
        })?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_rich_doc(&self, request: CreateRichDocRequest) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let conversation_id = request.conversation_id.clone();
        let msg = MessageBuilderService::build_rich_doc(BuildRichDocRequest {
            conversation_id: request.conversation_id,
            sender_id,
            doc_json: request.doc_json,
            content_schema: request.content_schema,
            plain_text: request.plain_text,
            channel_id: None,
            input_format: request.input_format,
            input_format_version: request.input_format_version,
            source_payload: request.source_payload,
            title: request.title,
            search_text: request.search_text,
            render_hints_json: request.render_hints_json,
        })?;
        self.apply_conversation_routing(&conversation_id, msg).await
    }

    pub async fn create_system(
        &self,
        conversation_id: &str,
        event_kind: &str,
        body: &str,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg =
            MessageBuilderService::build_system(conversation_id, &sender_id, event_kind, body)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_notification(
        &self,
        conversation_id: &str,
        title: &str,
        body: &str,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg =
            MessageBuilderService::build_notification(conversation_id, &sender_id, title, body)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_vote(
        &self,
        conversation_id: &str,
        vote_id: &str,
        title: &str,
        options: Vec<String>,
        participant_user_ids: Option<Vec<String>>,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_vote(
            conversation_id,
            &sender_id,
            vote_id,
            title,
            options,
            None,
            participant_user_ids,
        )?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_task(
        &self,
        conversation_id: &str,
        task_id: &str,
        title: &str,
        status: Option<&str>,
        participant_user_ids: Option<Vec<String>>,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_task(
            conversation_id,
            &sender_id,
            task_id,
            title,
            None,
            status,
            participant_user_ids,
        )?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_schedule(
        &self,
        conversation_id: &str,
        schedule_id: &str,
        title: &str,
        start_time_ms: i64,
        end_time_ms: i64,
        participant_user_ids: Option<Vec<String>>,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_schedule(BuildScheduleRequest {
            conversation_id: conversation_id.to_string(),
            sender_id,
            schedule_id: schedule_id.to_string(),
            title: title.to_string(),
            channel_id: None,
            start_time_ms,
            end_time_ms,
            participant_user_ids,
        })?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_announcement(
        &self,
        conversation_id: &str,
        title: &str,
        body: &str,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg =
            MessageBuilderService::build_announcement(conversation_id, &sender_id, title, body)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_custom(&self, conversation_id: &str, r#type: &str) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_custom(conversation_id, &sender_id, r#type, None)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }

    pub async fn create_placeholder(
        &self,
        conversation_id: &str,
        reason: &str,
    ) -> Result<IMMessage> {
        let sender_id = self.current_sender_id().await?;
        let msg = MessageBuilderService::build_placeholder(conversation_id, &sender_id, reason)?;
        self.apply_conversation_routing(conversation_id, msg).await
    }
}

#[cfg(test)]
mod needs_member_roster_tests {
    use super::needs_member_roster;
    use crate::model::Conversation;

    fn user_set_group(max_seq: u64) -> Conversation {
        let mut conv = Conversation::from_conversation_id("2ATESTGROUPCID0001".to_string());
        conv.ext
            .insert("group_source".to_string(), "user_ids".to_string());
        conv.max_seq = max_seq;
        conv
    }

    #[test]
    fn user_set_group_carries_roster_until_established() {
        // 未建群(max_seq==0):必须带成员表用于服务端建群。
        assert!(needs_member_roster(&user_set_group(0)));
    }

    #[test]
    fn user_set_group_omits_roster_after_established() {
        // 已建群(max_seq>0):成员恒定、服务端已持有 → 省略,回到 O(1)。
        assert!(!needs_member_roster(&user_set_group(7)));
    }

    #[test]
    fn user_set_group_omits_roster_when_server_established_flag_set() {
        // 显式建群 RPC 成功 → 即便 max_seq==0(尚无消息),首条消息也不带成员表。
        let mut conv = user_set_group(0);
        conv.ext
            .insert("group_server_established".to_string(), "1".to_string());
        assert!(!needs_member_roster(&conv));
    }

    #[test]
    fn non_user_set_group_always_carries_roster() {
        // 非 ad-hoc 用户集合群(无 group_source 标记,可能可变成员)即便已建群也按原样携带,不受优化影响。
        let mut conv = Conversation::from_conversation_id("2ATESTGROUPCID0002".to_string());
        conv.max_seq = 99;
        assert!(needs_member_roster(&conv));
    }
}

#[cfg(test)]
mod forward_sender_tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::MessageBuildApi;
    use crate::application::MessageBuilderService;
    use crate::content::message_elem::Elem;
    use crate::domain::{UserProfile, UserWriter};
    use crate::infrastructure::persistence::MemoryUserProfileStore;
    use crate::infrastructure::persistence::memory_im::MemoryConversationStore;

    /// A source the host hands over without a name is named from the identity cache by its sender id, as the
    /// timeline names it; a cached profile that only has the id as its name, or none at all, names no one.
    #[tokio::test]
    async fn a_forward_names_unnamed_sources_from_the_identity_cache() {
        let profiles = Arc::new(MemoryUserProfileStore::new());
        profiles
            .save_batch(&[
                UserProfile::new("u-carol").with_nickname("Lab Carol"),
                UserProfile::new("u-dave"),
                UserProfile::new("u-fay")
                    .with_nickname("Fay")
                    .with_alias("Fay from the lab"),
            ])
            .await
            .expect("profiles");
        let api = MessageBuildApi::new(
            Arc::new(RwLock::new("u-alice".to_string())),
            Arc::new(MemoryConversationStore::new()),
            profiles,
        );
        let source = |sender: &str, display: &str| {
            let mut message = MessageBuilderService::build_text(
                "2ATESTGROUPCID0001",
                sender,
                "hi",
                None,
                false,
                &[],
            )
            .expect("source");
            message.sender_display_name = display.into();
            message
        };
        let record = api
            .create_forward(
                "2ATESTGROUPCID0001",
                true,
                "",
                vec![
                    source("u-carol", ""),
                    source("u-dave", ""),
                    source("u-erin", ""),
                    source("u-bob", "Lab Bob"),
                    source("u-fay", ""),
                ],
            )
            .await
            .expect("forward");
        let Some(Elem::Forward(forward)) = record.content else {
            panic!("a forward elem");
        };
        let names: Vec<Option<&str>> = forward
            .items
            .iter()
            .map(|item| item.source_sender_name.as_deref())
            .collect();
        assert_eq!(
            names,
            vec![
                Some("Lab Carol"),
                None,
                None,
                Some("Lab Bob"),
                Some("Fay from the lab")
            ]
        );
    }

    /// The forwardability the test catalog declares: only these contributions.
    struct Declared(&'static [&'static str]);

    #[async_trait::async_trait]
    impl super::ForwardableCards for Declared {
        async fn forwardable(&self, card: &flare_plugin_protocol::ui::PluginMessage) -> bool {
            self.0.contains(&card.contribution_id.as_str())
        }
    }

    fn api() -> MessageBuildApi {
        MessageBuildApi::new(
            Arc::new(RwLock::new("u-alice".to_string())),
            Arc::new(MemoryConversationStore::new()),
            Arc::new(MemoryUserProfileStore::new()),
        )
    }

    fn text(server_id: &str, status: i32) -> crate::model::message::IMMessage {
        let mut message = MessageBuilderService::build_text(
            "2ATESTGROUPCID0001",
            "u-bob",
            "hi",
            None,
            false,
            &[],
        )
        .expect("text");
        message.server_id = server_id.into();
        message.sender_display_name = "Lab Bob".into();
        message.status = status;
        message
    }

    fn card(server_id: &str, plugin: &str, name: &str) -> crate::model::message::IMMessage {
        use crate::content::message_elem::{CustomElem, PLUGIN_MESSAGE_CARD_TYPE};
        let mut message = text(server_id, 3);
        // As a host hands a card over: its JSON carries the Custom projection and no encoded content.
        message.encoded_content.clear();
        message.content = Some(Elem::Custom(CustomElem {
            r#type: PLUGIN_MESSAGE_CARD_TYPE.into(),
            payload: serde_json::to_vec(&serde_json::json!({
                "pluginId": plugin, "contributionId": format!("{plugin}.card"),
                "schemaVersion": 1, "summary": "恭喜发财", "displayName": name,
                "payload": {"packetId": "p1", "amountMinor": "8888", "amountText": "¥88.88"}
            }))
            .expect("envelope"),
            description: String::new(),
            metadata: Default::default(),
        }));
        message
    }

    /// A merged forward of four messages that holds a red packet card and a recalled message is refused as a
    /// whole, naming both: nothing is left out of the record without the forwarder knowing.
    #[tokio::test]
    async fn a_forward_refuses_sources_that_cannot_be_forwarded_and_names_them() {
        let sources = vec![
            text("s-1", 3),
            card("s-2", "flare.redpacket", "红包"),
            text("s-3", 5),
            text("s-4", 3),
        ];
        for api in [
            api(),
            api().with_forwardable_cards(Arc::new(Declared(&["example.poll.card"]))),
        ] {
            let error = api
                .create_forward("2ATESTGROUPCID0001", true, "", sources.clone())
                .await
                .expect_err("refused");
            assert_eq!(
                error.code(),
                Some(crate::shared::error::ErrorCode::InvalidParameter)
            );
            assert_eq!(
                crate::refused_message_ids(&error),
                Some(vec!["s-2".to_owned(), "s-3".to_owned()])
            );
            assert_eq!(
                error.reason(),
                "sdk.message.forward.not_forwardable: s-2, s-3"
            );
        }
        let error = api()
            .create_forward(
                "2ATESTGROUPCID0001",
                false,
                "",
                vec![card("s-2", "flare.redpacket", "红包")],
            )
            .await
            .expect_err("a single forward too");
        assert_eq!(error.reason(), "sdk.message.forward.not_forwardable: s-2");
        assert_eq!(
            crate::refused_message_ids(&error),
            Some(vec!["s-2".to_owned()])
        );
        // A source picked twice is named once; one without a server id goes by its client id.
        let mut unsent = text("", 5);
        unsent.client_msg_id = "6b1f0c2e9a4d4f7e8c3b2a1d0e9f8a7b".into();
        let error = api()
            .create_forward(
                "2ATESTGROUPCID0001",
                true,
                "",
                vec![text("s-3", 5), unsent, text("s-3", 5), text("s-1", 3)],
            )
            .await
            .expect_err("refused");
        assert_eq!(
            crate::refused_message_ids(&error),
            Some(vec![
                "s-3".to_owned(),
                "6b1f0c2e9a4d4f7e8c3b2a1d0e9f8a7b".to_owned()
            ])
        );
        let record = api()
            .create_forward(
                "2ATESTGROUPCID0001",
                true,
                "",
                vec![text("s-1", 3), text("s-4", 4)],
            )
            .await
            .expect("plain and failed messages forward");
        assert!(matches!(record.content, Some(Elem::Forward(ref f)) if f.items.len() == 2));
    }

    /// A card whose contribution declares `forwardable` goes into the record, read as `[plugin] summary` and
    /// never as its data.
    #[tokio::test]
    async fn a_card_declared_forwardable_is_forwarded_by_its_words() {
        let api = api().with_forwardable_cards(Arc::new(Declared(&["example.poll.card"])));
        let record = api
            .create_forward(
                "2ATESTGROUPCID0001",
                true,
                "",
                vec![text("s-1", 3), card("s-2", "example.poll", "投票")],
            )
            .await
            .expect("forward");
        let Some(Elem::Forward(forward)) = record.content else {
            panic!("a forward elem");
        };
        assert_eq!(forward.items[1].plain_text, "[投票] 恭喜发财");
        assert!(forward.items[1].content.is_some());
    }
}
