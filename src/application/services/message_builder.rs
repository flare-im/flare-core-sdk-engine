//! 消息构建服务：封装 `ContentBuilder + MessageBuilder` 组合，产出 `IMMessage`。

use crate::content::MessageBuilder;
use crate::content::message_elem::{elem_plain_text, elem_to_message_content};
use crate::content::url_safety::is_safe_remote_url;
use crate::content::{BuiltContent, ContentBuilder, DEFAULT_STICKER_DISPLAY_SIDE};
use crate::model::message::IMMessage;
use crate::shared::error::{ErrorCode, FlareError, Result};
use flare_proto::common::{ForwardItem, ForwardMode, ImageInfo};
use std::collections::{HashMap, HashSet};

pub struct MessageBuilderService;

fn validate_optional_remote_url(field: &'static str, value: Option<&str>) -> Result<()> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    validate_required_remote_url(field, value)
}

fn validate_required_remote_url(field: &'static str, value: &str) -> Result<()> {
    if is_safe_remote_url(value) {
        return Ok(());
    }
    Err(FlareError::localized(
        ErrorCode::InvalidParameter,
        format!("{field}.invalid_url"),
    ))
}

/// 位置坐标必须是有限数、纬度在 -90..=90、经度在 -180..=180，且不同时为 0。
///
/// (0, 0) 视为没有坐标：`LocationContent` 的经纬度不是 optional，未赋值解码出来就是 (0, 0)，
/// 接收端分不清「真在 (0, 0)」与「没有坐标」，所以发送端不产出它；(0, 100) 这样赤道上的点照常。
pub fn validate_coordinate(latitude: f64, longitude: f64) -> Result<()> {
    let valid = latitude.is_finite()
        && longitude.is_finite()
        && (-90.0..=90.0).contains(&latitude)
        && (-180.0..=180.0).contains(&longitude)
        && !(latitude == 0.0 && longitude == 0.0);
    if valid {
        Ok(())
    } else {
        Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "sdk.message.location.invalid_coordinate",
        ))
    }
}

#[derive(Clone, Debug)]
pub struct BuildLocationRequest {
    pub conversation_id: String,
    pub sender_id: String,
    pub longitude: f64,
    pub latitude: f64,
    pub address: String,
    pub title: String,
    pub zoom: Option<u8>,
    pub snapshot_url: Option<String>,
    pub snapshot_local_path: Option<String>,
    pub channel_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BuildCardRequest {
    pub conversation_id: String,
    pub sender_id: String,
    pub id: String,
    pub card_type: String,
    pub title: String,
    pub subtitle: String,
    pub avatar: String,
    pub channel_id: Option<String>,
    /// `CardContent.attributes`, the card elem's `extra` (such as a shared
    /// person's `referrerId`); blank keys are dropped.
    pub attributes: Option<std::collections::HashMap<String, String>>,
}

#[derive(Clone, Debug)]
pub struct BuildStickerRequest {
    pub conversation_id: String,
    pub sender_id: String,
    pub sticker_id: String,
    pub channel_id: Option<String>,
    pub package_id: Option<String>,
    pub url: Option<String>,
    pub width: i32,
    pub height: i32,
    pub sticker_format: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BuildLinkCardRequest {
    pub conversation_id: String,
    pub sender_id: String,
    pub url: String,
    pub channel_id: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub thumbnail_url: Option<String>,
    pub site_name: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BuildMiniProgramRequest {
    pub conversation_id: String,
    pub sender_id: String,
    pub app_id: String,
    pub channel_id: Option<String>,
    pub title: Option<String>,
    pub page_path: Option<String>,
    pub thumbnail_url: Option<String>,
    pub extra: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug)]
pub struct BuildRichDocRequest {
    pub conversation_id: String,
    pub sender_id: String,
    pub doc_json: String,
    pub content_schema: String,
    pub plain_text: String,
    pub channel_id: Option<String>,
    pub input_format: Option<String>,
    pub input_format_version: Option<i32>,
    pub source_payload: Option<HashMap<String, String>>,
    pub title: Option<String>,
    pub search_text: Option<String>,
    pub render_hints_json: Option<String>,
}

#[derive(Clone, Debug)]
pub struct BuildScheduleRequest {
    pub conversation_id: String,
    pub sender_id: String,
    pub schedule_id: String,
    pub title: String,
    pub channel_id: Option<String>,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub participant_user_ids: Option<Vec<String>>,
}

impl MessageBuilderService {
    pub fn build_text(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        text: impl AsRef<str>,
        channel_id: Option<&str>,
        mention_all: bool,
        mention_user_ids: &[String],
    ) -> Result<IMMessage> {
        let text_str = text.as_ref();
        let mut content_builder = ContentBuilder::text(text_str);
        if mention_all {
            let len = text_str.chars().count().min(i32::MAX as usize) as i32;
            content_builder = content_builder.mention_all(0, len.max(1));
        }
        let mention_spans = mention_spans_for_text(text_str, mention_user_ids);
        for (user_id, start, length) in &mention_spans {
            content_builder = content_builder.mention_user(user_id, *start, *length);
        }
        let content = content_builder.build();
        let mut builder = MessageBuilder::new(conversation_id, sender_id).content(content);
        if let Some(channel_id) = channel_id {
            builder = builder.channel(channel_id).single_chat();
        }
        let mut message = IMMessage::new(builder.build()?);
        message.mention_all = mention_all;
        message.mention_users = mention_spans
            .into_iter()
            .map(|(user_id, _, _)| user_id)
            .collect();
        Ok(message)
    }

    pub fn build_quote(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        quoted_message_id: impl Into<String>,
        text: impl AsRef<str>,
        quoted_sender_id: Option<&str>,
        quoted_text_preview: Option<&str>,
        quoted_content: Option<BuiltContent>,
    ) -> Result<IMMessage> {
        let quoted_message_id = quoted_message_id.into();
        if quoted_message_id.trim().is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.message.quote.invalid_quoted_message_id",
            ));
        }
        let Some(quoted_content) = quoted_content else {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.message.quote.missing_quoted_content",
            ));
        };

        let quote_preview = quoted_text_preview.unwrap_or("").to_string();
        let current = ContentBuilder::text(text.as_ref()).build();
        let mut quote_builder = ContentBuilder::quote(quoted_message_id.clone())
            .quoted_text_preview(&quote_preview)
            .current(current);
        if let Some(sender_id) = quoted_sender_id
            && !sender_id.trim().is_empty()
        {
            quote_builder = quote_builder.quoted_sender_id(sender_id.trim());
        }
        quote_builder = quote_builder.quoted(quoted_content);
        let content = quote_builder.build();
        let message = MessageBuilder::new(conversation_id, sender_id)
            .content(content)
            .attributes("contentText", text.as_ref())
            .build()?;
        Ok(IMMessage::new(message))
    }

    pub fn build_thread_reply(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        thread_id: impl Into<String>,
        text: impl AsRef<str>,
    ) -> Result<IMMessage> {
        let thread_id = thread_id.into();
        if thread_id.trim().is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.message.thread.invalid_thread_id",
            ));
        }
        let content = ContentBuilder::text(text.as_ref()).build();
        let message = MessageBuilder::new(conversation_id, sender_id)
            .thread(thread_id)
            .content(content)
            .build()?;
        Ok(IMMessage::new(message))
    }

    /// 转发记录里源消息的发送者名：读到这条消息时的显示名（身份缓存的当前名字），否则发送时的快照名；账号 id
    /// 不是名字（身份缓存缺名时会以 id 充当显示名），两者都没有就不写，由展示端写「未知发送者」。
    pub fn forward_sender_name(source: &IMMessage) -> Option<String> {
        let sender_id = source.sender_id.trim();
        [
            source.sender_display_name.as_str(),
            source.sender_name.as_str(),
        ]
        .into_iter()
        .map(str::trim)
        .find(|name| !name.is_empty() && *name != sender_id)
        .map(str::to_owned)
    }

    /// `merge == false` 时必须恰好一条源消息（单条转发，`ForwardMode::Single`）。
    pub fn build_forward(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        merge: bool,
        title: impl Into<String>,
        sources: &[IMMessage],
    ) -> Result<IMMessage> {
        if sources.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.message.forward.empty_sources",
            ));
        }
        let mode = if merge {
            ForwardMode::Merged
        } else if sources.len() == 1 {
            ForwardMode::Single
        } else {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.message.forward.single_requires_one",
            ));
        };
        let items: Result<Vec<ForwardItem>> =
            sources.iter().map(forward_item_from_source).collect();
        let items = items?;
        let title_str = title.into();
        let title_opt = if title_str.trim().is_empty() {
            None
        } else {
            Some(title_str)
        };
        let content = ContentBuilder::forward(mode, title_opt, items).build();
        let message = MessageBuilder::new(conversation_id, sender_id)
            .content(content)
            .build()?;
        Ok(IMMessage::new(message))
    }

    pub fn build_with_content(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        content: BuiltContent,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        let mut builder = MessageBuilder::new(conversation_id, sender_id).content(content);
        if let Some(channel_id) = channel_id {
            builder = builder.channel(channel_id).single_chat();
        }
        Ok(IMMessage::new(builder.build()?))
    }

    pub fn build_image(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        image_id: impl Into<String>,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::image(image_id).build(),
            channel_id,
        )
    }

    /// 原图与缩略图使用不同本地路径或 file_id 时，发送阶段会分别上传。
    pub fn build_image_with_thumbnail(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        source_media_id: impl Into<String>,
        thumbnail_media_id: impl Into<String>,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::image_with_thumbnail(source_media_id, thumbnail_media_id).build(),
            channel_id,
        )
    }

    pub fn build_image_group(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        images: Vec<ImageInfo>,
        description: impl Into<String>,
        metadata: std::collections::HashMap<String, String>,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        if images.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.message.image_group.empty_images",
            ));
        }
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::image_group_with_details(images, description, metadata).build(),
            channel_id,
        )
    }

    pub fn build_video(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        video_id: impl Into<String>,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::video(video_id).build(),
            channel_id,
        )
    }

    pub fn build_audio(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        audio_id: impl Into<String>,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::audio(audio_id).build(),
            channel_id,
        )
    }

    pub fn build_file(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        file_id: impl Into<String>,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::file(file_id).build(),
            channel_id,
        )
    }

    pub fn build_location(request: BuildLocationRequest) -> Result<IMMessage> {
        validate_coordinate(request.latitude, request.longitude)?;
        validate_optional_remote_url(
            "sdk.message.location.snapshot_url",
            request.snapshot_url.as_deref(),
        )?;
        Self::build_with_content(
            request.conversation_id,
            request.sender_id,
            ContentBuilder::location(request.latitude, request.longitude)
                .address(request.address)
                .title(request.title)
                .location_zoom(request.zoom)
                .location_snapshot_url(request.snapshot_url)
                .location_snapshot_local_path(request.snapshot_local_path)
                .build(),
            request.channel_id.as_deref(),
        )
    }

    pub fn build_card(request: BuildCardRequest) -> Result<IMMessage> {
        validate_optional_remote_url("sdk.message.card.avatar", Some(request.avatar.as_str()))?;
        Self::build_with_content(
            request.conversation_id,
            request.sender_id,
            ContentBuilder::card(request.id)
                .card_type(request.card_type)
                .title(request.title)
                .subtitle(request.subtitle)
                .avatar(request.avatar)
                .card_extend_attributes(request.attributes.unwrap_or_default())
                .build(),
            request.channel_id.as_deref(),
        )
    }

    pub fn build_sticker(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        sticker_id: impl Into<String>,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        Self::build_sticker_with(BuildStickerRequest {
            conversation_id: conversation_id.into(),
            sender_id: sender_id.into(),
            sticker_id: sticker_id.into(),
            channel_id: channel_id.map(ToOwned::to_owned),
            package_id: None,
            url: None,
            width: 0,
            height: 0,
            sticker_format: None,
        })
    }

    /// 构建贴纸消息（`package_id` / `format` 与 proto `StickerContent` 一致）
    pub fn build_sticker_with(request: BuildStickerRequest) -> Result<IMMessage> {
        let mut b = ContentBuilder::sticker(request.sticker_id);
        if let Some(p) = request
            .package_id
            .as_deref()
            .filter(|s| !s.trim().is_empty())
        {
            b = b.package_id(p);
        }
        if let Some(u) = request.url.as_deref().filter(|s| !s.trim().is_empty()) {
            validate_required_remote_url("sdk.message.sticker.url", u)?;
            b = b.url(u);
        }
        let w = if request.width > 0 {
            request.width
        } else {
            DEFAULT_STICKER_DISPLAY_SIDE
        };
        let h = if request.height > 0 {
            request.height
        } else {
            DEFAULT_STICKER_DISPLAY_SIDE
        };
        b = b.size(w, h);
        if let Some(f) = request
            .sticker_format
            .as_deref()
            .filter(|s| !s.trim().is_empty())
        {
            b = b.sticker_format(f);
        }
        Self::build_with_content(
            request.conversation_id,
            request.sender_id,
            b.build(),
            request.channel_id.as_deref(),
        )
    }

    pub fn build_emoji(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        emoji: impl Into<String>,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::emoji(emoji).build(),
            channel_id,
        )
    }

    pub fn build_link_card(request: BuildLinkCardRequest) -> Result<IMMessage> {
        validate_required_remote_url("sdk.message.link_card.url", request.url.as_str())?;
        let mut b = ContentBuilder::link_card(request.url);
        if let Some(t) = request
            .title
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            b = b.title(t);
        }
        if let Some(d) = request
            .description
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            b = b.description(d);
        }
        if let Some(u) = request
            .thumbnail_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            validate_required_remote_url("sdk.message.link_card.thumbnail_url", u)?;
            b = b.link_card_thumbnail_url(u);
        }
        if let Some(s) = request
            .site_name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            b = b.link_card_site_name(s);
        }
        Self::build_with_content(
            request.conversation_id,
            request.sender_id,
            b.build(),
            request.channel_id.as_deref(),
        )
    }

    pub fn build_mini_program(request: BuildMiniProgramRequest) -> Result<IMMessage> {
        let mut b = ContentBuilder::mini_program(request.app_id);
        if let Some(t) = request
            .title
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            b = b.title(t);
        }
        if let Some(p) = request
            .page_path
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            b = b.page_path(p);
        }
        if let Some(u) = request
            .thumbnail_url
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            validate_required_remote_url("sdk.message.app_card.thumbnail_url", u)?;
            b = b.mini_program_thumbnail_url(u);
        }
        if let Some(e) = request.extra.filter(|m| !m.is_empty()) {
            b = b.mini_program_extend_extra(e);
        }
        Self::build_with_content(
            request.conversation_id,
            request.sender_id,
            b.build(),
            request.channel_id.as_deref(),
        )
    }

    /// 富文本（Rich Doc 主存储）。`content_schema == rich_doc` 时校验 `doc_json` 根结构。
    pub fn build_rich_doc(request: BuildRichDocRequest) -> Result<IMMessage> {
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
        Self::build_with_content(
            request.conversation_id,
            request.sender_id,
            cb.build(),
            request.channel_id.as_deref(),
        )
    }

    pub fn build_system(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        event_kind: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::system(event_kind, body).build(),
            None,
        )
    }

    pub fn build_notification(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::notification(title, body).build(),
            None,
        )
    }

    pub fn build_vote(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        vote_id: impl Into<String>,
        title: impl Into<String>,
        options: Vec<String>,
        channel_id: Option<&str>,
        participant_user_ids: Option<Vec<String>>,
    ) -> Result<IMMessage> {
        let mut b = ContentBuilder::vote(vote_id, title, options);
        if let Some(p) = participant_user_ids.filter(|v| !v.is_empty()) {
            b = b.vote_participant_user_ids(p);
        }
        Self::build_with_content(conversation_id, sender_id, b.build(), channel_id)
    }

    pub fn build_task(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        task_id: impl Into<String>,
        title: impl Into<String>,
        channel_id: Option<&str>,
        status: Option<&str>,
        participant_user_ids: Option<Vec<String>>,
    ) -> Result<IMMessage> {
        let mut b = ContentBuilder::task(task_id, title);
        if let Some(s) = status.map(str::trim).filter(|s| !s.is_empty()) {
            b = b.status(s);
        }
        if let Some(p) = participant_user_ids.filter(|v| !v.is_empty()) {
            b = b.task_participant_user_ids(p);
        }
        Self::build_with_content(conversation_id, sender_id, b.build(), channel_id)
    }

    pub fn build_schedule(request: BuildScheduleRequest) -> Result<IMMessage> {
        let mut b = ContentBuilder::schedule(request.schedule_id, request.title)
            .schedule_times_ms(request.start_time_ms, request.end_time_ms);
        if let Some(p) = request.participant_user_ids.filter(|v| !v.is_empty()) {
            b = b.schedule_participant_user_ids(p);
        }
        Self::build_with_content(
            request.conversation_id,
            request.sender_id,
            b.build(),
            request.channel_id.as_deref(),
        )
    }

    pub fn build_announcement(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        title: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::announcement(title, body).build(),
            None,
        )
    }

    pub fn build_custom(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        r#type: impl Into<String>,
        channel_id: Option<&str>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::custom(r#type).build(),
            channel_id,
        )
    }

    pub fn build_placeholder(
        conversation_id: impl Into<String>,
        sender_id: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<IMMessage> {
        Self::build_with_content(
            conversation_id,
            sender_id,
            ContentBuilder::placeholder(reason).build(),
            None,
        )
    }
}

fn mention_spans_for_text(text: &str, mention_user_ids: &[String]) -> Vec<(String, i32, i32)> {
    let mut spans = Vec::new();
    let mut seen = HashSet::new();
    for user_id in mention_user_ids {
        let user_id = user_id.trim();
        if user_id.is_empty() || !seen.insert(user_id.to_string()) {
            continue;
        }
        let token = format!("@{user_id}");
        let Some(start_byte) = text.find(&token) else {
            continue;
        };
        let start = text[..start_byte].chars().count().min(i32::MAX as usize) as i32;
        let length = token.chars().count().min(i32::MAX as usize) as i32;
        spans.push((user_id.to_string(), start, length.max(1)));
    }
    spans.sort_by_key(|(_, start, _)| *start);
    spans
}

fn forward_item_from_source(source: &IMMessage) -> Result<ForwardItem> {
    let elem = source.content.as_ref().ok_or_else(|| {
        FlareError::localized(
            ErrorCode::InvalidParameter,
            "sdk.message.forward.missing_content",
        )
    })?;
    let inner_mc = elem_to_message_content(elem);
    let plain = elem_plain_text(elem);
    let stable_id = {
        let sid = source.server_id.trim();
        if !sid.is_empty() {
            source.server_id.clone()
        } else {
            source.client_msg_id.clone()
        }
    };
    Ok(ForwardItem {
        source_message_id: Some(stable_id),
        source_conversation_id: Some(source.conversation_id.clone()),
        source_sender_id: Some(source.sender_id.clone()),
        source_sender_name: MessageBuilderService::forward_sender_name(source),
        source_message_created_at: Some(i64::try_from(source.created_at).unwrap_or(i64::MAX)),
        message_type: source.message_type,
        plain_text: plain,
        content: Some(inner_mc),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::message_elem::Elem;
    use crate::content::rich_doc_v2::pipeline::CONTENT_SCHEMA_RICH_DOC;
    use flare_proto::common::{ImageFormat, MessageType};

    fn location_at(latitude: f64, longitude: f64) -> Result<IMMessage> {
        MessageBuilderService::build_location(BuildLocationRequest {
            conversation_id: "conv-1".to_string(),
            sender_id: "sender-1".to_string(),
            longitude,
            latitude,
            address: String::new(),
            title: "Pin".to_string(),
            zoom: None,
            snapshot_url: None,
            snapshot_local_path: None,
            channel_id: None,
        })
    }

    #[test]
    fn location_coordinates_must_be_a_place() {
        for (latitude, longitude) in [
            (f64::NAN, 120.0),
            (30.0, f64::NAN),
            (f64::INFINITY, 120.0),
            (30.0, f64::NEG_INFINITY),
            (90.000_001, 0.5),
            (-90.5, 0.5),
            (30.0, 180.000_001),
            (30.0, -181.0),
            (0.0, 0.0),
            (-0.0, 0.0),
        ] {
            let error = location_at(latitude, longitude).expect_err("not a place");
            assert_eq!(error.code(), Some(ErrorCode::InvalidParameter));
            assert_eq!(
                error.reason(),
                "sdk.message.location.invalid_coordinate",
                "({latitude}, {longitude})"
            );
        }
    }

    #[test]
    fn location_coordinates_on_the_equator_or_the_edges_are_places() {
        for (latitude, longitude) in [
            (0.0, 100.0),
            (30.2, 0.0),
            (90.0, 180.0),
            (-90.0, -180.0),
            (30.2, 120.1),
        ] {
            location_at(latitude, longitude).expect("a place");
        }
    }

    fn link_request(url: &str) -> BuildLinkCardRequest {
        BuildLinkCardRequest {
            conversation_id: "conv-1".to_string(),
            sender_id: "sender-1".to_string(),
            url: url.to_string(),
            channel_id: None,
            title: None,
            description: None,
            thumbnail_url: None,
            site_name: None,
        }
    }

    fn image_info(id: &str) -> ImageInfo {
        ImageInfo {
            uuid: id.to_string(),
            image_id: id.to_string(),
            url: format!("https://flare.test/{id}.jpg"),
            mime_type: "image/jpeg".to_string(),
            size: 1024,
            width: 320,
            height: 180,
            format: ImageFormat::Jpeg as i32,
            animated: false,
            blurhash: String::new(),
        }
    }

    fn elem_kind(message: &IMMessage) -> &'static str {
        match message.content.as_ref().expect("message content") {
            Elem::Text(_) => "text",
            Elem::Image(_) => "image",
            Elem::Video(_) => "video",
            Elem::Audio(_) => "audio",
            Elem::File(_) => "file",
            Elem::Location(_) => "location",
            Elem::Card(_) => "card",
            Elem::Sticker(_) => "sticker",
            Elem::Emoji(_) => "emoji",
            Elem::Quote(_) => "quote",
            Elem::LinkCard(_) => "link_card",
            Elem::Forward(_) => "forward",
            Elem::Thread(_) => "thread",
            Elem::MiniProgram(_) => "mini_program",
            Elem::RichText(_) => "rich_text",
            Elem::ImageGroup(_) => "image_group",
            Elem::System(_) => "system",
            Elem::Notification(_) => "notification",
            Elem::Vote(_) => "vote",
            Elem::Task(_) => "task",
            Elem::Schedule(_) => "schedule",
            Elem::Announcement(_) => "announcement",
            Elem::Custom(_) => "custom",
            Elem::Placeholder(_) => "placeholder",
        }
    }

    fn assert_message_shape(
        name: &str,
        message: &IMMessage,
        expected_type: MessageType,
        expected_elem: &'static str,
    ) {
        assert_eq!(message.conversation_id, "conv-1", "{name}: conversation id");
        assert_eq!(message.sender_id, "sender-1", "{name}: sender id");
        assert_eq!(
            message.message_type, expected_type as i32,
            "{name}: message_type"
        );
        assert_eq!(elem_kind(message), expected_elem, "{name}: elem kind");
    }

    #[test]
    fn build_all_supported_message_types_to_strong_elem_contracts() {
        let source =
            MessageBuilderService::build_text("conv-src", "sender-1", "source", None, false, &[])
                .expect("source text");
        let quoted = ContentBuilder::text("quoted source").build();
        let rich_doc_json = r#"{"type":"doc","version":2,"children":[]}"#;
        let mut extra = HashMap::new();
        extra.insert("scope".to_string(), "all".to_string());

        let cases: Vec<(&str, MessageType, &'static str, IMMessage)> = vec![
            (
                "text",
                MessageType::Text,
                "text",
                MessageBuilderService::build_text("conv-1", "sender-1", "hello", None, true, &[])
                    .expect("text"),
            ),
            (
                "quote",
                MessageType::Quote,
                "quote",
                MessageBuilderService::build_quote(
                    "conv-1",
                    "sender-1",
                    "quoted-message-1",
                    "reply",
                    Some("alice"),
                    Some("quoted source"),
                    Some(quoted.clone()),
                )
                .expect("quote"),
            ),
            (
                "thread",
                MessageType::Text,
                "text",
                MessageBuilderService::build_thread_reply(
                    "conv-1", "sender-1", "thread-1", "reply",
                )
                .expect("thread"),
            ),
            (
                "forward",
                MessageType::Forward,
                "forward",
                MessageBuilderService::build_forward(
                    "conv-1",
                    "sender-1",
                    false,
                    "forward",
                    std::slice::from_ref(&source),
                )
                .expect("forward"),
            ),
            (
                "image",
                MessageType::Image,
                "image",
                MessageBuilderService::build_image("conv-1", "sender-1", "image-1", None)
                    .expect("image"),
            ),
            (
                "image_with_thumbnail",
                MessageType::Image,
                "image",
                MessageBuilderService::build_image_with_thumbnail(
                    "conv-1",
                    "sender-1",
                    "image-source-1",
                    "image-thumbnail-1",
                    None,
                )
                .expect("image thumbnail"),
            ),
            (
                "image_group",
                MessageType::ImageGroup,
                "image_group",
                MessageBuilderService::build_image_group(
                    "conv-1",
                    "sender-1",
                    vec![image_info("image-a"), image_info("image-b")],
                    "album",
                    HashMap::new(),
                    None,
                )
                .expect("image group"),
            ),
            (
                "video",
                MessageType::Video,
                "video",
                MessageBuilderService::build_video("conv-1", "sender-1", "video-1", None)
                    .expect("video"),
            ),
            (
                "audio",
                MessageType::Audio,
                "audio",
                MessageBuilderService::build_audio("conv-1", "sender-1", "audio-1", None)
                    .expect("audio"),
            ),
            (
                "file",
                MessageType::File,
                "file",
                MessageBuilderService::build_file("conv-1", "sender-1", "file-1", None)
                    .expect("file"),
            ),
            (
                "location",
                MessageType::Location,
                "location",
                MessageBuilderService::build_location(BuildLocationRequest {
                    conversation_id: "conv-1".to_string(),
                    sender_id: "sender-1".to_string(),
                    longitude: 120.1,
                    latitude: 30.2,
                    address: "Hangzhou".to_string(),
                    title: "Office".to_string(),
                    zoom: Some(16),
                    snapshot_url: Some("https://flare.test/map.png".to_string()),
                    snapshot_local_path: None,
                    channel_id: None,
                })
                .expect("location"),
            ),
            (
                "card",
                MessageType::Card,
                "card",
                MessageBuilderService::build_card(BuildCardRequest {
                    conversation_id: "conv-1".to_string(),
                    sender_id: "sender-1".to_string(),
                    id: "user-1".to_string(),
                    card_type: "user".to_string(),
                    title: "Alice".to_string(),
                    subtitle: "Engineer".to_string(),
                    avatar: "https://flare.test/avatar.png".to_string(),
                    channel_id: None,
                    attributes: None,
                })
                .expect("card"),
            ),
            (
                "sticker",
                MessageType::Sticker,
                "sticker",
                MessageBuilderService::build_sticker_with(BuildStickerRequest {
                    conversation_id: "conv-1".to_string(),
                    sender_id: "sender-1".to_string(),
                    sticker_id: "sticker-1".to_string(),
                    channel_id: None,
                    package_id: Some("pack-1".to_string()),
                    url: Some("https://flare.test/sticker.webp".to_string()),
                    width: 128,
                    height: 128,
                    sticker_format: Some("webp".to_string()),
                })
                .expect("sticker"),
            ),
            (
                "emoji",
                MessageType::Emoji,
                "emoji",
                MessageBuilderService::build_emoji("conv-1", "sender-1", "🙂", None)
                    .expect("emoji"),
            ),
            (
                "link_card",
                MessageType::LinkCard,
                "link_card",
                MessageBuilderService::build_link_card(BuildLinkCardRequest {
                    conversation_id: "conv-1".to_string(),
                    sender_id: "sender-1".to_string(),
                    url: "https://flare.test/post".to_string(),
                    channel_id: None,
                    title: Some("Post".to_string()),
                    description: Some("A post".to_string()),
                    thumbnail_url: Some("https://flare.test/thumb.png".to_string()),
                    site_name: Some("Flare".to_string()),
                })
                .expect("link card"),
            ),
            (
                "mini_program",
                MessageType::AppCard,
                "mini_program",
                MessageBuilderService::build_mini_program(BuildMiniProgramRequest {
                    conversation_id: "conv-1".to_string(),
                    sender_id: "sender-1".to_string(),
                    app_id: "flare.todo".to_string(),
                    channel_id: None,
                    title: Some("Todo".to_string()),
                    page_path: Some("/home".to_string()),
                    thumbnail_url: Some("https://flare.test/app.png".to_string()),
                    extra: Some(extra.clone()),
                })
                .expect("mini program"),
            ),
            (
                "rich_doc",
                MessageType::RichText,
                "rich_text",
                MessageBuilderService::build_rich_doc(BuildRichDocRequest {
                    conversation_id: "conv-1".to_string(),
                    sender_id: "sender-1".to_string(),
                    doc_json: rich_doc_json.to_string(),
                    content_schema: CONTENT_SCHEMA_RICH_DOC.to_string(),
                    plain_text: "doc".to_string(),
                    channel_id: None,
                    input_format: Some("markdown".to_string()),
                    input_format_version: Some(2),
                    source_payload: Some(extra.clone()),
                    title: Some("Doc".to_string()),
                    search_text: Some("doc".to_string()),
                    render_hints_json: Some("{}".to_string()),
                })
                .expect("rich doc"),
            ),
            (
                "system",
                MessageType::System,
                "system",
                MessageBuilderService::build_system(
                    "conv-1",
                    "sender-1",
                    "member_joined",
                    "joined",
                )
                .expect("system"),
            ),
            (
                "notification",
                MessageType::Notification,
                "notification",
                MessageBuilderService::build_notification("conv-1", "sender-1", "Notice", "Body")
                    .expect("notification"),
            ),
            (
                "vote",
                MessageType::AppCard,
                "vote",
                MessageBuilderService::build_vote(
                    "conv-1",
                    "sender-1",
                    "vote-1",
                    "Vote",
                    vec!["A".to_string(), "B".to_string()],
                    None,
                    Some(vec!["u1".to_string(), "u2".to_string()]),
                )
                .expect("vote"),
            ),
            (
                "task",
                MessageType::AppCard,
                "task",
                MessageBuilderService::build_task(
                    "conv-1",
                    "sender-1",
                    "task-1",
                    "Task",
                    None,
                    Some("open"),
                    Some(vec!["u1".to_string()]),
                )
                .expect("task"),
            ),
            (
                "schedule",
                MessageType::AppCard,
                "schedule",
                MessageBuilderService::build_schedule(BuildScheduleRequest {
                    conversation_id: "conv-1".to_string(),
                    sender_id: "sender-1".to_string(),
                    schedule_id: "schedule-1".to_string(),
                    title: "Standup".to_string(),
                    channel_id: None,
                    start_time_ms: 1_782_000_000_000,
                    end_time_ms: 1_782_003_600_000,
                    participant_user_ids: Some(vec!["u1".to_string()]),
                })
                .expect("schedule"),
            ),
            (
                "announcement",
                MessageType::AppCard,
                "announcement",
                MessageBuilderService::build_announcement(
                    "conv-1",
                    "sender-1",
                    "Announcement",
                    "Body",
                )
                .expect("announcement"),
            ),
            (
                "custom",
                MessageType::Custom,
                "custom",
                MessageBuilderService::build_custom("conv-1", "sender-1", "biz.custom", None)
                    .expect("custom"),
            ),
            (
                "placeholder",
                MessageType::Placeholder,
                "placeholder",
                MessageBuilderService::build_placeholder("conv-1", "sender-1", "encrypted")
                    .expect("placeholder"),
            ),
        ];

        for (name, expected_type, expected_elem, message) in cases {
            assert_message_shape(name, &message, expected_type, expected_elem);
        }
    }

    #[test]
    fn build_thread_reply_uses_typed_thread_id_and_text_content() {
        let message =
            MessageBuilderService::build_thread_reply("conv-1", "sender-1", "thread-1", "reply")
                .expect("thread");

        let Elem::Text(text) = message.content.as_ref().expect("thread reply content") else {
            panic!("text elem expected");
        };
        assert_eq!(message.thread_id.as_deref(), Some("thread-1"));
        assert_eq!(text.text, "reply");
        assert!(!message.attributes.contains_key("thread_id"));
        assert!(!message.attributes.contains_key("contentText"));
        assert!(
            message.text_preview.contains("reply"),
            "thread reply preview should include typed text: {}",
            message.text_preview
        );
    }

    #[test]
    fn build_card_writes_attributes_to_the_card_extra() {
        let card = |attributes| {
            MessageBuilderService::build_card(BuildCardRequest {
                conversation_id: "conv-1".to_string(),
                sender_id: "sender-1".to_string(),
                id: "user-7".to_string(),
                card_type: "user".to_string(),
                title: "Ann".to_string(),
                subtitle: String::new(),
                avatar: String::new(),
                channel_id: None,
                attributes,
            })
            .expect("card")
        };
        let shared = card(Some(
            [
                ("cardType", "user"),
                ("referrerId", "user-9"),
                ("  ", "blank key"),
                (" spaced ", "kept"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ));
        let Some(Elem::Card(elem)) = shared.content.as_ref() else {
            panic!("card elem expected");
        };
        assert_eq!(
            elem.extra,
            [
                ("cardType", "user"),
                ("referrerId", "user-9"),
                ("spaced", "kept")
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<std::collections::HashMap<_, _>>(),
            "blank keys dropped, keys trimmed"
        );
        let value = serde_json::to_value(&shared).expect("serialize card");
        assert_eq!(value["content"]["extra"]["referrerId"], "user-9");
        let Some(Elem::Card(plain)) = card(None).content else {
            panic!("card elem expected");
        };
        assert!(plain.extra.is_empty());
    }

    #[test]
    fn built_notification_roundtrips_through_binding_message_json() {
        let message =
            MessageBuilderService::build_notification("conv-1", "sender-1", "Notice", "Body")
                .expect("notification");

        let value = serde_json::to_value(&message).expect("serialize notification message");
        assert_eq!(value["content"]["contentType"], "notification");
        assert_eq!(value["content"]["title"], "Notice");
        assert_eq!(value["content"]["body"], "Body");

        let decoded: IMMessage =
            serde_json::from_value(value).expect("deserialize binding notification message");

        assert_message_shape(
            "notification",
            &decoded,
            MessageType::Notification,
            "notification",
        );
    }

    #[test]
    fn build_message_type_constraints_are_enforced() {
        let quoted_missing = MessageBuilderService::build_quote(
            "conv-1", "sender-1", "quoted-1", "reply", None, None, None,
        )
        .expect_err("quote requires quoted content");
        assert_eq!(quoted_missing.code(), Some(ErrorCode::InvalidParameter));

        let empty_group = MessageBuilderService::build_image_group(
            "conv-1",
            "sender-1",
            vec![],
            "album",
            HashMap::new(),
            None,
        )
        .expect_err("image group requires images");
        assert_eq!(empty_group.code(), Some(ErrorCode::InvalidParameter));

        let source_a =
            MessageBuilderService::build_text("conv-src", "sender-1", "a", None, false, &[])
                .expect("source a");
        let source_b =
            MessageBuilderService::build_text("conv-src", "sender-1", "b", None, false, &[])
                .expect("source b");
        let bad_forward = MessageBuilderService::build_forward(
            "conv-1",
            "sender-1",
            false,
            "forward",
            &[source_a, source_b],
        )
        .expect_err("single forward requires exactly one source");
        assert_eq!(bad_forward.code(), Some(ErrorCode::InvalidParameter));
    }

    /// A forward record carries each message's readable words and its sender's name: the words the conversation
    /// list shows as they are (a plugin card as `[plugin] summary`, never its data), nothing for a message only a
    /// type word can name, and the name the forwarder sees — never the account id. Whether a card may be forwarded
    /// at all is `MessageBuildApi::create_forward`'s check: a card whose contribution does not declare
    /// `forwardable`, such as a red packet, is refused there and never reaches the builder.
    #[test]
    fn a_forward_record_carries_readable_words_and_sender_names() {
        use crate::content::message_elem::{CustomElem, PLUGIN_MESSAGE_CARD_TYPE};
        let text = |display: &str, snapshot: &str| {
            let mut source = MessageBuilderService::build_text(
                "conv-src",
                "u-bob",
                "今晚七点见",
                None,
                false,
                &[],
            )
            .expect("text source");
            source.sender_display_name = display.into();
            source.sender_name = snapshot.into();
            source
        };
        let image = MessageBuilderService::build_image("conv-src", "u-bob", "image-1", None)
            .expect("image source");
        let mut card = text("Lab Bob", "");
        card.content = Some(Elem::Custom(CustomElem {
            r#type: PLUGIN_MESSAGE_CARD_TYPE.into(),
            payload: serde_json::to_vec(&serde_json::json!({
                "pluginId": "example.poll", "contributionId": "example.poll.card",
                "schemaVersion": 1, "summary": "午饭吃什么", "displayName": "投票",
                "payload": {"pollId": "poll-1", "voters": 88}
            }))
            .expect("envelope"),
            description: String::new(),
            metadata: Default::default(),
        }));
        let sources = [
            text("Lab Bob", "Bob"),
            text("u-bob", "Bob"),
            text(" ", "u-bob"),
            image,
            card,
        ];
        let record = MessageBuilderService::build_forward("conv-1", "sender-1", true, "", &sources)
            .expect("forward");
        let Some(Elem::Forward(forward)) = record.content else {
            panic!("a forward elem");
        };
        let items: Vec<(Option<&str>, &str)> = forward
            .items
            .iter()
            .map(|item| (item.source_sender_name.as_deref(), item.plain_text.as_str()))
            .collect();
        assert_eq!(
            items,
            vec![
                (Some("Lab Bob"), "今晚七点见"),
                (Some("Bob"), "今晚七点见"),
                (None, "今晚七点见"),
                (None, ""),
                (Some("Lab Bob"), "[投票] 午饭吃什么"),
            ]
        );
        assert!(forward.items.iter().all(|item| item.content.is_some()));
        assert!(!forward.items[4].plain_text.contains("88"));
    }

    #[test]
    fn build_text_materializes_structured_user_mentions() {
        let mention_users = vec!["10".to_string(), "12".to_string()];
        let message = MessageBuilderService::build_text(
            "conv-mentions",
            "sender-1",
            "hi @10 and @12",
            None,
            false,
            &mention_users,
        )
        .expect("text with mentions");

        assert_eq!(message.mention_users, mention_users);
        assert!(!message.mention_all);
        let Elem::Text(text) = message.content.as_ref().expect("text content") else {
            panic!("expected text content");
        };
        assert_eq!(text.mentions.len(), 2);
        assert_eq!(text.mentions[0].user_id, "10");
        assert_eq!(text.mentions[0].start, 3);
        assert_eq!(text.mentions[0].length, 3);
        assert_eq!(text.mentions[1].user_id, "12");
        assert_eq!(text.mentions[1].start, 11);
        assert_eq!(text.mentions[1].length, 3);
    }

    #[test]
    fn build_link_card_rejects_unsafe_url() {
        let err = MessageBuilderService::build_link_card(link_request("javascript:alert(1)"))
            .expect_err("unsafe link card url must fail");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    #[test]
    fn build_link_card_rejects_unsafe_thumbnail_url() {
        let mut request = link_request("https://flare.test/post");
        request.thumbnail_url = Some("data:text/html,<script></script>".to_string());

        let err = MessageBuilderService::build_link_card(request)
            .expect_err("unsafe thumbnail url must fail");

        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter));
    }

    #[test]
    fn build_link_card_accepts_https_urls() {
        let mut request = link_request("https://flare.test/post");
        request.thumbnail_url = Some("https://flare.test/thumb.png".to_string());

        MessageBuilderService::build_link_card(request).expect("https urls must pass");
    }
}
