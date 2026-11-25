use crate::content::message_elem::{AudioInfoElem, ImageInfoElem, VideoInfoElem};
use crate::model::{Elem, MessageSearchKind, decode_content_bytes, decoded_content_to_elem};
use flare_proto::common::MessageType;

fn push_search_part(out: &mut String, value: &str) {
    let value = value.trim();
    if value.is_empty() {
        return;
    }
    if !out.is_empty() {
        out.push(' ');
    }
    out.push_str(value);
}

fn push_image_info_search_text(out: &mut String, info: &ImageInfoElem) {
    push_search_part(out, &info.uuid);
    push_search_part(out, &info.mime_type);
}

fn push_video_info_search_text(out: &mut String, info: &VideoInfoElem) {
    push_search_part(out, &info.uuid);
    push_search_part(out, &info.mime_type);
}

fn push_audio_info_search_text(out: &mut String, info: &AudioInfoElem) {
    push_search_part(out, &info.uuid);
    push_search_part(out, &info.mime_type);
}

pub fn elem_search_text(elem: &Elem) -> Option<String> {
    let mut out = String::new();
    match elem {
        Elem::Text(text) => {
            push_search_part(&mut out, &text.text);
        }
        Elem::RichText(rich) => {
            if let Some(title) = &rich.title {
                push_search_part(&mut out, title);
            }
            push_search_part(&mut out, &rich.plain_text);
            if let Some(search_text) = &rich.search_text {
                push_search_part(&mut out, search_text);
            }
        }
        Elem::Image(image) => {
            push_search_part(&mut out, &image.description);
            if let Some(source) = &image.source {
                push_image_info_search_text(&mut out, source);
            }
            if let Some(thumbnail) = &image.thumbnail {
                push_image_info_search_text(&mut out, thumbnail);
            }
        }
        Elem::Video(video) => {
            push_search_part(&mut out, &video.description);
            push_search_part(&mut out, &video.video_id);
            if let Some(source) = &video.source {
                push_video_info_search_text(&mut out, source);
            }
            if let Some(cover) = &video.cover {
                push_image_info_search_text(&mut out, cover);
            }
        }
        Elem::Audio(audio) => {
            push_search_part(&mut out, &audio.description);
            push_search_part(&mut out, &audio.audio_id);
            if let Some(source) = &audio.source {
                push_audio_info_search_text(&mut out, source);
            }
        }
        Elem::File(file) => {
            push_search_part(&mut out, &file.file_name);
            push_search_part(&mut out, &file.description);
            push_search_part(&mut out, &file.mime_type);
            push_search_part(&mut out, &file.file_id);
        }
        Elem::Location(location) => {
            push_search_part(&mut out, &location.title);
            push_search_part(&mut out, &location.address);
        }
        Elem::Card(card) => {
            push_search_part(&mut out, &card.title);
            push_search_part(&mut out, &card.subtitle);
        }
        Elem::Emoji(emoji) => {
            push_search_part(&mut out, &emoji.emoji);
            push_search_part(&mut out, &emoji.description);
        }
        Elem::Quote(quote) => {
            push_search_part(&mut out, &quote.quoted_text_preview);
            if let Some(current) = &quote.current_content
                && let Some(text) = elem_search_text(current)
            {
                push_search_part(&mut out, &text);
            }
            if let Some(quoted) = &quote.quoted_content
                && let Some(text) = elem_search_text(quoted)
            {
                push_search_part(&mut out, &text);
            }
        }
        Elem::LinkCard(link) => {
            push_search_part(&mut out, &link.title);
            push_search_part(&mut out, &link.description);
            push_search_part(&mut out, &link.site_name);
            push_search_part(&mut out, &link.url);
        }
        Elem::Forward(forward) => {
            if let Some(title) = &forward.title {
                push_search_part(&mut out, title);
            }
            for item in &forward.items {
                push_search_part(&mut out, &item.plain_text);
                if let Some(content) = &item.content
                    && let Some(text) = elem_search_text(content)
                {
                    push_search_part(&mut out, &text);
                }
            }
        }
        Elem::Thread(thread) => {
            push_search_part(&mut out, &thread.thread_title);
            if let Some(root) = &thread.root_content
                && let Some(text) = elem_search_text(root)
            {
                push_search_part(&mut out, &text);
            }
        }
        Elem::MiniProgram(mini) => {
            push_search_part(&mut out, &mini.title);
            push_search_part(&mut out, &mini.app_id);
            push_search_part(&mut out, &mini.page_path);
        }
        Elem::ImageGroup(group) => {
            push_search_part(&mut out, &group.description);
            for image in &group.images {
                push_image_info_search_text(&mut out, image);
            }
        }
        Elem::System(system) => {
            push_search_part(&mut out, &system.body);
            push_search_part(&mut out, &system.event_kind);
        }
        Elem::Notification(notification) => {
            push_search_part(&mut out, &notification.title);
            push_search_part(&mut out, &notification.body);
            push_search_part(&mut out, &notification.notification_type);
        }
        Elem::Vote(vote) => {
            push_search_part(&mut out, &vote.title);
            for option in &vote.options {
                push_search_part(&mut out, option);
            }
        }
        Elem::Task(task) => {
            push_search_part(&mut out, &task.title);
            push_search_part(&mut out, &task.status);
        }
        Elem::Schedule(schedule) => {
            push_search_part(&mut out, &schedule.title);
        }
        Elem::Announcement(announcement) => {
            push_search_part(&mut out, &announcement.title);
            push_search_part(&mut out, &announcement.body);
        }
        Elem::Custom(custom) => {
            push_search_part(&mut out, &custom.r#type);
            push_search_part(&mut out, &custom.description);
        }
        Elem::Placeholder(placeholder) => {
            push_search_part(&mut out, &placeholder.fallback_text);
            push_search_part(&mut out, &placeholder.reason);
        }
        Elem::Sticker(sticker) => {
            push_search_part(&mut out, &sticker.sticker_id);
            push_search_part(&mut out, &sticker.package_id);
            push_search_part(&mut out, &sticker.format);
        }
    }
    (!out.is_empty()).then_some(out)
}

pub fn search_text_for_content_bytes(bytes: &[u8]) -> Option<String> {
    decode_content_bytes(bytes)
        .ok()
        .and_then(|decoded| decoded_content_to_elem(&decoded))
        .and_then(|elem| elem_search_text(&elem))
}

pub fn message_type_values_for_search(kinds: &[MessageSearchKind]) -> Vec<i32> {
    let mut values = Vec::new();
    for kind in kinds {
        match kind {
            MessageSearchKind::Message => return Vec::new(),
            MessageSearchKind::Text => {
                values.push(MessageType::Text as i32);
                values.push(MessageType::RichText as i32);
                values.push(MessageType::Quote as i32);
            }
            MessageSearchKind::Media => {
                values.push(MessageType::Image as i32);
                values.push(MessageType::Video as i32);
                values.push(MessageType::Audio as i32);
                values.push(MessageType::File as i32);
                values.push(MessageType::ImageGroup as i32);
            }
            MessageSearchKind::Image => {
                values.push(MessageType::Image as i32);
                values.push(MessageType::ImageGroup as i32);
            }
            MessageSearchKind::Video => values.push(MessageType::Video as i32),
            MessageSearchKind::Audio => values.push(MessageType::Audio as i32),
            MessageSearchKind::File => values.push(MessageType::File as i32),
        }
    }
    values.sort_unstable();
    values.dedup();
    values
}
