use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::application::commands::SendMessageCommand;
use crate::application::{UploadPhase, UploadProgress, UploadProgressCallback};
use crate::content::message_elem::{AudioInfoElem, Elem, ImageInfoElem, VideoInfoElem};
use crate::domain::{
    ConversationIdentityService, ConversationStore, MessageActor, MessageDraftService, MessageStore,
};
use crate::extension::middleware::MiddlewareChain;
use crate::infrastructure::protocol::PacketSender;
use crate::kernel::event::{EventBus, MessageEvent, SdkEvent};
use crate::kernel::{CurrentUserIdStore, ReliableSendQueuePort};
use crate::model::UploadedMedia;
use crate::model::conversation::ConversationType;
use crate::model::message::{IMMessage, MessageLocalState, MessageStatus, SendAck};
use crate::platform::ports::media::{
    MediaMetadata, MediaServicePort, MediaSourceDescriptor, MediaSourceKind, ProcessedMedia,
    UploadProgressSink,
};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::spawn_background;
use base64::Engine as _;
use flare_proto::common::ImageFormat;

pub struct MessageSendUseCase {
    sender: Arc<PacketSender>,
    store: Arc<dyn MessageStore>,
    conversation_store: Arc<dyn ConversationStore>,
    chain: Arc<MiddlewareChain>,
    current_user_id: CurrentUserIdStore,
    reliable_queue: Option<Arc<dyn ReliableSendQueuePort>>,
    media_service: Arc<dyn MediaServicePort>,
    bus: EventBus,
    draft_service: MessageDraftService,
    conversation_identity: ConversationIdentityService,
}

impl MessageSendUseCase {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sender: Arc<PacketSender>,
        store: Arc<dyn MessageStore>,
        conversation_store: Arc<dyn ConversationStore>,
        chain: Arc<MiddlewareChain>,
        current_user_id: CurrentUserIdStore,
        reliable_queue: Option<Arc<dyn ReliableSendQueuePort>>,
        media_service: Arc<dyn MediaServicePort>,
        bus: EventBus,
    ) -> Self {
        Self {
            sender,
            store,
            conversation_store,
            chain,
            current_user_id,
            reliable_queue,
            media_service,
            bus,
            draft_service: MessageDraftService::default(),
            conversation_identity: ConversationIdentityService,
        }
    }

    pub async fn current_user_id(&self) -> Result<String> {
        let uid = self.current_user_id.read().await.clone();
        if uid.is_empty() {
            return Err(FlareError::localized(ErrorCode::NotConnected, "未连接"));
        }
        Ok(uid)
    }

    async fn actor(&self) -> Result<MessageActor> {
        MessageActor::require(self.current_user_id().await?)
    }

    pub async fn send(&self, message: IMMessage) -> Result<SendAck> {
        let (_actor, message) = self.prepare_outbound_identity(message).await?;
        self.draft_service.validate_outbound_message(&message)?;
        self.dispatch_prepared_message(message).await
    }

    async fn prepare_outbound_identity(
        &self,
        message: IMMessage,
    ) -> Result<(MessageActor, IMMessage)> {
        let actor = self.actor().await?;
        let mut message = message;
        if let Some(rewrite) = ConversationIdentityService::canonicalize_single_chat_message(
            &mut message,
            &actor.user_id,
        ) {
            self.store
                .rewrite_conversation_id(&rewrite.from, &rewrite.to)
                .await?;
            self.conversation_store
                .merge_conversation_identity(&rewrite.from, &rewrite.to)
                .await?;
        }
        let message = self
            .draft_service
            .prepare_outbound_identity(&actor, message)?;
        Ok((actor, message))
    }

    async fn dispatch_prepared_message(&self, message: IMMessage) -> Result<SendAck> {
        self.ensure_optimistic_conversation(&message).await?;
        if let Some(queue) = &self.reliable_queue {
            // 仅取构建 ack 所需的两个 id，避免在发送热路径整体 clone IMMessage
            // （含 encoded_content/attributes，文本消息小、媒体消息可达 KB 级）。
            let client_msg_id = message.client_msg_id.clone();
            let conversation_id = message.conversation_id.clone();
            SendMessageCommand::new(message)
                .execute_via_queue(queue.as_ref(), &self.chain)
                .await?;
            Ok(SendAck {
                client_msg_id,
                conversation_id,
                ack_id: None,
                result: None,
            })
        } else {
            // 存储不带 PendingSend 仓储时的直发：没有暂时性重试，见 SendMessageCommand::execute。
            SendMessageCommand::new(message)
                .execute(&self.sender, self.store.as_ref(), &self.chain)
                .await
        }
    }

    async fn ensure_optimistic_conversation(&self, message: &IMMessage) -> Result<()> {
        let conversation_id = message.conversation_id.trim();
        if conversation_id.is_empty() {
            return Ok(());
        }
        if self
            .conversation_store
            .get(conversation_id)
            .await?
            .is_some()
        {
            return Ok(());
        }

        let conversation_type = ConversationType::from_proto_int(message.conversation_type);
        if matches!(conversation_type, ConversationType::Unspecified) {
            return Ok(());
        }

        let channel_id = message.channel_id.trim();
        if channel_id.is_empty() {
            return Ok(());
        }

        let business_type = message
            .attributes
            .get("business_type")
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| conversation_type.as_str());

        let mut conversation = self.conversation_identity.build_local_conversation(
            conversation_id,
            Some(channel_id),
            conversation_type,
            business_type,
            channel_id.to_string(),
        );
        conversation.updated_at = message.created_at;
        conversation.created_at = conversation.updated_at;
        self.conversation_store.save_batch(&[conversation]).await
    }

    pub async fn send_with_media(
        &self,
        message: IMMessage,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<SendAck> {
        let (_actor, mut message) = self.prepare_outbound_identity(message).await?;
        let sort_ts = message.client_created_at.max(message.created_at);
        message.status = MessageStatus::Created as i32;
        message.local_state = MessageLocalState {
            sending: true,
            failed: false,
            is_local: true,
            uploading: true,
            upload_progress: 0,
            sort_ts,
        };
        attach_local_media_preview(&mut message);
        message.materialize_encoded_content_from_elem();
        self.ensure_optimistic_conversation(&message).await?;
        persist_local_message_update(&self.store, &self.conversation_store, &self.bus, &message)
            .await?;

        let client_msg_id = message.client_msg_id.clone();
        let progress = self.upload_progress_callback(client_msg_id.clone(), on_progress);
        let normalized = match self
            .normalize_message_media_with_progress(message.clone(), Some(progress))
            .await
        {
            Ok(mut normalized) => {
                normalized.status = MessageStatus::Created as i32;
                normalized.local_state = MessageLocalState {
                    sending: true,
                    failed: false,
                    is_local: true,
                    uploading: false,
                    upload_progress: 100,
                    sort_ts,
                };
                normalized.materialize_encoded_content_from_elem();
                persist_local_message_update(
                    &self.store,
                    &self.conversation_store,
                    &self.bus,
                    &normalized,
                )
                .await?;
                normalized
            }
            Err(error) => {
                let mut failed = self
                    .store
                    .get_by_client_msg_id(&client_msg_id)
                    .await?
                    .unwrap_or(message);
                failed.status = MessageStatus::Failed as i32;
                failed.local_state = MessageLocalState {
                    sending: false,
                    failed: true,
                    is_local: true,
                    uploading: false,
                    upload_progress: failed.local_state.upload_progress,
                    sort_ts,
                };
                persist_local_message_update(
                    &self.store,
                    &self.conversation_store,
                    &self.bus,
                    &failed,
                )
                .await?;
                self.bus
                    .publish(SdkEvent::Message(MessageEvent::SendFailed {
                        client_msg_id,
                        reason: error.to_string(),
                        error: crate::domain::SendFailure::from_error(&error),
                    }));
                return Err(error);
            }
        };

        self.draft_service.validate_outbound_message(&normalized)?;
        match self.dispatch_prepared_message(normalized.clone()).await {
            Ok(ack) => Ok(ack),
            Err(error) => {
                let mut failed = normalized;
                failed.status = MessageStatus::Failed as i32;
                failed.local_state = MessageLocalState {
                    sending: false,
                    failed: true,
                    is_local: true,
                    uploading: false,
                    upload_progress: 100,
                    sort_ts,
                };
                persist_local_message_update(
                    &self.store,
                    &self.conversation_store,
                    &self.bus,
                    &failed,
                )
                .await?;
                self.bus
                    .publish(SdkEvent::Message(MessageEvent::SendFailed {
                        client_msg_id: failed.client_msg_id.clone(),
                        reason: error.to_string(),
                        error: crate::domain::SendFailure::from_error(&error),
                    }));
                Err(error)
            }
        }
    }

    fn upload_progress_callback(
        &self,
        client_msg_id: String,
        external: Option<UploadProgressCallback>,
    ) -> UploadProgressCallback {
        let store = self.store.clone();
        let conversation_store = self.conversation_store.clone();
        let bus = self.bus.clone();
        let last_persisted = Arc::new(AtomicU32::new(0));
        Arc::new(move |progress: UploadProgress| {
            if let Some(callback) = external.as_ref() {
                callback(progress.clone());
            }
            let next_progress = if matches!(progress.phase, UploadPhase::Finished) {
                100
            } else {
                upload_progress_percent(&progress).min(99)
            };
            if !progress_worth_persisting(last_persisted.load(Ordering::Relaxed), next_progress) {
                return;
            }
            last_persisted.store(next_progress, Ordering::Relaxed);
            let client_msg_id = client_msg_id.clone();
            let store = store.clone();
            let conversation_store = conversation_store.clone();
            let bus = bus.clone();
            spawn_background(async move {
                let Ok(Some(mut current)) = store.get_by_client_msg_id(&client_msg_id).await else {
                    return;
                };
                if current.local_state.failed || current.status == MessageStatus::Failed as i32 {
                    return;
                }
                if !current.local_state.uploading && current.local_state.upload_progress >= 100 {
                    return;
                }
                if next_progress < current.local_state.upload_progress {
                    return;
                }
                current.local_state.sending = true;
                current.local_state.failed = false;
                current.local_state.is_local = true;
                current.local_state.uploading = true;
                current.local_state.upload_progress = next_progress;
                let _ =
                    persist_local_message_update(&store, &conversation_store, &bus, &current).await;
            });
        })
    }

    async fn normalize_message_media_with_progress(
        &self,
        mut message: IMMessage,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<IMMessage> {
        let mut touched = false;
        if let Some(ref mut content) = message.content {
            match content {
                Elem::File(file) => {
                    if let Some(source) = extract_media_source(&file.file_id)
                        .or_else(|| extract_media_source(&file.url))
                    {
                        let uploaded = self
                            .upload_media_source(source, on_progress.clone())
                            .await?;
                        let display_url = uploaded_media_display_url(&uploaded);
                        file.file_id = uploaded.file_id;
                        if !uploaded.file_name.is_empty() {
                            file.file_name = uploaded.file_name;
                        }
                        if !uploaded.mime_type.is_empty() {
                            file.mime_type = uploaded.mime_type;
                        }
                        if uploaded.size > 0 {
                            file.file_size = uploaded.size;
                        }
                        file.url = display_url;
                        touched = true;
                    }
                }
                Elem::Image(image) => {
                    let source_media: Option<MediaSourceDescriptor> =
                        image.source.as_ref().and_then(image_info_media_source);
                    let thumb_media: Option<MediaSourceDescriptor> =
                        image.thumbnail.as_ref().and_then(image_info_media_source);
                    if let Some(source) = source_media {
                        let uploaded = self
                            .upload_media_source(source.clone(), on_progress.clone())
                            .await?;
                        let desc = uploaded_media_to_image_descriptor(&uploaded);
                        image.source = desc.clone();
                        match thumb_media {
                            Some(thumb) if !same_media_source(&thumb, &source) => {
                                let uploaded_thumb =
                                    self.upload_media_source(thumb, on_progress.clone()).await?;
                                image.thumbnail =
                                    uploaded_media_to_image_descriptor(&uploaded_thumb);
                            }
                            _ => {
                                image.thumbnail = image.source.clone();
                            }
                        }
                        touched = true;
                    } else if let Some(thumb) = thumb_media {
                        let uploaded = self.upload_media_source(thumb, on_progress.clone()).await?;
                        let desc = uploaded_media_to_image_descriptor(&uploaded);
                        image.source = desc.clone();
                        image.thumbnail = desc;
                        touched = true;
                    }
                }
                Elem::Video(video) => {
                    if let Some(source) = extract_media_source(&video.video_id).or_else(|| {
                        video
                            .source
                            .as_ref()
                            .and_then(|info| extract_media_source(&info.url))
                    }) {
                        let uploaded = self
                            .upload_media_source(source, on_progress.clone())
                            .await?;
                        video.video_id = uploaded.file_id.clone();
                        video.source =
                            uploaded_media_to_video_descriptor(&uploaded, video.source.as_ref());
                        touched = true;
                    }
                }
                Elem::Audio(audio) => {
                    if let Some(source) = extract_media_source(&audio.audio_id).or_else(|| {
                        audio
                            .source
                            .as_ref()
                            .and_then(|info| extract_media_source(&info.url))
                    }) {
                        let uploaded = self
                            .upload_media_source(source, on_progress.clone())
                            .await?;
                        audio.audio_id = uploaded.file_id.clone();
                        audio.source =
                            uploaded_media_to_audio_descriptor(&uploaded, audio.source.as_ref());
                        touched = true;
                    }
                }
                Elem::ImageGroup(group) => {
                    let count = group.images.len();
                    for (index, image) in group.images.iter_mut().enumerate() {
                        if let Some(source) = image_info_media_source(image) {
                            let progress = on_progress
                                .clone()
                                .map(|callback| image_group_progress(callback, index, count));
                            let uploaded = self.upload_media_source(source, progress).await?;
                            if let Some(desc) = uploaded_media_to_image_descriptor(&uploaded) {
                                *image = desc;
                                touched = true;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        if touched {
            message.encoded_content.clear();
        }
        Ok(message)
    }

    async fn upload_media_source(
        &self,
        source: MediaSourceDescriptor,
        on_progress: Option<UploadProgressCallback>,
    ) -> Result<UploadedMedia> {
        let media = processed_media_from_source(source)?;
        let progress: Option<UploadProgressSink> =
            on_progress.map(|callback| Box::new(move |progress| callback(progress)) as _);
        self.media_service.upload(media, None, progress).await
    }
}

async fn persist_local_message_update(
    store: &Arc<dyn MessageStore>,
    conversation_store: &Arc<dyn ConversationStore>,
    bus: &EventBus,
    message: &IMMessage,
) -> Result<()> {
    store.save_one(message).await?;
    update_local_conversation_projection(conversation_store, message).await?;
    bus.publish(SdkEvent::Message(MessageEvent::ReceivedBatch {
        messages: vec![message.clone()],
    }));
    Ok(())
}

async fn update_local_conversation_projection(
    conversation_store: &Arc<dyn ConversationStore>,
    message: &IMMessage,
) -> Result<()> {
    let conversation_id = message.conversation_id.trim();
    if conversation_id.is_empty() {
        return Ok(());
    }
    let message_id = if message.server_id.trim().is_empty() {
        message.client_msg_id.trim()
    } else {
        message.server_id.trim()
    };
    if message_id.is_empty() {
        return Ok(());
    }
    let preview = message.text_for_storage();
    conversation_store
        .update_last_message(
            conversation_id,
            message_id,
            message.sender_id(),
            message.timeline_sort_ts(),
            preview.as_deref(),
            message.conversation_seq,
        )
        .await
}

/// 多图消息逐张上传，每张各自从 0 报到 100；换算成整组进度，否则第二张起
/// 全被「进度不回退」挡掉，进度条停在第一张传完的位置。
fn image_group_progress(
    callback: UploadProgressCallback,
    index: usize,
    count: usize,
) -> UploadProgressCallback {
    let count = count.max(1) as u64;
    let index = index as u64;
    Arc::new(move |mut progress: UploadProgress| {
        let percent = u64::from(upload_progress_percent(&progress));
        progress.uploaded_bytes = index * 100 + percent;
        progress.total_bytes = count * 100;
        if matches!(progress.phase, UploadPhase::Finished) && index + 1 < count {
            progress.phase = UploadPhase::Uploading;
        }
        callback(progress);
    })
}

/// 进度落库的最小步长（百分点）。每次落库都整条重写消息再经总线推给界面，
/// 而传输层按 64KB 块 / XHR 事件上报 —— 不节流，几 MB 的文件就是上百次整条重写。
const UPLOAD_PROGRESS_PERSIST_STEP: u32 = 5;

fn progress_worth_persisting(last_persisted: u32, next: u32) -> bool {
    next > last_persisted
        && (next >= 99 || next >= last_persisted.saturating_add(UPLOAD_PROGRESS_PERSIST_STEP))
}

fn upload_progress_percent(progress: &UploadProgress) -> u32 {
    if matches!(progress.phase, UploadPhase::Finished) {
        return 100;
    }
    if let Some(percent) = progress
        .uploaded_bytes
        .saturating_mul(100)
        .checked_div(progress.total_bytes)
    {
        return percent.min(100) as u32;
    }
    match progress.phase {
        UploadPhase::Preparing => 0,
        UploadPhase::Uploading => 1,
        UploadPhase::Completing => 99,
        UploadPhase::Finished => 100,
    }
}

fn attach_local_media_preview(message: &mut IMMessage) {
    let Some(ref mut content) = message.content else {
        return;
    };
    match content {
        Elem::File(file) => {
            if file.url.trim().is_empty()
                && let Some(locator) = local_preview_locator(&file.file_id)
            {
                file.url = locator;
            }
            // 上传完成前气泡就要有文件名、大小、类型：以前要等上传结果回填，
            // 上传期间文件气泡只有一个没名字的图标。
            if let Some(facts) = local_media_facts(&file.file_id) {
                if file.file_name.trim().is_empty() {
                    file.file_name = facts.file_name;
                }
                if file.file_size <= 0 {
                    file.file_size = facts.size;
                }
                if file.mime_type.trim().is_empty() {
                    file.mime_type = facts.mime_type;
                }
            }
        }
        Elem::Image(image) => {
            if let Some(source) = image.source.as_mut() {
                attach_image_preview(source);
                fill_local_facts(
                    &source.image_id.clone(),
                    &mut source.mime_type,
                    &mut source.size,
                );
            }
            if let Some(thumbnail) = image.thumbnail.as_mut() {
                attach_image_preview(thumbnail);
            }
            if image.thumbnail.is_none() {
                image.thumbnail = image.source.clone();
            }
        }
        Elem::ImageGroup(group) => {
            for image in &mut group.images {
                attach_image_preview(image);
                let locator = image.image_id.clone();
                fill_local_facts(&locator, &mut image.mime_type, &mut image.size);
            }
        }
        Elem::Video(video) => {
            if video.source.is_none()
                && let Some(locator) = local_preview_locator(&video.video_id)
            {
                video.source = Some(VideoInfoElem {
                    uuid: video.video_id.clone(),
                    url: locator,
                    mime_type: String::new(),
                    size: 0,
                    duration_ms: 0,
                    width: 0,
                    height: 0,
                });
            }
            if let Some(source) = video.source.as_mut() {
                attach_video_preview(source, &video.video_id);
                fill_local_facts(&video.video_id, &mut source.mime_type, &mut source.size);
                fill_local_duration(&video.video_id, &source.url, &mut source.duration_ms);
            }
        }
        Elem::Audio(audio) => {
            if audio.source.is_none()
                && let Some(locator) = local_preview_locator(&audio.audio_id)
            {
                audio.source = Some(AudioInfoElem {
                    uuid: audio.audio_id.clone(),
                    url: locator,
                    mime_type: String::new(),
                    size: 0,
                    duration_ms: 0,
                });
            }
            if let Some(source) = audio.source.as_mut() {
                attach_audio_preview(source, &audio.audio_id);
                fill_local_facts(&audio.audio_id, &mut source.mime_type, &mut source.size);
                fill_local_duration(&audio.audio_id, &source.url, &mut source.duration_ms);
            }
        }
        _ => {}
    }
    message.encoded_content.clear();
}

fn attach_image_preview(info: &mut ImageInfoElem) {
    if !info.url.trim().is_empty() {
        return;
    }
    let locator =
        local_preview_locator(&info.image_id).or_else(|| local_preview_locator(&info.uuid));
    if let Some(locator) = locator {
        info.url = locator;
    }
}

fn attach_video_preview(info: &mut VideoInfoElem, fallback: &str) {
    if !info.url.trim().is_empty() {
        return;
    }
    let locator = local_preview_locator(&info.uuid).or_else(|| local_preview_locator(fallback));
    if let Some(locator) = locator {
        info.url = locator;
    }
}

fn attach_audio_preview(info: &mut AudioInfoElem, fallback: &str) {
    if !info.url.trim().is_empty() {
        return;
    }
    let locator = local_preview_locator(&info.uuid).or_else(|| local_preview_locator(fallback));
    if let Some(locator) = locator {
        info.url = locator;
    }
}

/// 从本地媒体来源读出的文件名、大小、类型（上传前就能显示）。
#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalMediaFacts {
    file_name: String,
    size: i64,
    mime_type: String,
}

fn fill_local_facts(locator: &str, mime_type: &mut String, size: &mut i64) {
    if let Some(facts) = local_media_facts(locator) {
        if mime_type.trim().is_empty() {
            *mime_type = facts.mime_type;
        }
        if *size <= 0 {
            *size = facts.size;
        }
    }
}

/// 音视频没带时长（0）时，从本地文件头读出来：MP4/MOV/M4A 读 `mvhd`，WAV 读 `fmt `/`data`。
///
/// 宿主按本地路径发音视频通常不传时长（Flutter `createMedia(path, 'video')`、kit 录的
/// pcm16 `.wav` 语音都没有），以前气泡在各端都显示 00:00。只读容器头；webm、截断文件、
/// 非本地路径（`data:`、`content://`、`blob:`）读不出就维持 0，绝不因此让发送失败。
fn fill_local_duration(media_id: &str, preview_url: &str, duration_ms: &mut i64) {
    if *duration_ms > 0 {
        return;
    }
    if let Some(ms) =
        local_container_duration_ms(media_id).or_else(|| local_container_duration_ms(preview_url))
    {
        *duration_ms = ms;
    }
}

fn local_container_duration_ms(locator: &str) -> Option<i64> {
    let source = extract_media_source(locator)?;
    if !matches!(source.kind, MediaSourceKind::Path) {
        return None;
    }
    #[cfg(not(target_arch = "wasm32"))]
    let duration = crate::shared::util::media_duration::file_duration_ms(std::path::Path::new(
        &source.locator,
    ));
    #[cfg(target_arch = "wasm32")]
    let duration: Option<u64> = None;
    duration.and_then(|ms| i64::try_from(ms).ok())
}

/// `data:<mime>;name=<编码后的文件名>;size=<字节数>;base64,…` 读头部；本地路径读文件名，
/// 原生端再读文件大小。认不出的来源（blob:、content:// 等）返回 `None`，交给上传结果回填。
fn local_media_facts(locator: &str) -> Option<LocalMediaFacts> {
    let source = extract_media_source(locator)?;
    match source.kind {
        MediaSourceKind::Bytes if source.locator.starts_with("data:") => {
            let (header, body) = source.locator.split_once(',')?;
            let header = header.strip_prefix("data:").unwrap_or(header);
            let mut facts = LocalMediaFacts {
                file_name: String::new(),
                size: 0,
                mime_type: String::new(),
            };
            let mut base64_encoded = false;
            for part in header.split(';').map(str::trim).filter(|p| !p.is_empty()) {
                if part.eq_ignore_ascii_case("base64") {
                    base64_encoded = true;
                } else if let Some((key, value)) = part.split_once('=') {
                    match key {
                        "name" => facts.file_name = percent_decode(value),
                        "size" => facts.size = value.parse::<i64>().unwrap_or(0),
                        _ => {}
                    }
                } else if part.contains('/') {
                    facts.mime_type = part.to_string();
                }
            }
            if facts.size <= 0 && base64_encoded {
                let body = body.trim_end();
                let padding = body.chars().rev().take_while(|c| *c == '=').count();
                facts.size = ((body.len() / 4) * 3).saturating_sub(padding) as i64;
            }
            if facts.mime_type.is_empty() {
                facts.mime_type = mime_type_for_name(&facts.file_name);
            }
            Some(facts)
        }
        MediaSourceKind::Path => {
            let path = std::path::Path::new(&source.locator);
            let file_name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            #[cfg(not(target_arch = "wasm32"))]
            let size = std::fs::metadata(path)
                .map(|meta| meta.len() as i64)
                .unwrap_or(0);
            #[cfg(target_arch = "wasm32")]
            let size = 0;
            let mime_type = mime_type_for_name(&file_name);
            Some(LocalMediaFacts {
                file_name,
                size,
                mime_type,
            })
        }
        _ => None,
    }
}

/// 按扩展名给出 MIME；认不出返回空串（不硬塞 octet-stream，让上传结果来定）。
fn mime_type_for_name(file_name: &str) -> String {
    let ext = file_name
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "heic" => "image/heic",
        "bmp" => "image/bmp",
        "svg" => "image/svg+xml",
        "mp4" | "m4v" => "video/mp4",
        "mov" => "video/quicktime",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        "m4a" => "audio/mp4",
        "aac" => "audio/aac",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "txt" => "text/plain",
        "csv" => "text/csv",
        "doc" => "application/msword",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls" => "application/vnd.ms-excel",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ppt" => "application/vnd.ms-powerpoint",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        _ => "",
    }
    .to_string()
}

fn local_preview_locator(value: &str) -> Option<String> {
    extract_media_source(value).map(|source| match source.kind {
        MediaSourceKind::Path => source.locator,
        MediaSourceKind::Uri
        | MediaSourceKind::Asset
        | MediaSourceKind::Blob
        | MediaSourceKind::Bytes
        | MediaSourceKind::Custom(_) => source.locator,
    })
}

/// 识别「可上传媒体源」：Unix/Windows path、`file://`、Android `content://`、
/// iOS `ph://`/`assets-library://`、RN/uni-app 临时 URI、Web `blob:`/`data:`。
fn extract_media_source(input: &str) -> Option<MediaSourceDescriptor> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }
    if input.starts_with('/') || input.starts_with("./") || input.starts_with("../") {
        return Some(MediaSourceDescriptor::path(input));
    }
    // Windows: C:\path 或 C:/path
    let b = input.as_bytes();
    if input.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' {
        return Some(MediaSourceDescriptor::path(input));
    }
    // UNC \\server\share
    if input.starts_with("\\\\") {
        return Some(MediaSourceDescriptor::path(input));
    }
    if let Some(rest) = input.strip_prefix("file://") {
        let path_str = file_url_to_path_string(rest)?;
        return Some(MediaSourceDescriptor::path(path_str));
    }
    if input.starts_with("content://")
        || input.starts_with("rnfs://")
        || input.starts_with("wxfile://")
        || input.starts_with("uni://")
    {
        return Some(MediaSourceDescriptor::uri(input));
    }
    if input.starts_with("ph://")
        || input.starts_with("assets-library://")
        || input.starts_with("asset://")
        || input.starts_with("assets://")
    {
        return Some(MediaSourceDescriptor::asset(input));
    }
    if input.starts_with("blob:") {
        return Some(MediaSourceDescriptor::blob(input));
    }
    if input.starts_with("data:") {
        return Some(MediaSourceDescriptor::bytes(input, Default::default()));
    }
    None
}

/// `file://` 之后的路径段：`/Users/...` 保留；`/C:/...` 去掉前导 `/` 得到 `C:/...`。
fn file_url_to_path_string(after_scheme: &str) -> Option<String> {
    if after_scheme.is_empty() {
        return None;
    }
    if after_scheme.starts_with('/')
        && after_scheme.len() >= 3
        && after_scheme.as_bytes()[2] == b':'
        && after_scheme.as_bytes()[1].is_ascii_alphabetic()
    {
        return Some(after_scheme[1..].to_string());
    }
    Some(after_scheme.to_string())
}

fn image_info_media_source(info: &ImageInfoElem) -> Option<MediaSourceDescriptor> {
    let id = info.image_id.trim();
    if !id.is_empty()
        && let Some(source) = extract_media_source(id)
    {
        return Some(source);
    }
    let u = info.uuid.trim();
    if !u.is_empty()
        && let Some(source) = extract_media_source(u)
    {
        return Some(source);
    }
    extract_media_source(&info.url)
}

fn same_media_source(a: &MediaSourceDescriptor, b: &MediaSourceDescriptor) -> bool {
    std::mem::discriminant(&a.kind) == std::mem::discriminant(&b.kind) && a.locator == b.locator
}

fn uploaded_media_to_image_descriptor(uploaded: &UploadedMedia) -> Option<ImageInfoElem> {
    if uploaded.file_id.trim().is_empty() {
        return None;
    }
    let mime_lower = uploaded.mime_type.to_lowercase();
    let (format, animated) = if mime_lower.contains("gif") {
        (ImageFormat::Gif as i32, true)
    } else if mime_lower.contains("png") && mime_lower.contains("apng") {
        (ImageFormat::Apng as i32, true)
    } else if mime_lower.contains("png") {
        (ImageFormat::Png as i32, false)
    } else if mime_lower.contains("jpeg") || mime_lower.contains("jpg") {
        (ImageFormat::Jpeg as i32, false)
    } else if mime_lower.contains("webp") {
        (ImageFormat::Webp as i32, false)
    } else if mime_lower.contains("bmp") {
        (ImageFormat::Bmp as i32, false)
    } else if mime_lower.contains("heic") || mime_lower.contains("heif") {
        (ImageFormat::Heic as i32, false)
    } else if mime_lower.contains("svg") {
        (ImageFormat::Svg as i32, false)
    } else {
        (ImageFormat::Unspecified as i32, false)
    };
    Some(ImageInfoElem {
        uuid: uploaded.file_id.clone(),
        image_id: uploaded.file_id.clone(),
        url: uploaded_media_display_url(uploaded),
        mime_type: uploaded.mime_type.clone(),
        size: uploaded.size,
        width: 0,
        height: 0,
        format,
        animated,
        blurhash: String::new(),
    })
}

/// 上传结果只有 id/名字/类型/大小；时长和宽高是上传前从本地文件读出（或宿主给）的，
/// 换成上传后的描述时要从 `local` 带过来，否则发出去的视频时长又回到 0。
fn uploaded_media_to_video_descriptor(
    uploaded: &UploadedMedia,
    local: Option<&VideoInfoElem>,
) -> Option<VideoInfoElem> {
    if uploaded.file_id.trim().is_empty() {
        return None;
    }
    Some(VideoInfoElem {
        uuid: uploaded.file_id.clone(),
        url: uploaded_media_display_url(uploaded),
        mime_type: uploaded.mime_type.clone(),
        size: uploaded.size,
        duration_ms: local.map_or(0, |local| local.duration_ms),
        width: local.map_or(0, |local| local.width),
        height: local.map_or(0, |local| local.height),
    })
}

fn uploaded_media_to_audio_descriptor(
    uploaded: &UploadedMedia,
    local: Option<&AudioInfoElem>,
) -> Option<AudioInfoElem> {
    if uploaded.file_id.trim().is_empty() {
        return None;
    }
    Some(AudioInfoElem {
        uuid: uploaded.file_id.clone(),
        url: uploaded_media_display_url(uploaded),
        mime_type: uploaded.mime_type.clone(),
        size: uploaded.size,
        duration_ms: local.map_or(0, |local| local.duration_ms),
    })
}

/// The address an uploaded file goes out with: none. The message carries the file id and every reader resolves it
/// through `media.get_url`, which answers with whatever this deployment can serve — a public address, or a presigned
/// one for a private bucket.
///
/// The upload response's own addresses cannot stand in for that. For a private object `url` is a presigned link that
/// expires, and `cdn_url` is `cdn_base + object path` with no signature, which the bucket refuses (403): a message
/// stamped with either showed "image failed to load" to its sender and to everyone who received it.
fn uploaded_media_display_url(_uploaded: &UploadedMedia) -> String {
    String::new()
}

fn processed_media_from_source(mut source: MediaSourceDescriptor) -> Result<ProcessedMedia> {
    if matches!(source.kind, MediaSourceKind::Bytes) && source.locator.starts_with("data:") {
        let (payload, metadata) =
            decode_data_media_source(&source.locator, source.metadata.clone().unwrap_or_default())?;
        source.metadata = Some(metadata.clone());
        return Ok(ProcessedMedia {
            source,
            metadata,
            payload: Some(payload),
        });
    }

    let metadata = source.metadata.clone().unwrap_or_default();
    Ok(ProcessedMedia {
        source,
        metadata,
        payload: None,
    })
}

fn decode_data_media_source(
    locator: &str,
    fallback: MediaMetadata,
) -> Result<(Vec<u8>, MediaMetadata)> {
    let Some((header, body)) = locator.split_once(',') else {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "invalid data media source",
        ));
    };
    let header = header.strip_prefix("data:").unwrap_or(header);
    let mut mime_type = fallback.mime_type;
    let mut file_name = fallback.file_name;
    let mut declared_size = fallback.size;
    let mut base64_encoded = false;

    for part in header.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if part.eq_ignore_ascii_case("base64") {
            base64_encoded = true;
            continue;
        }
        if let Some((key, value)) = part.split_once('=') {
            match key {
                "name" => file_name = percent_decode(value),
                "size" => declared_size = value.parse::<u64>().unwrap_or(declared_size),
                _ => {}
            }
            continue;
        }
        if part.contains('/') {
            mime_type = part.to_string();
        }
    }

    if !base64_encoded {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "data media source must be base64 encoded",
        ));
    }
    let payload = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|error| {
            FlareError::localized(
                ErrorCode::InvalidParameter,
                format!("invalid data media payload: {error}"),
            )
        })?;
    if mime_type.trim().is_empty() {
        mime_type = "application/octet-stream".to_string();
    }
    if file_name.trim().is_empty() {
        file_name = default_file_name_for_mime(&mime_type).to_string();
    }
    if declared_size == 0 {
        declared_size = payload.len() as u64;
    }

    Ok((
        payload,
        MediaMetadata {
            file_name,
            mime_type,
            size: declared_size,
            width: fallback.width,
            height: fallback.height,
            duration_ms: fallback.duration_ms,
            extra: fallback.extra,
        },
    ))
}

fn percent_decode(value: &str) -> String {
    let mut out = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_value(bytes[i + 1]);
            let lo = hex_value(bytes[i + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| value.to_string())
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn default_file_name_for_mime(mime_type: &str) -> &'static str {
    if mime_type.starts_with("image/") {
        "image-upload"
    } else if mime_type.starts_with("video/") {
        "video-upload"
    } else if mime_type.starts_with("audio/") {
        "audio-upload"
    } else {
        "file-upload"
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ImageFormat, MessageSendUseCase, attach_local_media_preview, extract_media_source,
        processed_media_from_source, uploaded_media_display_url,
        uploaded_media_to_image_descriptor,
    };
    use async_trait::async_trait;
    use flare_proto::common::ImageInfo;
    use std::sync::Arc;
    use tokio::sync::{Mutex, RwLock, oneshot, watch};

    use crate::content::{ContentBuilder, Elem, MessageBuilder};
    use crate::domain::{ConversationStore, MessageStore};
    use crate::extension::middleware::MiddlewareChain;
    use crate::infrastructure::protocol::{PacketSender, ProtobufCodec};
    use crate::kernel::event::{EventBus, MessageEvent, SdkEvent};
    use crate::kernel::{CurrentUserIdStore, ReliableSendQueuePort};
    use crate::model::message::{IMMessage, MessageStatus, SendAck};
    use crate::model::{
        MediaAccessUrl, MediaCacheEntryVo, MediaResolvedAccess, UploadOptions, UploadedMedia,
    };
    use crate::platform::ports::media::{
        MediaServicePort, MediaSourceKind, ProcessedMedia, UploadProgressSink,
    };
    use crate::shared::error::{ErrorCode, FlareError, Result};
    use crate::storage::{MemoryConversationStore, MemoryMessageStore};

    struct TestHarness {
        usecase: MessageSendUseCase,
        messages: Arc<dyn MessageStore>,
        _conversations: Arc<dyn ConversationStore>,
        queue: Arc<CapturingReliableQueue>,
        media: Arc<ControlledMediaService>,
    }

    impl TestHarness {
        fn new(media_result: MediaResult) -> Self {
            let messages: Arc<dyn MessageStore> = Arc::new(MemoryMessageStore::new());
            let conversations: Arc<dyn ConversationStore> =
                Arc::new(MemoryConversationStore::new());
            let queue = Arc::new(CapturingReliableQueue::default());
            let media = Arc::new(ControlledMediaService::new(media_result));
            let sender = Arc::new(PacketSender::new(
                Arc::new(Mutex::new(None)),
                Arc::new(ProtobufCodec),
            ));
            let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("hugo".to_string()));
            let usecase = MessageSendUseCase::new(
                sender,
                messages.clone(),
                conversations.clone(),
                Arc::new(MiddlewareChain::new()),
                current_user_id,
                Some(queue.clone()),
                media.clone(),
                EventBus::new(),
            );
            Self {
                usecase,
                messages,
                _conversations: conversations,
                queue,
                media,
            }
        }
    }

    #[derive(Clone)]
    enum MediaResult {
        Uploaded(UploadedMedia),
        Failed,
    }

    struct ControlledMediaService {
        result: MediaResult,
        started_tx: watch::Sender<bool>,
        started_rx: watch::Receiver<bool>,
        release_tx: Mutex<Option<oneshot::Sender<()>>>,
        release_rx: Mutex<Option<oneshot::Receiver<()>>>,
    }

    impl ControlledMediaService {
        fn new(result: MediaResult) -> Self {
            let (release_tx, release_rx) = oneshot::channel();
            let (started_tx, started_rx) = watch::channel(false);
            Self {
                result,
                started_tx,
                started_rx,
                release_tx: Mutex::new(Some(release_tx)),
                release_rx: Mutex::new(Some(release_rx)),
            }
        }

        async fn wait_until_upload_started(&self) {
            let mut started_rx = self.started_rx.clone();
            if *started_rx.borrow() {
                return;
            }
            let _ = started_rx.changed().await;
        }

        async fn finish_upload(&self) {
            if let Some(tx) = self.release_tx.lock().await.take() {
                let _ = tx.send(());
            }
        }
    }

    #[async_trait]
    impl MediaServicePort for ControlledMediaService {
        async fn upload(
            &self,
            media: ProcessedMedia,
            _options: Option<UploadOptions>,
            progress: Option<UploadProgressSink>,
        ) -> Result<UploadedMedia> {
            if let Some(progress) = &progress {
                progress(crate::application::UploadProgress {
                    file_name: media.source.locator.clone(),
                    upload_id: "upload-test".to_string(),
                    phase: crate::application::UploadPhase::Uploading,
                    uploaded_bytes: 50,
                    total_bytes: 100,
                    chunk_index: Some(1),
                    total_chunks: Some(2),
                });
            }
            let _ = self.started_tx.send(true);
            if let Some(rx) = self.release_rx.lock().await.take() {
                let _ = rx.await;
            }
            match &self.result {
                MediaResult::Uploaded(uploaded) => {
                    if let Some(progress) = &progress {
                        progress(crate::application::UploadProgress {
                            file_name: uploaded.file_name.clone(),
                            upload_id: "upload-test".to_string(),
                            phase: crate::application::UploadPhase::Finished,
                            uploaded_bytes: 100,
                            total_bytes: 100,
                            chunk_index: Some(2),
                            total_chunks: Some(2),
                        });
                    }
                    Ok(uploaded.clone())
                }
                MediaResult::Failed => Err(FlareError::localized(
                    ErrorCode::GeneralError,
                    "controlled upload failed",
                )),
            }
        }

        async fn delete_file(&self, _file_id: &str, _hard_delete: bool) -> Result<bool> {
            Ok(true)
        }

        async fn get_file_url(&self, file_id: &str, _expires_in: i32) -> Result<MediaAccessUrl> {
            Ok(MediaAccessUrl {
                url: format!("https://media.example/{file_id}"),
                cdn_url: None,
            })
        }

        async fn get_temp_url_for_file_download(
            &self,
            file_id: &str,
            expires_in: i32,
        ) -> Result<MediaAccessUrl> {
            self.get_file_url(file_id, expires_in).await
        }

        async fn resolve_media_access(
            &self,
            file_id: &str,
            expires_in: i32,
        ) -> Result<MediaResolvedAccess> {
            Ok(MediaResolvedAccess {
                source: "remote".to_string(),
                local_path: None,
                remote: Some(self.get_file_url(file_id, expires_in).await?),
            })
        }

        async fn cache_remote_media(
            &self,
            file_id: &str,
            expires_in: i32,
        ) -> Result<MediaCacheEntryVo> {
            Ok(MediaCacheEntryVo {
                file_id: file_id.to_string(),
                local_path: String::new(),
                mime_type: String::new(),
                size_bytes: 0,
                updated_at_ms: expires_in as i64,
            })
        }
    }

    #[derive(Default)]
    struct CapturingReliableQueue {
        enqueued: Mutex<Vec<IMMessage>>,
    }

    impl CapturingReliableQueue {
        async fn enqueued(&self) -> Vec<IMMessage> {
            self.enqueued.lock().await.clone()
        }
    }

    #[async_trait]
    impl ReliableSendQueuePort for CapturingReliableQueue {
        async fn enqueue(&self, message: IMMessage) -> Result<()> {
            self.enqueued.lock().await.push(message);
            Ok(())
        }

        async fn on_ack(&self, _ack: SendAck) -> Result<()> {
            Ok(())
        }

        async fn reset_pending_on_login(&self) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn recover_pending_for_current_user(&self) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
    }

    fn bare_message(content: crate::content::content_builder::BuiltContent) -> IMMessage {
        IMMessage::new(
            MessageBuilder::new("conv-media", "hugo")
                .single_chat()
                .channel("peer-1")
                .content(content)
                .build()
                .expect("message"),
        )
    }

    fn file_of(message: &IMMessage) -> &crate::content::message_elem::FileElem {
        match message.content.as_ref().expect("content") {
            Elem::File(file) => file,
            other => panic!("expected file, got {other:?}"),
        }
    }

    #[test]
    fn image_group_progress_advances_across_images() {
        use super::{image_group_progress, upload_progress_percent};
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        let callback: crate::application::UploadProgressCallback =
            Arc::new(move |progress: crate::application::UploadProgress| {
                sink.lock().unwrap().push((
                    upload_progress_percent(&progress),
                    matches!(progress.phase, crate::application::UploadPhase::Finished),
                ));
            });
        let step = |index, phase, uploaded| {
            image_group_progress(callback.clone(), index, 2)(crate::application::UploadProgress {
                file_name: String::new(),
                upload_id: String::new(),
                phase,
                uploaded_bytes: uploaded,
                total_bytes: 10,
                chunk_index: None,
                total_chunks: None,
            })
        };
        step(0, crate::application::UploadPhase::Uploading, 5);
        step(0, crate::application::UploadPhase::Finished, 10);
        step(1, crate::application::UploadPhase::Uploading, 5);
        step(1, crate::application::UploadPhase::Finished, 10);
        assert_eq!(
            seen.lock().unwrap().clone(),
            vec![(25, false), (50, false), (75, false), (100, true)]
        );
    }

    #[test]
    fn progress_is_persisted_in_five_point_steps_and_never_backwards() {
        use super::progress_worth_persisting;
        // 逐块上报 0..=99：只有跨过 5 个百分点才落库
        let mut last = 0;
        let mut written = Vec::new();
        for next in 0..=99 {
            if progress_worth_persisting(last, next) {
                written.push(next);
                last = next;
            }
        }
        assert_eq!(
            written,
            vec![
                5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 99
            ]
        );
        // 大跨步照写；完成必写；回退与重复不写
        assert!(progress_worth_persisting(0, 50));
        assert!(progress_worth_persisting(99, 100));
        assert!(progress_worth_persisting(97, 99));
        assert!(!progress_worth_persisting(50, 40));
        assert!(!progress_worth_persisting(99, 99));
    }

    #[test]
    fn a_pending_file_shows_its_name_size_and_type_before_upload() {
        let mut message = bare_message(
            ContentBuilder::file(
                "data:application/pdf;name=%E6%8A%A5%E5%91%8A%20v2.pdf;size=1234;base64,AAAA",
            )
            .build(),
        );
        attach_local_media_preview(&mut message);
        let file = file_of(&message);
        assert_eq!(file.file_name, "报告 v2.pdf");
        assert_eq!(file.file_size, 1234);
        assert_eq!(file.mime_type, "application/pdf");
    }

    #[test]
    fn a_data_url_without_a_declared_size_is_measured() {
        // "hello" → aGVsbG8=（5 字节）
        let mut message = bare_message(
            ContentBuilder::file("data:text/plain;name=a.txt;base64,aGVsbG8=").build(),
        );
        attach_local_media_preview(&mut message);
        let file = file_of(&message);
        assert_eq!(file.file_size, 5);
        assert_eq!(file.file_name, "a.txt");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn a_local_path_gives_the_real_name_and_size() {
        let dir = std::env::temp_dir().join(format!("flare-local-facts-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("季度报表.xlsx");
        std::fs::write(&path, [0u8; 10]).unwrap();
        let mut message = bare_message(ContentBuilder::file(path.to_str().unwrap()).build());
        attach_local_media_preview(&mut message);
        let file = file_of(&message);
        assert_eq!(file.file_name, "季度报表.xlsx");
        assert_eq!(file.file_size, 10);
        assert_eq!(
            file.mime_type,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn declared_metadata_is_not_overwritten() {
        let mut message = local_file_message("data:image/png;name=other.png;size=9;base64,AAAA");
        attach_local_media_preview(&mut message);
        let file = file_of(&message);
        assert_eq!(file.file_name, "demo.png");
        assert_eq!(file.file_size, 100);
    }

    #[test]
    fn a_pending_image_and_video_carry_size_and_type() {
        let mut image = bare_message(
            ContentBuilder::image("data:image/png;name=p.png;size=77;base64,AAAA").build(),
        );
        attach_local_media_preview(&mut image);
        let Some(Elem::Image(image)) = image.content.as_ref() else {
            panic!("expected image")
        };
        let source = image.source.as_ref().expect("source");
        assert_eq!((source.size, source.mime_type.as_str()), (77, "image/png"));

        let mut video = bare_message(
            ContentBuilder::video("data:video/mp4;name=v.mp4;size=88;base64,AAAA").build(),
        );
        attach_local_media_preview(&mut video);
        let Some(Elem::Video(video)) = video.content.as_ref() else {
            panic!("expected video")
        };
        let source = video.source.as_ref().expect("source");
        assert_eq!((source.size, source.mime_type.as_str()), (88, "video/mp4"));
    }

    /// 写一个本测试独占的临时文件（并行测试互不踩）。
    #[cfg(not(target_arch = "wasm32"))]
    fn temp_media_file(test: &str, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "flare-local-duration-{test}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    fn video_of(message: &IMMessage) -> &crate::content::message_elem::VideoElem {
        match message.content.as_ref().expect("content") {
            Elem::Video(video) => video,
            other => panic!("expected video, got {other:?}"),
        }
    }

    /// 宿主按本地路径建视频消息不带时长（Flutter `createMedia(path, 'video')`），
    /// 以前各端气泡都是 00:00：上屏的乐观消息和上传后真正发出去的消息都要带容器时长，
    /// 且落在各端读的 `content.source.durationMs`（毫秒）。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_local_mp4_video_built_without_duration_goes_out_with_its_container_duration() {
        use crate::application::MessageBuilderService;
        let path = temp_media_file(
            "video-send",
            "clip.mp4",
            &crate::shared::util::media_duration::mp4::tiny_mp4(1000, 4000),
        );
        let message = MessageBuilderService::build_video(
            "conv-media",
            "hugo",
            path.to_str().unwrap(),
            Some("peer-1"),
        )
        .expect("video message");
        assert!(
            video_of(&message).source.is_none(),
            "builder carries no duration"
        );
        let client_msg_id = message.client_msg_id.clone();
        let harness = TestHarness::new(MediaResult::Uploaded(UploadedMedia {
            file_id: "remote-video-1".to_string(),
            file_name: "clip.mp4".to_string(),
            mime_type: "video/mp4".to_string(),
            size: 208,
            url: None,
            cdn_url: None,
        }));

        let send_task = harness.usecase.send_with_media(message, None);
        tokio::pin!(send_task);
        tokio::select! {
            _ = harness.media.wait_until_upload_started() => {}
            result = &mut send_task => panic!("send finished before upload could be inspected: {result:?}"),
        }
        let optimistic = harness
            .messages
            .get_by_client_msg_id(&client_msg_id)
            .await
            .expect("store read")
            .expect("optimistic message");
        let source = video_of(&optimistic)
            .source
            .as_ref()
            .expect("preview source");
        assert_eq!(source.duration_ms, 4000, "uploading bubble shows 0:04");

        harness.media.finish_upload().await;
        send_task.await.expect("send");
        let queued = harness.queue.enqueued().await;
        assert_eq!(queued.len(), 1);
        let video = video_of(&queued[0]);
        assert_eq!(video.video_id, "remote-video-1");
        assert_eq!(
            video.source.as_ref().expect("uploaded source").duration_ms,
            4000,
            "the upload result must not drop the duration read before upload"
        );
        let wire = serde_json::to_value(queued[0].content.as_ref().unwrap()).unwrap();
        assert_eq!(wire["source"]["durationMs"], 4000);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn a_local_m4a_voice_gets_its_container_duration() {
        let path = temp_media_file(
            "audio",
            "voice.m4a",
            &crate::shared::util::media_duration::mp4::tiny_mp4(44_100, 44_100 * 7),
        );
        let mut message = bare_message(ContentBuilder::audio(path.to_str().unwrap()).build());
        attach_local_media_preview(&mut message);
        let Some(Elem::Audio(audio)) = message.content.as_ref() else {
            panic!("expected audio")
        };
        assert_eq!(audio.source.as_ref().expect("source").duration_ms, 7000);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    fn audio_of(message: &IMMessage) -> &crate::content::message_elem::AudioElem {
        match message.content.as_ref().expect("content") {
            Elem::Audio(audio) => audio,
            other => panic!("expected audio, got {other:?}"),
        }
    }

    /// kit 录的语音是 16kHz 单声道 pcm16 `.wav`，宿主只给路径不给时长：
    /// 上屏和上传后发出去的消息都要带从 WAV 头算出来的 `content.source.durationMs`。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_local_wav_voice_built_without_duration_goes_out_with_its_duration() {
        use crate::application::MessageBuilderService;
        let path = temp_media_file(
            "voice-send",
            "flare-voice.wav",
            &crate::shared::util::media_duration::wav::tiny_wav(32_000, 80_000),
        );
        let message = MessageBuilderService::build_audio(
            "conv-media",
            "hugo",
            path.to_str().unwrap(),
            Some("peer-1"),
        )
        .expect("audio message");
        assert!(
            audio_of(&message).source.is_none(),
            "builder carries no duration"
        );
        let client_msg_id = message.client_msg_id.clone();
        let harness = TestHarness::new(MediaResult::Uploaded(UploadedMedia {
            file_id: "remote-voice-1".to_string(),
            file_name: "flare-voice.wav".to_string(),
            mime_type: "audio/wav".to_string(),
            size: 80_044,
            url: None,
            cdn_url: None,
        }));

        let send_task = harness.usecase.send_with_media(message, None);
        tokio::pin!(send_task);
        tokio::select! {
            _ = harness.media.wait_until_upload_started() => {}
            result = &mut send_task => panic!("send finished before upload could be inspected: {result:?}"),
        }
        let optimistic = harness
            .messages
            .get_by_client_msg_id(&client_msg_id)
            .await
            .expect("store read")
            .expect("optimistic message");
        assert_eq!(
            audio_of(&optimistic).source.as_ref().unwrap().duration_ms,
            2500,
            "uploading voice bubble shows 2.5s"
        );

        harness.media.finish_upload().await;
        send_task.await.expect("send");
        let queued = harness.queue.enqueued().await;
        assert_eq!(queued.len(), 1);
        let audio = audio_of(&queued[0]);
        assert_eq!(audio.audio_id, "remote-voice-1");
        assert_eq!(audio.source.as_ref().unwrap().duration_ms, 2500);
        let wire = serde_json::to_value(queued[0].content.as_ref().unwrap()).unwrap();
        assert_eq!(wire["source"]["durationMs"], 2500);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn a_declared_duration_is_kept_and_an_unreadable_file_leaves_zero() {
        let mp4 = temp_media_file(
            "declared",
            "clip.mp4",
            &crate::shared::util::media_duration::mp4::tiny_mp4(1000, 4000),
        );
        let mut declared = bare_message(
            ContentBuilder::video(mp4.to_str().unwrap())
                .video_source(flare_proto::common::VideoInfo {
                    uuid: mp4.to_str().unwrap().to_string(),
                    duration_ms: 1234,
                    ..Default::default()
                })
                .build(),
        );
        attach_local_media_preview(&mut declared);
        assert_eq!(
            video_of(&declared).source.as_ref().unwrap().duration_ms,
            1234
        );

        let webm = temp_media_file(
            "declared",
            "clip.webm",
            &[0x1A, 0x45, 0xDF, 0xA3, 0x9F, 0x42, 0x86, 0x81],
        );
        let mut unreadable = bare_message(ContentBuilder::video(webm.to_str().unwrap()).build());
        attach_local_media_preview(&mut unreadable);
        let source = video_of(&unreadable).source.as_ref().unwrap();
        assert_eq!(source.duration_ms, 0);
        assert_eq!(source.url, webm.to_str().unwrap(), "still previewable");
        std::fs::remove_dir_all(mp4.parent().unwrap()).ok();
    }

    fn local_file_message(path: &str) -> IMMessage {
        let message = MessageBuilder::new("conv-media", "hugo")
            .single_chat()
            .channel("peer-1")
            .content(
                ContentBuilder::file(path)
                    .file_name("demo.png")
                    .mime_type("image/png")
                    .file_size(100)
                    .build(),
            )
            .build()
            .expect("message");
        IMMessage::new(message)
    }

    fn local_image_group_message(paths: &[&str]) -> IMMessage {
        let images = paths
            .iter()
            .map(|path| ImageInfo {
                uuid: (*path).to_string(),
                image_id: (*path).to_string(),
                url: String::new(),
                mime_type: "image/png".to_string(),
                size: 100,
                width: 0,
                height: 0,
                format: ImageFormat::Png as i32,
                animated: false,
                blurhash: String::new(),
            })
            .collect();
        let message = MessageBuilder::new("conv-media", "hugo")
            .single_chat()
            .channel("peer-1")
            .content(
                ContentBuilder::image_group_with_details(images, "album", Default::default())
                    .build(),
            )
            .build()
            .expect("message");
        IMMessage::new(message)
    }

    #[test]
    fn extract_media_source_accepts_filesystem_inputs() {
        assert_eq!(
            extract_media_source("/tmp/demo.png").map(|source| source.locator),
            Some("/tmp/demo.png".to_string())
        );
        assert_eq!(
            extract_media_source("./demo.png").map(|source| source.locator),
            Some("./demo.png".to_string())
        );
        assert_eq!(
            extract_media_source("file:///tmp/demo.png").map(|source| source.locator),
            Some("/tmp/demo.png".to_string())
        );
        assert_eq!(
            extract_media_source(r"C:\data\demo.png").map(|source| source.locator),
            Some(r"C:\data\demo.png".to_string())
        );
        assert_eq!(
            extract_media_source("file:///C:/data/demo.png").map(|source| source.locator),
            Some("C:/data/demo.png".to_string())
        );
    }

    #[test]
    fn extract_media_source_accepts_host_platform_inputs() {
        assert!(matches!(
            extract_media_source("content://media/external/images/1").map(|source| source.kind),
            Some(MediaSourceKind::Uri)
        ));
        assert!(matches!(
            extract_media_source("ph://asset-id").map(|source| source.kind),
            Some(MediaSourceKind::Asset)
        ));
        assert!(matches!(
            extract_media_source("blob:https://example.com/id").map(|source| source.kind),
            Some(MediaSourceKind::Blob)
        ));
        assert!(matches!(
            extract_media_source("data:image/png;base64,AAA").map(|source| source.kind),
            Some(MediaSourceKind::Bytes)
        ));
    }

    #[test]
    fn extract_media_source_rejects_remote_and_empty_inputs() {
        assert!(extract_media_source("").is_none());
        assert!(extract_media_source("https://example.com/demo.png").is_none());
        assert!(extract_media_source("media-file-id").is_none());
    }

    #[test]
    fn data_media_source_builds_payload_and_metadata_for_web_upload() {
        let source =
            extract_media_source("data:image/png;name=real%20image.png;size=3;base64,QUJD")
                .expect("data source");

        let media = processed_media_from_source(source).expect("processed media");

        assert_eq!(media.payload.as_deref(), Some(&b"ABC"[..]));
        assert_eq!(media.metadata.file_name, "real image.png");
        assert_eq!(media.metadata.mime_type, "image/png");
        assert_eq!(media.metadata.size, 3);
    }

    #[test]
    fn uploaded_image_descriptor_keeps_stable_metadata() {
        let uploaded = UploadedMedia {
            file_id: "img-1".to_string(),
            file_name: "demo.png".to_string(),
            mime_type: "image/png".to_string(),
            size: 123,
            url: Some("https://origin.example.com/demo.png".to_string()),
            cdn_url: Some("https://cdn.example.com/demo.png".to_string()),
        };

        let source = uploaded_media_to_image_descriptor(&uploaded).expect("image descriptor");

        assert_eq!(source.uuid, "img-1");
        assert_eq!(source.image_id, "img-1");
        assert_eq!(source.url, "");
        assert_eq!(source.mime_type, "image/png");
        assert_eq!(source.size, 123);
        assert_eq!(source.format, ImageFormat::Png as i32);
        assert!(!source.animated);
    }

    /// An uploaded file goes out as its id alone: the upload response's `url` expires (presigned) and its `cdn_url`
    /// is unsigned, so neither may be stamped into a message everyone keeps. Readers resolve the id.
    #[test]
    fn uploaded_media_goes_out_as_its_id_not_the_upload_response_address() {
        let uploaded = UploadedMedia {
            file_id: "img-1".to_string(),
            file_name: "demo.png".to_string(),
            mime_type: "image/png".to_string(),
            size: 123,
            url: Some("https://origin.example.com/demo.png?X-Amz-Signature=abc".to_string()),
            cdn_url: Some("https://cdn.example.com/media/demo.png".to_string()),
        };

        let source = uploaded_media_to_image_descriptor(&uploaded).expect("image descriptor");

        assert_eq!(source.image_id, "img-1");
        assert_eq!(source.url, "");
        assert_eq!(uploaded_media_display_url(&uploaded), "");
    }

    #[test]
    fn uploaded_media_display_url_ignores_local_upload_locator() {
        let uploaded = UploadedMedia {
            file_id: "file-1".to_string(),
            file_name: "demo.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: 12,
            url: Some("/tmp/demo.txt".to_string()),
            cdn_url: None,
        };

        assert_eq!(uploaded_media_display_url(&uploaded), "");
    }

    #[test]
    fn attach_local_media_preview_keeps_optimistic_sources_renderable() {
        let mut file = local_file_message("/tmp/demo.png");
        attach_local_media_preview(&mut file);
        let Elem::File(file_content) = file.content.as_ref().expect("file content") else {
            panic!("expected file content")
        };
        assert_eq!(file_content.file_id, "/tmp/demo.png");
        assert_eq!(file_content.url, "/tmp/demo.png");

        let mut group = local_image_group_message(&["/tmp/one.png", "/tmp/two.png"]);
        attach_local_media_preview(&mut group);
        let Elem::ImageGroup(group_content) = group.content.as_ref().expect("image group") else {
            panic!("expected image group")
        };
        assert_eq!(group_content.images[0].image_id, "/tmp/one.png");
        assert_eq!(group_content.images[0].url, "/tmp/one.png");
        assert_eq!(group_content.images[1].url, "/tmp/two.png");
    }

    /// 上传进度必须写回 message store（进而经总线下发给各端渲染），
    /// 并且同时透传给调用方传入的外部回调。
    ///
    /// 这条链路此前没有任何测试：`upload_progress_callback` 在后台任务里更新消息，
    /// 一旦它断了，UI 只会看到一个不动的「上传中」，而没有任何报错。
    #[tokio::test]
    async fn upload_progress_is_written_back_to_the_stored_message() {
        let uploaded = UploadedMedia {
            file_id: "remote-file-2".to_string(),
            file_name: "remote-demo.png".to_string(),
            mime_type: "image/png".to_string(),
            size: 100,
            url: Some("https://origin.example/demo.png".to_string()),
            cdn_url: Some("https://cdn.example/demo.png".to_string()),
        };
        let harness = TestHarness::new(MediaResult::Uploaded(uploaded));
        let message = local_file_message("/tmp/demo.png");
        let client_msg_id = message.client_msg_id.clone();

        // 外部回调：验证调用方也能拿到进度（原生端据此做自定义 UI）
        let seen: Arc<std::sync::Mutex<Vec<u64>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_for_cb = seen.clone();
        let external: crate::application::UploadProgressCallback =
            Arc::new(move |progress: crate::application::UploadProgress| {
                seen_for_cb
                    .lock()
                    .expect("lock")
                    .push(progress.uploaded_bytes);
            });

        let send_task = harness.usecase.send_with_media(message, Some(external));
        tokio::pin!(send_task);
        tokio::select! {
            _ = harness.media.wait_until_upload_started() => {}
            result = &mut send_task => panic!("upload 尚未开始 send 就返回了: {result:?}"),
        }

        // 进度写回发生在后台任务里，轮询等待而不是硬 sleep
        let mut mid_progress = 0;
        for _ in 0..200 {
            if let Ok(Some(stored)) = harness.messages.get_by_client_msg_id(&client_msg_id).await
                && stored.local_state.upload_progress > 0
            {
                mid_progress = stored.local_state.upload_progress;
                assert!(
                    stored.local_state.uploading,
                    "上传中期间 uploading 必须为真"
                );
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            (1..=99).contains(&mid_progress),
            "上传中期的进度应落在 1..=99（实际 {mid_progress}）：未完成前不能显示 100%"
        );

        harness.media.finish_upload().await;
        send_task.await.expect("send");

        let stored = harness
            .messages
            .get_by_client_msg_id(&client_msg_id)
            .await
            .expect("store read")
            .expect("message");
        assert!(
            !stored.local_state.uploading,
            "上传结束后 uploading 必须归假"
        );
        assert_eq!(stored.local_state.upload_progress, 100);
        assert!(
            stored.local_state.sending,
            "上传完成后进入发送中，直到 ack 才落定"
        );

        let observed = seen.lock().expect("lock").clone();
        assert!(
            observed.contains(&50),
            "外部进度回调没有收到中间进度（实际 {observed:?}）"
        );
    }

    #[tokio::test]
    async fn send_with_media_persists_uploading_message_before_upload_finishes() {
        let uploaded = UploadedMedia {
            file_id: "remote-file-1".to_string(),
            file_name: "remote-demo.png".to_string(),
            mime_type: "image/png".to_string(),
            size: 100,
            url: Some("https://origin.example/demo.png".to_string()),
            cdn_url: Some("https://cdn.example/demo.png".to_string()),
        };
        let harness = TestHarness::new(MediaResult::Uploaded(uploaded));
        let message = local_file_message("/tmp/demo.png");
        let client_msg_id = message.client_msg_id.clone();
        let mut events = harness.usecase.bus.subscribe();

        let send_task = harness.usecase.send_with_media(message, None);
        tokio::pin!(send_task);
        tokio::select! {
            _ = harness.media.wait_until_upload_started() => {}
            result = &mut send_task => panic!("send finished before upload could be inspected: {result:?}"),
        }

        let stored = harness
            .messages
            .get_by_client_msg_id(&client_msg_id)
            .await
            .expect("store read")
            .expect("optimistic uploading message");
        assert_eq!(stored.client_msg_id, client_msg_id);
        assert_eq!(stored.status, MessageStatus::Created as i32);
        assert!(stored.local_state.is_local);
        assert!(stored.local_state.sending);
        assert!(stored.local_state.uploading);
        assert!(stored.local_state.upload_progress <= 99);
        let Elem::File(file) = stored.content.as_ref().expect("file content") else {
            panic!("expected file content")
        };
        assert_eq!(file.file_id, "/tmp/demo.png");
        assert_eq!(file.url, "/tmp/demo.png");

        harness.media.finish_upload().await;
        let ack = send_task.await.expect("send");
        assert_eq!(ack.client_msg_id, client_msg_id);

        let queued = harness.queue.enqueued().await;
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].client_msg_id, client_msg_id);
        let Elem::File(file) = queued[0].content.as_ref().expect("file content") else {
            panic!("expected queued file content")
        };
        assert_eq!(file.file_id, "remote-file-1");
        assert_eq!(file.file_name, "remote-demo.png");
        assert!(
            file.url.is_empty(),
            "an uploaded file goes out as its id alone, readers resolve it (got {:?})",
            file.url
        );

        let final_local = harness
            .messages
            .get_by_client_msg_id(&client_msg_id)
            .await
            .expect("store read")
            .expect("final local message");
        assert!(!final_local.local_state.failed);
        assert!(!final_local.local_state.uploading);
        assert_eq!(final_local.local_state.upload_progress, 100);

        let mut received_updates = 0;
        while let Ok(event) = events.try_recv() {
            if let SdkEvent::Message(MessageEvent::ReceivedBatch { messages }) = event
                && messages
                    .iter()
                    .any(|message| message.client_msg_id == client_msg_id)
            {
                received_updates += 1;
            }
        }
        assert!(
            received_updates >= 2,
            "uploading and uploaded states should be emitted as timeline deltas"
        );
    }

    #[tokio::test]
    async fn send_with_media_marks_same_optimistic_message_failed_when_upload_fails() {
        let harness = TestHarness::new(MediaResult::Failed);
        let message = local_file_message("/tmp/fail.png");
        let client_msg_id = message.client_msg_id.clone();

        let send_task = harness.usecase.send_with_media(message, None);
        tokio::pin!(send_task);
        tokio::select! {
            _ = harness.media.wait_until_upload_started() => {}
            result = &mut send_task => panic!("send finished before upload could be inspected: {result:?}"),
        }
        harness.media.finish_upload().await;

        let err = send_task.await.expect_err("upload should fail");
        assert!(err.to_string().contains("controlled upload failed"));
        assert!(harness.queue.enqueued().await.is_empty());

        let stored = harness
            .messages
            .get_by_client_msg_id(&client_msg_id)
            .await
            .expect("store read")
            .expect("failed optimistic message");
        assert_eq!(stored.client_msg_id, client_msg_id);
        assert_eq!(stored.status, MessageStatus::Failed as i32);
        assert!(stored.local_state.failed);
        assert!(!stored.local_state.sending);
        assert!(!stored.local_state.uploading);
        let Elem::File(file) = stored.content.as_ref().expect("file content") else {
            panic!("expected file content")
        };
        assert_eq!(file.file_id, "/tmp/fail.png");
        assert_eq!(file.url, "/tmp/fail.png");
    }
}
