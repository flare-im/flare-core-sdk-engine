use super::{
    Dispatcher, EventPayload, detect_outgoing_seq_regressions, first_gap_after,
    first_internal_gap_after, first_internal_gap_after_from, is_waterline_ping, max_contiguous_seq,
};
use super::{
    SEQ_REPAIR_IDLE_TTL_MS, SEQ_REPAIR_MAX_BACKOFF_MS, SEQ_REPAIR_MAX_TRACKED_CONVERSATIONS,
    SeqRepairState, WATERLINE_ATTR_CONVERSATION_ID_CAMEL, WATERLINE_ATTR_KIND,
    WATERLINE_ATTR_MAX_SEQ_CAMEL, attr_non_empty, attr_u64, prune_seq_repair_state,
    seq_repair_backoff_ms, seq_repair_gap_after,
};
use crate::application::notification::{NotificationHandlerRegistry, NotificationInboundPipeline};
use crate::application::services::EventDeduper;
use crate::application::services::MessageDeduper;
use crate::application::usecases::SyncApplyUseCase;
use crate::domain::{
    ConversationReader, ConversationWriter, MessageReader, MessageStore, MessageWriter,
    PendingSendReader, PendingSendVo, PendingSendWriter, SyncCursorReader, SyncCursorVo,
    SyncCursorWriter,
};
use crate::infrastructure::persistence::StoreProvider;
use crate::infrastructure::protocol::DownlinkPayload;
use crate::infrastructure::protocol::{Codec, PacketSender, ProtobufCodec};
use crate::kernel::event::{EventBus, MessageEvent, SdkEvent};
use crate::kernel::{CurrentUserIdStore, SessionSyncRunner};
use crate::model::IMMessage;
use crate::runtime::{ReliableSendQueue, ReliableSendQueueConfig};
use crate::shared::error::Result;
use crate::spi::metrics::MetricsRecorder;
use async_trait::async_trait;
use flare_proto::common::event::Payload as ProtoEventPayload;
use flare_proto::common::{
    ConversationUpdateEvent, Event, EventEnvelope, MessageDeleteEvent, MessageRecallEvent,
    SendAccepted, SendAck, SendAckDurability, send_ack,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::time::{Duration, timeout};

fn test_notification_pipeline(bus: EventBus) -> NotificationInboundPipeline {
    test_notification_pipeline_with_deduper(bus, MessageDeduper::new(Some(64)))
}

fn test_notification_pipeline_with_deduper(
    bus: EventBus,
    message_deduper: MessageDeduper,
) -> NotificationInboundPipeline {
    NotificationInboundPipeline::new(
        Arc::new(NotificationHandlerRegistry::new()),
        message_deduper,
        bus,
    )
}

fn accepted_ack(
    client_msg_id: &str,
    conversation_id: &str,
    server_msg_id: &str,
    conversation_seq: u64,
) -> SendAck {
    SendAck {
        client_msg_id: client_msg_id.to_string(),
        conversation_id: conversation_id.to_string(),
        result: Some(send_ack::Result::Accepted(SendAccepted {
            server_msg_id: server_msg_id.to_string(),
            conversation_seq,
            server_time: 0,
            durability: SendAckDurability::Persisted as i32,
        })),
        ..Default::default()
    }
}

fn accepted_server_msg_id(ack: &SendAck) -> Option<&str> {
    match ack.result.as_ref() {
        Some(send_ack::Result::Accepted(accepted)) => Some(accepted.server_msg_id.as_str()),
        _ => None,
    }
}

struct RecordingSessionSyncRunner {
    message_sync_calls: Mutex<Vec<String>>,
    notify: Notify,
}

impl RecordingSessionSyncRunner {
    fn new() -> Self {
        Self {
            message_sync_calls: Mutex::new(Vec::new()),
            notify: Notify::new(),
        }
    }

    async fn message_sync_calls(&self) -> Vec<String> {
        self.message_sync_calls.lock().await.clone()
    }
}

struct BlockingSessionSyncRunner {
    message_sync_calls: Mutex<Vec<String>>,
    notify: Notify,
    release_first: Notify,
}

impl BlockingSessionSyncRunner {
    fn new() -> Self {
        Self {
            message_sync_calls: Mutex::new(Vec::new()),
            notify: Notify::new(),
            release_first: Notify::new(),
        }
    }

    async fn message_sync_calls(&self) -> Vec<String> {
        self.message_sync_calls.lock().await.clone()
    }
}

impl SessionSyncRunner for BlockingSessionSyncRunner {
    fn request_message_sync(
        &self,
        conversation_id: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            let call_index = {
                let mut calls = self.message_sync_calls.lock().await;
                calls.push(conversation_id);
                calls.len()
            };
            self.notify.notify_one();
            if call_index == 1 {
                self.release_first.notified().await;
            }
            Ok(())
        })
    }

    fn request_message_sync_from_seq(
        &self,
        conversation_id: &str,
        last_seq: u64,
        _limit: i32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let call = format!("{conversation_id}:{last_seq}");
        Box::pin(async move {
            self.message_sync_calls.lock().await.push(call);
            self.notify.notify_one();
            Ok(())
        })
    }

    fn send_read_ack(
        &self,
        _conversation_id: &str,
        _read_seq: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn request_participants_sync(
        &self,
        _conversation_id: &str,
        _limit: i32,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<crate::model::ConversationParticipant>>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl SessionSyncRunner for RecordingSessionSyncRunner {
    fn request_message_sync(
        &self,
        conversation_id: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let conversation_id = conversation_id.to_string();
        Box::pin(async move {
            self.message_sync_calls.lock().await.push(conversation_id);
            self.notify.notify_one();
            Ok(())
        })
    }

    fn request_message_sync_from_seq(
        &self,
        conversation_id: &str,
        last_seq: u64,
        _limit: i32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        let call = format!("{conversation_id}:{last_seq}");
        Box::pin(async move {
            self.message_sync_calls.lock().await.push(call);
            self.notify.notify_one();
            Ok(())
        })
    }

    fn send_read_ack(
        &self,
        _conversation_id: &str,
        _read_seq: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    fn request_participants_sync(
        &self,
        _conversation_id: &str,
        _limit: i32,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<crate::model::ConversationParticipant>>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct MemoryPendingSendStore {
    data: Mutex<Vec<PendingSendVo>>,
}

impl MemoryPendingSendStore {
    fn new() -> Self {
        Self {
            data: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl PendingSendReader for MemoryPendingSendStore {
    async fn get(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>> {
        let data = self.data.lock().await;
        Ok(data
            .iter()
            .find(|entry| entry.client_msg_id == client_msg_id)
            .cloned())
    }

    async fn list(&self) -> Result<Vec<PendingSendVo>> {
        Ok(self.data.lock().await.clone())
    }
}

#[async_trait]
impl PendingSendWriter for MemoryPendingSendStore {
    async fn push(&self, entry: PendingSendVo) -> Result<()> {
        self.data.lock().await.push(entry);
        Ok(())
    }

    async fn pop(&self, client_msg_id: &str) -> Result<Option<PendingSendVo>> {
        let mut data = self.data.lock().await;
        let pos = data
            .iter()
            .position(|entry| entry.client_msg_id == client_msg_id);
        Ok(pos.map(|index| data.remove(index)))
    }
}

struct MemoryMessageStore {
    data: RwLock<HashMap<String, IMMessage>>,
    fail_saves: bool,
}

impl MemoryMessageStore {
    fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            fail_saves: false,
        }
    }

    /// 模拟持久化失败的消息存储：用于验证「先投递后持久化」在 save_batch 失败时仍投递视图。
    fn failing() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            fail_saves: true,
        }
    }
}

#[async_trait]
impl MessageReader for MemoryMessageStore {
    async fn get(&self, message_id: &str) -> Result<Option<IMMessage>> {
        Ok(self.data.read().await.get(message_id).cloned())
    }

    async fn get_by_client_msg_id(&self, client_msg_id: &str) -> Result<Option<IMMessage>> {
        Ok(self
            .data
            .read()
            .await
            .values()
            .find(|message| message.client_msg_id == client_msg_id)
            .cloned())
    }

    async fn get_by_conversation(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        let bound = if before_seq == 0 {
            u64::MAX
        } else {
            before_seq
        };
        let mut messages = self
            .data
            .read()
            .await
            .values()
            .filter(|message| {
                message.conversation_id == conversation_id
                    && message.conversation_seq > 0
                    && message.conversation_seq < bound
            })
            .cloned()
            .collect::<Vec<_>>();
        messages.sort_by(|left, right| {
            right
                .conversation_seq
                .cmp(&left.conversation_seq)
                .then_with(|| right.created_at.cmp(&left.created_at))
        });
        messages.truncate(limit as usize);
        Ok(messages)
    }

    async fn search(&self, _keyword: &str, _limit: u32) -> Result<Vec<IMMessage>> {
        Ok(Vec::new())
    }

    async fn search_in_conversation(
        &self,
        _conversation_id: &str,
        _keyword: &str,
        _limit: u32,
    ) -> Result<Vec<IMMessage>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl MessageWriter for MemoryMessageStore {
    async fn save_batch(&self, messages: &[IMMessage]) -> Result<()> {
        if self.fail_saves {
            return Err(crate::shared::error::FlareError::general_error(
                "simulated persist failure",
            ));
        }
        let mut data = self.data.write().await;
        for message in messages {
            let key = if !message.server_id.is_empty() {
                message.server_id.clone()
            } else {
                message.client_msg_id.clone()
            };
            data.insert(key, message.clone());
        }
        Ok(())
    }

    async fn save_one(&self, message: &IMMessage) -> Result<()> {
        self.save_batch(std::slice::from_ref(message)).await
    }

    async fn update_status(&self, message_id: &str, status: i32) -> Result<u64> {
        if let Some(message) = self.data.write().await.get_mut(message_id) {
            message.status = status;
            return Ok(1);
        }
        Ok(0)
    }

    async fn update_content(&self, _message_id: &str, _new_content: Vec<u8>) -> Result<bool> {
        Ok(false)
    }

    async fn delete(&self, message_id: &str) -> Result<()> {
        self.data.write().await.remove(message_id);
        Ok(())
    }

    async fn rewrite_conversation_id(
        &self,
        from_conversation_id: &str,
        to_conversation_id: &str,
    ) -> Result<u64> {
        let from = from_conversation_id.trim();
        let to = to_conversation_id.trim();
        if from.is_empty() || to.is_empty() || from == to {
            return Ok(0);
        }
        let mut count = 0;
        for message in self.data.write().await.values_mut() {
            if message.conversation_id == from {
                message.conversation_id = to.to_string();
                count += 1;
            }
        }
        Ok(count)
    }

    async fn update_after_ack(&self, client_msg_id: &str, message: &IMMessage) -> Result<()> {
        let mut data = self.data.write().await;
        data.remove(client_msg_id);
        data.insert(message.server_id.clone(), message.clone());
        Ok(())
    }
}

impl MessageStore for MemoryMessageStore {}

struct NoopConversationStore;
struct NoopSyncCursorStore;
struct CursorAtSeqStore {
    last_seq: u64,
}

#[async_trait]
impl ConversationReader for NoopConversationStore {
    async fn get(&self, _conversation_id: &str) -> Result<Option<crate::model::Conversation>> {
        Ok(None)
    }

    async fn list(&self) -> Result<Vec<crate::model::Conversation>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl ConversationWriter for NoopConversationStore {
    async fn save_batch(&self, _conversations: &[crate::model::Conversation]) -> Result<()> {
        Ok(())
    }

    async fn save_one(&self, _conversation: &crate::model::Conversation) -> Result<()> {
        Ok(())
    }

    async fn update_unread(
        &self,
        _conversation_id: &str,
        _unread_count: u32,
        _last_read_seq: u64,
    ) -> Result<()> {
        Ok(())
    }

    async fn set_pinned(&self, _conversation_id: &str, _pinned: bool) -> Result<()> {
        Ok(())
    }

    async fn set_muted(&self, _conversation_id: &str, _muted: bool) -> Result<()> {
        Ok(())
    }

    async fn set_archived(&self, _conversation_id: &str, _archived: bool) -> Result<()> {
        Ok(())
    }

    async fn mark_unread(&self, _conversation_id: &str) -> Result<u32> {
        Ok(1)
    }

    async fn update_draft(&self, _conversation_id: &str, _draft: Option<&str>) -> Result<()> {
        Ok(())
    }

    async fn delete(&self, _conversation_id: &str) -> Result<()> {
        Ok(())
    }

    async fn merge_conversation_identity(
        &self,
        _from_conversation_id: &str,
        _to_conversation_id: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn clear_local_chat_history(
        &self,
        _conversation_id: &str,
        _cleared_through_seq: u64,
    ) -> Result<()> {
        Ok(())
    }

    async fn update_last_message(
        &self,
        _conversation_id: &str,
        _last_message_id: &str,
        _last_sender_id: &str,
        _last_message_at: u64,
        _last_message_preview: Option<&str>,
        _max_seq: u64,
    ) -> Result<()> {
        Ok(())
    }

    async fn recompute_unread_for_user(
        &self,
        _conversation_id: &str,
        _current_user_id: &str,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl SyncCursorReader for NoopSyncCursorStore {
    async fn get_conversation_cursor(
        &self,
        _user_id: &str,
        _conversation_id: &str,
    ) -> Result<Option<SyncCursorVo>> {
        Ok(None)
    }

    async fn get_raw(&self, _key: &str) -> Result<Option<String>> {
        Ok(None)
    }
}

#[async_trait]
impl SyncCursorWriter for NoopSyncCursorStore {
    async fn save_conversation_cursor(&self, _cursor: &SyncCursorVo) -> Result<()> {
        Ok(())
    }

    async fn save_raw(&self, _key: &str, _cursor: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl SyncCursorReader for CursorAtSeqStore {
    async fn get_conversation_cursor(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<Option<SyncCursorVo>> {
        Ok(Some(SyncCursorVo {
            user_id: user_id.to_string(),
            conversation_id: conversation_id.to_string(),
            last_seq: self.last_seq,
            synced_at: 0,
        }))
    }

    async fn get_raw(&self, _key: &str) -> Result<Option<String>> {
        Ok(None)
    }
}

#[async_trait]
impl SyncCursorWriter for CursorAtSeqStore {
    async fn save_conversation_cursor(&self, _cursor: &SyncCursorVo) -> Result<()> {
        Ok(())
    }

    async fn save_raw(&self, _key: &str, _cursor: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn send_ack_is_published_once_when_reliable_queue_enabled() {
    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let pending_store = Arc::new(MemoryPendingSendStore::new());
    let message_store = Arc::new(MemoryMessageStore::new());
    let sender = Arc::new(PacketSender::new(
        Arc::new(Mutex::new(None)),
        Arc::new(ProtobufCodec) as Arc<dyn Codec>,
    ));
    let reliable_queue = Arc::new(ReliableSendQueue::new(ReliableSendQueueConfig {
        pending_reader: pending_store.clone(),
        pending_writer: pending_store,
        sender,
        message_store,
        conversation_store: Arc::new(NoopConversationStore),
        current_user_id: current_user_id.clone(),
        bus: bus.clone(),
        timeout_secs: Some(60),
        max_retries: Some(3),
        max_in_flight: Some(32),
        metrics: MetricsRecorder::disabled(),
    }));

    let dispatcher = Dispatcher::new(
        bus.clone(),
        Some(reliable_queue.clone()),
        None,
        None,
        None,
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus.clone()),
        MetricsRecorder::disabled(),
    );

    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.client_msg_id = "client-1".to_string();
    message.conversation_id = "conv-1".to_string();
    message.sender_id = "u1".to_string();
    reliable_queue.enqueue(message).await.unwrap();

    let optimistic = timeout(Duration::from_millis(200), receiver.recv())
        .await
        .expect("expected optimistic message event")
        .expect("bus closed");
    assert!(matches!(
        optimistic,
        SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
    ));

    tokio::time::sleep(Duration::from_millis(20)).await;

    dispatcher
        .dispatch(DownlinkPayload::SendAck(accepted_ack(
            "client-1", "conv-1", "server-1", 1,
        )))
        .await
        .unwrap();

    let first = timeout(Duration::from_millis(200), receiver.recv())
        .await
        .expect("expected one send ack event")
        .expect("bus closed");

    match first {
        SdkEvent::Message(MessageEvent::SendAck { ack }) => {
            assert_eq!(ack.client_msg_id, "client-1");
            assert_eq!(accepted_server_msg_id(&ack), Some("server-1"));
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let second = timeout(Duration::from_millis(80), receiver.recv()).await;
    assert!(second.is_err(), "send ack should not be published twice");
}

#[tokio::test]
async fn self_sent_realtime_message_converges_to_send_ack_without_received() {
    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let pending_store = Arc::new(MemoryPendingSendStore::new());
    let message_store = Arc::new(MemoryMessageStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let sender = Arc::new(PacketSender::new(
        Arc::new(Mutex::new(None)),
        Arc::new(ProtobufCodec) as Arc<dyn Codec>,
    ));
    let reliable_queue = Arc::new(ReliableSendQueue::new(ReliableSendQueueConfig {
        pending_reader: pending_store.clone(),
        pending_writer: pending_store,
        sender,
        message_store: message_store.clone(),
        conversation_store: Arc::new(NoopConversationStore),
        current_user_id: current_user_id.clone(),
        bus: bus.clone(),
        timeout_secs: Some(60),
        max_retries: Some(3),
        max_in_flight: Some(32),
        metrics: MetricsRecorder::disabled(),
    }));

    let dispatcher = Dispatcher::new(
        bus.clone(),
        Some(reliable_queue.clone()),
        None,
        None,
        Some(stores),
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus.clone()),
        MetricsRecorder::disabled(),
    );

    let mut optimistic = IMMessage::new(flare_proto::common::Message::default());
    optimistic.client_msg_id = "client-self-1".to_string();
    optimistic.conversation_id = "conv-1".to_string();
    optimistic.sender_id = "u1".to_string();
    reliable_queue.enqueue(optimistic).await.unwrap();

    // enqueue surfaces the optimistic message via ReceivedBatch (asserted in
    // reliable_queue tests); drain it so the assertions below target the
    // self-echo convergence, not the optimistic insert.
    let enqueue_optimistic = timeout(Duration::from_millis(200), receiver.recv())
        .await
        .expect("expected optimistic message event")
        .expect("bus closed");
    assert!(matches!(
        enqueue_optimistic,
        SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
    ));

    tokio::time::sleep(Duration::from_millis(20)).await;

    let proto_message = flare_proto::common::Message {
        server_id: "server-self-1".to_string(),
        client_msg_id: "client-self-1".to_string(),
        conversation_id: "conv-1".to_string(),
        sender_id: "u1".to_string(),
        conversation_seq: 11,
        ..Default::default()
    };

    dispatcher
        .dispatch(DownlinkPayload::MessagePush(
            flare_proto::common::MessagePush {
                messages: vec![proto_message],
                notifications: Vec::new(),
            },
        ))
        .await
        .unwrap();

    let first = timeout(Duration::from_millis(200), receiver.recv())
        .await
        .expect("expected convergence event")
        .expect("bus closed");
    match first {
        SdkEvent::Message(MessageEvent::SendAck { ack }) => {
            assert_eq!(ack.client_msg_id, "client-self-1");
            assert_eq!(accepted_server_msg_id(&ack), Some("server-self-1"));
        }
        other => panic!("unexpected event: {other:?}"),
    }

    let second = timeout(Duration::from_millis(80), receiver.recv()).await;
    assert!(
        second.is_err(),
        "self-sent convergence should suppress Received callback"
    );
}

#[tokio::test]
async fn out_of_order_ack_is_buffered_and_applied_once() {
    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let pending_store = Arc::new(MemoryPendingSendStore::new());
    let message_store = Arc::new(MemoryMessageStore::new());
    let sender = Arc::new(PacketSender::new(
        Arc::new(Mutex::new(None)),
        Arc::new(ProtobufCodec) as Arc<dyn Codec>,
    ));
    let reliable_queue = Arc::new(ReliableSendQueue::new(ReliableSendQueueConfig {
        pending_reader: pending_store.clone(),
        pending_writer: pending_store,
        sender,
        message_store,
        conversation_store: Arc::new(NoopConversationStore),
        current_user_id: current_user_id.clone(),
        bus: bus.clone(),
        timeout_secs: Some(60),
        max_retries: Some(3),
        max_in_flight: Some(32),
        metrics: MetricsRecorder::disabled(),
    }));
    let dispatcher = Dispatcher::new(
        bus.clone(),
        Some(reliable_queue.clone()),
        None,
        None,
        None,
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus.clone()),
        MetricsRecorder::disabled(),
    );

    let mut message1 = IMMessage::new(flare_proto::common::Message::default());
    message1.client_msg_id = "client-1".to_string();
    message1.conversation_id = "conv-1".to_string();
    message1.sender_id = "u1".to_string();

    let mut message2 = IMMessage::new(flare_proto::common::Message::default());
    message2.client_msg_id = "client-2".to_string();
    message2.conversation_id = "conv-1".to_string();
    message2.sender_id = "u1".to_string();

    reliable_queue.enqueue(message1).await.unwrap();
    reliable_queue.enqueue(message2).await.unwrap();

    // Each enqueue surfaces its optimistic message via ReceivedBatch; drain
    // both so the assertions below target the buffered out-of-order acks.
    for _ in 0..2 {
        let optimistic = timeout(Duration::from_millis(200), receiver.recv())
            .await
            .expect("expected optimistic message event")
            .expect("bus closed");
        assert!(matches!(
            optimistic,
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));
    }

    tokio::time::sleep(Duration::from_millis(20)).await;

    dispatcher
        .dispatch(DownlinkPayload::SendAck(accepted_ack(
            "client-2", "conv-1", "server-2", 2,
        )))
        .await
        .unwrap();

    dispatcher
        .dispatch(DownlinkPayload::SendAck(accepted_ack(
            "client-1", "conv-1", "server-1", 1,
        )))
        .await
        .unwrap();

    let first = timeout(Duration::from_millis(200), receiver.recv())
        .await
        .expect("expected first ack event")
        .expect("bus closed");
    let second = timeout(Duration::from_millis(200), receiver.recv())
        .await
        .expect("expected second ack event")
        .expect("bus closed");

    let mut ack_ids = Vec::new();
    for event in [first, second] {
        match event {
            SdkEvent::Message(MessageEvent::SendAck { ack }) => {
                ack_ids.push(ack.client_msg_id);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
    ack_ids.sort();
    assert_eq!(
        ack_ids,
        vec!["client-1".to_string(), "client-2".to_string()]
    );

    let third = timeout(Duration::from_millis(80), receiver.recv()).await;
    assert!(
        third.is_err(),
        "acks should not be published more than once"
    );
}

#[tokio::test]
async fn realtime_and_sync_replay_share_event_deduper() {
    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let deduper = EventDeduper::new(Some(64));
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let message_store = Arc::new(MemoryMessageStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store,
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores.clone()),
        current_user_id,
        deduper.clone(),
        test_notification_pipeline(bus.clone()),
        MetricsRecorder::disabled(),
    );
    let sync_apply = SyncApplyUseCase::new(
        stores,
        bus.clone(),
        deduper,
        test_notification_pipeline(bus.clone()),
    );

    let mut event = flare_proto::common::Event::default();
    event.event_id = "evt-delete-1".to_string();
    event.conversation_id = "conv-1".to_string();
    event.payload = Some(flare_proto::common::event::Payload::Delete(
        MessageDeleteEvent {
            server_msg_id: "server-1".to_string(),
            scope: Some(2),
            ..Default::default()
        },
    ));

    dispatcher
        .dispatch(DownlinkPayload::Event(event.clone()))
        .await
        .unwrap();
    sync_apply.apply_critical_events("u1", &[event]).await;

    let mut deleted_count = 0usize;
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_millis(200) {
        match timeout(Duration::from_millis(30), receiver.recv()).await {
            Ok(Ok(SdkEvent::Message(MessageEvent::Deleted { event, .. }))) => {
                assert_eq!(event.server_msg_id, "server-1");
                deleted_count += 1;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }

    assert_eq!(
        deleted_count, 1,
        "duplicate replay should not emit second Deleted event"
    );
}

#[tokio::test]
async fn realtime_and_sync_message_replay_share_message_deduper() {
    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let deduper = EventDeduper::new(Some(64));
    let message_deduper = MessageDeduper::new(Some(64));
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let stores = StoreProvider {
        plugin_commands: None,
        messages: Arc::new(MemoryMessageStore::new()),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores.clone()),
        current_user_id,
        deduper.clone(),
        notification_pipeline.clone(),
        MetricsRecorder::disabled(),
    );
    let sync_apply = SyncApplyUseCase::new(stores, bus.clone(), deduper, notification_pipeline);

    let proto_message = flare_proto::common::Message {
        server_id: "server-msg-1".to_string(),
        client_msg_id: "client-msg-1".to_string(),
        conversation_id: "conv-1".to_string(),
        sender_id: "u2".to_string(),
        conversation_seq: 10,
        ..Default::default()
    };

    dispatcher
        .dispatch(DownlinkPayload::MessagePush(
            flare_proto::common::MessagePush {
                messages: vec![proto_message.clone()],
                notifications: Vec::new(),
            },
        ))
        .await
        .unwrap();

    sync_apply
        .apply_single_conversation_page(
            "conv-1",
            "u1",
            0,
            &flare_proto::common::SingleConversationSyncRes {
                conversation_id: "conv-1".to_string(),
                items: vec![flare_proto::common::SyncSliceItem {
                    conversation_seq: 10,
                    created_at: 0,
                    payload: Some(flare_proto::common::sync_slice_item::Payload::Message(
                        proto_message.clone(),
                    )),
                }],
                max_conversation_seq: 10,
                next_cursor: String::new(),
                has_more: false,
                hints: None,
                stale: None,
            },
        )
        .await
        .unwrap();

    let mut received_count = 0usize;
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_millis(200) {
        match timeout(Duration::from_millis(30), receiver.recv()).await {
            Ok(Ok(SdkEvent::Message(MessageEvent::Received { message }))) => {
                assert_eq!(message.server_id, "server-msg-1");
                received_count += 1;
            }
            Ok(Ok(SdkEvent::Message(MessageEvent::ReceivedBatch { messages }))) => {
                for message in messages {
                    assert_eq!(message.server_id, "server-msg-1");
                    received_count += 1;
                }
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }

    assert_eq!(
        received_count, 1,
        "duplicate replay should not emit second Received event"
    );
}

#[tokio::test]
async fn sync_page_cursor_stops_before_failed_event_seq() {
    // I1 游标-落盘顺序契约：应用失败（保留重放机会）的事件必须留在游标之外——
    // 游标停在失败事件前的连续位点并报告 seq 缺口，由既有补拉重放；
    // 若把解码 seq 直接计入游标，失败事件会被游标越过而永久丢失。
    let bus = EventBus::new();
    let deduper = EventDeduper::new(Some(64));
    let message_deduper = MessageDeduper::new(Some(64));
    let stores = StoreProvider {
        plugin_commands: None,
        messages: Arc::new(MemoryMessageStore::new()),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let sync_apply = SyncApplyUseCase::new(stores, bus.clone(), deduper, notification_pipeline);

    let message_at = |seq: u64| flare_proto::common::Message {
        server_id: format!("server-msg-{seq}"),
        client_msg_id: format!("client-msg-{seq}"),
        conversation_id: "conv-1".to_string(),
        sender_id: "u2".to_string(),
        conversation_seq: seq,
        ..Default::default()
    };
    // 编辑事件缺 new_content → applier 确定性返回「暂不可应用，保留重放」。
    let failing_edit = Event {
        conversation_id: "conv-1".to_string(),
        conversation_seq: 2,
        r#type: 3, // EVENT_MESSAGE_EDIT
        payload: Some(ProtoEventPayload::Edit(
            flare_proto::common::MessageEditEvent {
                server_msg_id: "missing-msg".to_string(),
                new_content: None,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let item = |seq: u64, payload: flare_proto::common::sync_slice_item::Payload| {
        flare_proto::common::SyncSliceItem {
            conversation_seq: seq,
            created_at: 0,
            payload: Some(payload),
        }
    };

    let applied = sync_apply
        .apply_single_conversation_page(
            "conv-1",
            "u1",
            0,
            &flare_proto::common::SingleConversationSyncRes {
                conversation_id: "conv-1".to_string(),
                items: vec![
                    item(
                        1,
                        flare_proto::common::sync_slice_item::Payload::Message(message_at(1)),
                    ),
                    item(
                        2,
                        flare_proto::common::sync_slice_item::Payload::Event(failing_edit),
                    ),
                    item(
                        3,
                        flare_proto::common::sync_slice_item::Payload::Message(message_at(3)),
                    ),
                ],
                max_conversation_seq: 3,
                next_cursor: String::new(),
                has_more: false,
                hints: None,
                stale: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        applied.max_seq, 1,
        "cursor must stop at the contiguous point before the failed event"
    );
    assert!(applied.has_seq_gap, "failed event must surface as seq gap");
    assert!(
        applied.has_more,
        "seq gap must force another pull so the event can be replayed"
    );
}

#[tokio::test]
async fn clamped_cursor_save_proves_only_window_above_prior_cursor() {
    // floor 有界连续性：旧游标(5)之下的历史缺失（快照建立/本地清空）不阻塞推进——
    // 只需证明 (5, 8] 窗口连续。旧实现从 0 证明 → 恒钳 0 → 静默跳过，游标永久卡死。
    let bus = EventBus::new();
    let deduper = EventDeduper::new(Some(64));
    let message_deduper = MessageDeduper::new(Some(64));
    let message_store = Arc::new(MemoryMessageStore::new());
    let cursor_store = Arc::new(crate::infrastructure::persistence::MemorySyncCursorStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: cursor_store.clone(),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let sync_apply = SyncApplyUseCase::new(stores, bus.clone(), deduper, notification_pipeline);

    // 旧游标 5（例如快照切换建立），本地只有 6..=8，1..=4 缺失。
    cursor_store
        .save_conversation_cursor(&SyncCursorVo {
            user_id: "u1".to_string(),
            conversation_id: "conv-w".to_string(),
            last_seq: 5,
            synced_at: 1,
        })
        .await
        .unwrap();
    for seq in 6..=8u64 {
        let mut message = IMMessage::new(flare_proto::common::Message {
            server_id: format!("m-{seq}"),
            client_msg_id: format!("c-{seq}"),
            conversation_id: "conv-w".to_string(),
            sender_id: "u2".to_string(),
            conversation_seq: seq,
            ..Default::default()
        });
        message.conversation_seq = seq;
        message_store.save_batch(&[message]).await.unwrap();
    }

    sync_apply
        .save_cursor_with_remote("u1", "conv-w", 8, &[], |_, _, _| async { Ok(()) })
        .await
        .unwrap();

    let saved = cursor_store
        .get_conversation_cursor("u1", "conv-w")
        .await
        .unwrap()
        .expect("cursor must exist");
    assert_eq!(
        saved.last_seq, 8,
        "window (5,8] is contiguous locally; missing 1..=4 below the proven floor must not block"
    );
}

#[tokio::test]
async fn cursor_advances_past_seqs_the_server_proved_have_no_message() {
    // 服务端对「已提交但没有消息行」的 seq 会下发 tombstone/skip 占位，页面据此把它们
    // 计入已覆盖位点。存游标时若只按本地消息行重算连续性，这些 seq 永远补不齐 ——
    // 游标卡在缺口前，每轮同步拉回同一页，实时消息也就永远物化不了。
    let bus = EventBus::new();
    let deduper = EventDeduper::new(Some(64));
    let message_deduper = MessageDeduper::new(Some(64));
    let cursor_store = Arc::new(crate::infrastructure::persistence::MemorySyncCursorStore::new());
    let message_store = Arc::new(MemoryMessageStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: cursor_store.clone(),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let sync_apply = SyncApplyUseCase::new(stores, bus.clone(), deduper, notification_pipeline);

    cursor_store
        .save_conversation_cursor(&SyncCursorVo {
            user_id: "u1".to_string(),
            conversation_id: "conv-gap".to_string(),
            last_seq: 12,
            synced_at: 1,
        })
        .await
        .unwrap();
    // 真实现场：seq 13..=21 全被非消息事件吃掉，只有 22 是消息。
    let mut message = IMMessage::new(flare_proto::common::Message {
        server_id: "m-22".to_string(),
        client_msg_id: "c-22".to_string(),
        conversation_id: "conv-gap".to_string(),
        sender_id: "u2".to_string(),
        conversation_seq: 22,
        ..Default::default()
    });
    message.conversation_seq = 22;
    message_store.save_batch(&[message]).await.unwrap();

    let proven_absent: Vec<u64> = (13..=21).collect();
    sync_apply
        .save_cursor_with_remote("u1", "conv-gap", 22, &proven_absent, |_, _, _| async {
            Ok(())
        })
        .await
        .unwrap();

    let saved = cursor_store
        .get_conversation_cursor("u1", "conv-gap")
        .await
        .unwrap()
        .expect("cursor must exist");
    assert_eq!(
        saved.last_seq, 22,
        "服务端已证明 13..=21 没有消息，游标必须越过它们，否则同步永远原地打转"
    );
}

#[tokio::test]
async fn cursor_still_stops_at_messages_that_failed_to_persist() {
    // 反向锁死 I1：没有服务端证明的缺口（消息该落库却没落）仍然必须挡住游标。
    let bus = EventBus::new();
    let deduper = EventDeduper::new(Some(64));
    let message_deduper = MessageDeduper::new(Some(64));
    let cursor_store = Arc::new(crate::infrastructure::persistence::MemorySyncCursorStore::new());
    let message_store = Arc::new(MemoryMessageStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: cursor_store.clone(),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let sync_apply = SyncApplyUseCase::new(stores, bus.clone(), deduper, notification_pipeline);

    cursor_store
        .save_conversation_cursor(&SyncCursorVo {
            user_id: "u1".to_string(),
            conversation_id: "conv-hole".to_string(),
            last_seq: 12,
            synced_at: 1,
        })
        .await
        .unwrap();
    let mut message = IMMessage::new(flare_proto::common::Message {
        server_id: "m-15".to_string(),
        client_msg_id: "c-15".to_string(),
        conversation_id: "conv-hole".to_string(),
        sender_id: "u2".to_string(),
        conversation_seq: 15,
        ..Default::default()
    });
    message.conversation_seq = 15;
    message_store.save_batch(&[message]).await.unwrap();

    // 只证明了 13，14 仍是未知缺口。
    sync_apply
        .save_cursor_with_remote("u1", "conv-hole", 15, &[13], |_, _, _| async { Ok(()) })
        .await
        .unwrap();

    let saved = cursor_store
        .get_conversation_cursor("u1", "conv-hole")
        .await
        .unwrap()
        .expect("cursor must exist");
    assert_eq!(
        saved.last_seq, 13,
        "14 既没有消息行也没有服务端证明，游标不能越过去"
    );
}

#[tokio::test]
async fn watermark_cursor_save_persists_pseudo_keys_that_clamped_save_skips() {
    // 伪 key（critical_event:*/__conversations__ 水位）与快照建立型游标没有本地连续消息，
    // 钳制保存会把它们清零静默跳过 → 必须走无钳制的 watermark 保存。此测试钉死两条路径的分工。
    let bus = EventBus::new();
    let deduper = EventDeduper::new(Some(64));
    let message_deduper = MessageDeduper::new(Some(64));
    let cursor_store = Arc::new(crate::infrastructure::persistence::MemorySyncCursorStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: Arc::new(MemoryMessageStore::new()),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: cursor_store.clone(),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let sync_apply = SyncApplyUseCase::new(stores, bus.clone(), deduper, notification_pipeline);

    // 钳制保存：伪 key 无消息行 → 连续位点 0 → 静默跳过（这是它的既定语义）。
    sync_apply
        .save_cursor_with_remote("u1", "critical_event:c1", 7, &[], |_, _, _| async {
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        cursor_store
            .get_conversation_cursor("u1", "critical_event:c1")
            .await
            .unwrap()
            .is_none(),
        "clamped save must skip pseudo keys (documented behavior)"
    );

    // 水位保存：必须持久化。
    sync_apply
        .save_watermark_with_remote("u1", "critical_event:c1", 7, |_, _, _| async { Ok(()) })
        .await
        .unwrap();
    let saved = cursor_store
        .get_conversation_cursor("u1", "critical_event:c1")
        .await
        .unwrap()
        .expect("watermark save must persist pseudo-key cursors");
    assert_eq!(saved.last_seq, 7);
}

#[tokio::test]
async fn snapshot_row_apply_persists_messages_and_returns_cutover_seq() {
    // I2 gap-too-large：快照行应用=消息落库 + 返回快照水位；游标由调用方在落库后推进（I1 契约）。
    let bus = EventBus::new();
    let deduper = EventDeduper::new(Some(64));
    let message_deduper = MessageDeduper::new(Some(64));
    let message_store = Arc::new(MemoryMessageStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let sync_apply = SyncApplyUseCase::new(stores, bus.clone(), deduper, notification_pipeline);

    let row = flare_proto::common::SnapshotConversationRow {
        conversation_id: "conv-1".to_string(),
        messages: vec![flare_proto::common::Message {
            server_id: "snap-msg-9".to_string(),
            client_msg_id: "snap-client-9".to_string(),
            conversation_id: "conv-1".to_string(),
            sender_id: "u2".to_string(),
            conversation_seq: 9,
            ..Default::default()
        }],
        last_conversation_seq: 9,
        last_message_at: 0,
        unread_count: 1,
        last_read_seq: 8,
        summary: None,
    };

    let cutover_seq = sync_apply.apply_snapshot_row("u1", row).await.unwrap();

    assert_eq!(cutover_seq, 9, "cutover seq must be the snapshot watermark");
    let persisted = message_store.get("snap-msg-9").await.unwrap();
    assert!(
        persisted.is_some(),
        "snapshot messages must be durably persisted before cursor advance"
    );
}

#[tokio::test]
async fn message_push_delivers_to_view_even_when_persist_fails() {
    // A2/BX-02：先投递后持久化。save_batch 失败时消息仍应投递到视图（bus），
    // 由既有 seq-gap 补拉负责重取持久化，而不是从本批次丢弃。
    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let deduper = EventDeduper::new(Some(64));
    let message_deduper = MessageDeduper::new(Some(64));
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let stores = StoreProvider {
        plugin_commands: None,
        messages: Arc::new(MemoryMessageStore::failing()),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores),
        current_user_id,
        deduper,
        notification_pipeline,
        MetricsRecorder::disabled(),
    );

    dispatcher
        .dispatch(DownlinkPayload::MessagePush(
            flare_proto::common::MessagePush {
                messages: vec![flare_proto::common::Message {
                    server_id: "server-fail-1".to_string(),
                    client_msg_id: "client-fail-1".to_string(),
                    conversation_id: "conv-1".to_string(),
                    sender_id: "u2".to_string(),
                    conversation_seq: 7,
                    ..Default::default()
                }],
                notifications: Vec::new(),
            },
        ))
        .await
        .unwrap();

    let mut received = false;
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_millis(200) {
        match timeout(Duration::from_millis(30), receiver.recv()).await {
            Ok(Ok(SdkEvent::Message(MessageEvent::Received { message }))) => {
                assert_eq!(message.server_id, "server-fail-1");
                received = true;
                break;
            }
            Ok(Ok(SdkEvent::Message(MessageEvent::ReceivedBatch { messages }))) => {
                if messages
                    .iter()
                    .any(|message| message.server_id == "server-fail-1")
                {
                    received = true;
                    break;
                }
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }

    assert!(
        received,
        "save_batch 失败时消息仍须投递到视图（deliver-then-persist），不得从本批次丢弃"
    );
}

#[tokio::test]
async fn concurrent_message_push_dispatch_is_lossless_for_slow_subscriber() {
    const TASKS: usize = 8;
    const PER_TASK: usize = 250;
    const TOTAL: usize = TASKS * PER_TASK;

    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let message_store = Arc::new(MemoryMessageStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let dispatcher = Arc::new(Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores.clone()),
        current_user_id,
        EventDeduper::new(Some(TOTAL * 2)),
        test_notification_pipeline(bus.clone()),
        MetricsRecorder::disabled(),
    ));

    let mut tasks = Vec::with_capacity(TASKS);
    for task_id in 0..TASKS {
        let dispatcher = dispatcher.clone();
        tasks.push(tokio::spawn(async move {
            let mut messages = Vec::with_capacity(PER_TASK);
            for index in 0..PER_TASK {
                let seq = (task_id * PER_TASK + index + 1) as u64;
                messages.push(flare_proto::common::Message {
                    server_id: format!("server-{seq}"),
                    client_msg_id: format!("client-{seq}"),
                    conversation_id: format!("conv-{}", task_id % 4),
                    sender_id: "u2".to_string(),
                    conversation_seq: seq,
                    message_type: flare_proto::common::MessageType::Text as i32,
                    ..Default::default()
                });
            }
            dispatcher
                .dispatch(DownlinkPayload::MessagePush(
                    flare_proto::common::MessagePush {
                        messages,
                        notifications: Vec::new(),
                    },
                ))
                .await
                .expect("message push dispatch");
        }));
    }

    for task in tasks {
        task.await.expect("dispatch task panicked");
    }

    let mut received_server_ids = std::collections::HashSet::with_capacity(TOTAL);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while received_server_ids.len() < TOTAL {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out after receiving {} of {TOTAL} messages",
            received_server_ids.len()
        );
        match timeout(remaining.min(Duration::from_millis(100)), receiver.recv()).await {
            Ok(Ok(SdkEvent::Message(MessageEvent::Received { message }))) => {
                assert!(
                    received_server_ids.insert(message.server_id.clone()),
                    "duplicate received message {}",
                    message.server_id
                );
            }
            Ok(Ok(SdkEvent::Message(MessageEvent::ReceivedBatch { messages }))) => {
                for message in messages {
                    assert!(
                        received_server_ids.insert(message.server_id.clone()),
                        "duplicate received message {}",
                        message.server_id
                    );
                }
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("event receiver closed: {error:?}"),
            Err(_) => {}
        }
    }

    assert_eq!(
        message_store.data.read().await.len(),
        TOTAL,
        "all dispatched messages should be persisted exactly once"
    );
}

#[tokio::test]
async fn persist_worker_persists_asynchronously_when_started() {
    // A2 win#2：装载 worker 后，持久化由后台串行 worker 完成（最终一致），dispatch 不再内联 await 落盘。
    let bus = EventBus::new();
    let deduper = EventDeduper::new(Some(64));
    let message_deduper = MessageDeduper::new(Some(64));
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let message_store = Arc::new(MemoryMessageStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let dispatcher = Arc::new(Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores),
        current_user_id,
        deduper,
        notification_pipeline,
        MetricsRecorder::disabled(),
    ));
    dispatcher.start_persist_worker();

    dispatcher
        .dispatch(DownlinkPayload::MessagePush(
            flare_proto::common::MessagePush {
                messages: vec![flare_proto::common::Message {
                    server_id: "server-worker-1".to_string(),
                    client_msg_id: "client-worker-1".to_string(),
                    conversation_id: "conv-1".to_string(),
                    sender_id: "u2".to_string(),
                    conversation_seq: 5,
                    ..Default::default()
                }],
                notifications: Vec::new(),
            },
        ))
        .await
        .unwrap();

    // worker 异步落盘 → 最终一致地轮询存储
    let mut persisted = false;
    let start = tokio::time::Instant::now();
    while start.elapsed() < Duration::from_millis(500) {
        if message_store
            .data
            .read()
            .await
            .contains_key("server-worker-1")
        {
            persisted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        persisted,
        "启动 worker 后，消息应由后台串行 worker 异步持久化"
    );
}

/// A2 win#2 压测:启动 worker 后并发投递(超过 channel 容量 → 触发背压),
/// 断言**零丢失**(全部经后台串行 worker 落盘)、无死锁。
#[tokio::test]
async fn persist_worker_under_concurrent_dispatch_loses_nothing() {
    const TASKS: usize = 8;
    const PER_TASK: usize = 40; // 8×40=320 > 持久化 channel 容量 256 → 必经背压
    let total = TASKS * PER_TASK;

    let bus = EventBus::new();
    let deduper = EventDeduper::new(Some(2048));
    let message_deduper = MessageDeduper::new(Some(2048));
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let message_store = Arc::new(MemoryMessageStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let notification_pipeline =
        test_notification_pipeline_with_deduper(bus.clone(), message_deduper.clone());
    let dispatcher = Arc::new(Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores),
        current_user_id,
        deduper,
        notification_pipeline,
        MetricsRecorder::disabled(),
    ));
    dispatcher.start_persist_worker();

    let mut handles = Vec::with_capacity(TASKS);
    for t in 0..TASKS {
        let d = dispatcher.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..PER_TASK {
                d.dispatch(DownlinkPayload::MessagePush(
                    flare_proto::common::MessagePush {
                        messages: vec![flare_proto::common::Message {
                            server_id: format!("srv-{t}-{i}"),
                            client_msg_id: format!("cli-{t}-{i}"),
                            conversation_id: format!("conv-{t}"),
                            sender_id: "u2".to_string(),
                            conversation_seq: (i as u64) + 1,
                            ..Default::default()
                        }],
                        notifications: Vec::new(),
                    },
                ))
                .await
                .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // 持久化经后台 worker 异步完成 → 最终一致轮询:全部落盘、零丢失。
    let start = tokio::time::Instant::now();
    loop {
        let n = message_store.data.read().await.len();
        if n >= total {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "persist worker lost messages under concurrency: {n}/{total}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        message_store.data.read().await.len(),
        total,
        "all concurrently dispatched messages must be persisted exactly once via the worker"
    );
}

#[test]
fn seq_helpers_ignore_duplicates_and_find_contiguous_tail() {
    assert_eq!(max_contiguous_seq(10, &[12, 11, 11, 13]), 13);
    assert_eq!(max_contiguous_seq(10, &[12, 13]), 10);
    assert_eq!(first_gap_after(10, &[11, 13, 14]), Some(11));
    assert_eq!(first_gap_after(10, &[11, 12, 13]), None);
}

fn message_with(sender: &str, seq: u64) -> IMMessage {
    IMMessage::new(flare_proto::common::Message {
        sender_id: sender.to_string(),
        conversation_seq: seq,
        ..Default::default()
    })
}

#[test]
fn detects_outgoing_message_that_got_a_regressed_seq() {
    // 本端(u1)发出的消息拿到 seq=8/9，但会话已同步到高水位 76 → 回退，应被检出。
    let batch = vec![message_with("u1", 8), message_with("u1", 9)];
    assert_eq!(
        detect_outgoing_seq_regressions(&batch, "u1", 76),
        vec![8, 9]
    );
}

#[test]
fn ignores_incoming_and_forward_progress_and_missing_baseline() {
    // 对端(u2)的历史消息即使 seq 低也不算本端回退（只测本端新发）。
    let incoming_only = vec![message_with("u2", 8), message_with("u2", 9)];
    assert!(detect_outgoing_seq_regressions(&incoming_only, "u1", 76).is_empty());
    // 本端消息拿到更高 seq（正常前进）→ 不报。
    let forward = vec![message_with("u1", 77), message_with("u1", 78)];
    assert!(detect_outgoing_seq_regressions(&forward, "u1", 76).is_empty());
    // 无高水位基线(0) 或无 user → 不误报。
    assert!(detect_outgoing_seq_regressions(&[message_with("u1", 8)], "u1", 0).is_empty());
    assert!(detect_outgoing_seq_regressions(&[message_with("u1", 8)], "", 76).is_empty());
    // seq==high_water 边界视为回退（撞号，见观测数据 seq=9 撞旧消息）。
    assert_eq!(
        detect_outgoing_seq_regressions(&[message_with("u1", 76)], "u1", 76),
        vec![76]
    );
}

// ---- TEST-1: 生成式属性测试(seq-gap 辅助函数 = 游标推进/补拉正确性的支点)----
// 用确定性 LCG 造大量乱序+重复+空洞的输入,断言结构性不变量(独立于实现,非镜像)。

struct Lcg(u64);
impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }
}

/// 小值域 → 强制出现空洞/重复/0;含 0 以覆盖「0 非真实 seq」的过滤。
fn gen_seqs(rng: &mut Lcg) -> Vec<u64> {
    let len = rng.below(12) as usize;
    let span = 1 + rng.below(10);
    (0..len).map(|_| rng.below(span + 1)).collect()
}

#[test]
fn prop_max_contiguous_seq_is_the_maximal_present_run_from_known() {
    let mut rng = Lcg(0x1234_5678_9abc_def0);
    for _ in 0..4000 {
        let seqs = gen_seqs(&mut rng);
        let known = rng.below(8);
        let set: std::collections::BTreeSet<u64> = seqs.iter().copied().collect();

        let m = max_contiguous_seq(known, &seqs);
        assert!(
            m >= known,
            "result below known: m={m} known={known} seqs={seqs:?}"
        );
        // (known, m] 全部在集合内(运行确实连续且都存在)。
        for i in (known + 1)..=m {
            assert!(
                set.contains(&i),
                "hole inside run at {i}: m={m} known={known} seqs={seqs:?}"
            );
        }
        // 运行停在第一个缺口:m+1 不在集合。
        assert!(
            !set.contains(&(m + 1)),
            "run must stop at first hole but m+1 present: m={m} known={known} seqs={seqs:?}"
        );
    }
}

#[test]
fn prop_first_gap_after_is_contiguous_tail_iff_data_beyond_hole() {
    let mut rng = Lcg(0xdead_beef_cafe_babe);
    for _ in 0..4000 {
        let seqs = gen_seqs(&mut rng);
        let known = rng.below(8);
        let set: std::collections::BTreeSet<u64> = seqs.iter().copied().collect();
        let m = max_contiguous_seq(known, &seqs);
        let has_later = set.iter().any(|s| *s > m + 1);

        match first_gap_after(known, &seqs) {
            Some(g) => {
                assert_eq!(
                    g, m,
                    "gap-after must equal the contiguous tail; seqs={seqs:?}"
                );
                assert!(
                    has_later,
                    "Some(gap) requires data beyond the hole; seqs={seqs:?}"
                );
            }
            None => assert!(
                !has_later,
                "None requires no data beyond the hole; seqs={seqs:?}"
            ),
        }
    }
}

#[test]
fn prop_first_internal_gap_from_matches_independent_reference() {
    let mut rng = Lcg(0x0f0f_0f0f_1357_9bdf);
    for _ in 0..4000 {
        let seqs = gen_seqs(&mut rng);
        let base = rng.below(8);
        // 实现内部对 >= base 后还会再滤掉 0(0 非真实 seq),参考须对齐:>= base 且 > 0。
        let mut t: Vec<u64> = seqs
            .iter()
            .copied()
            .filter(|s| *s >= base && *s > 0)
            .collect();
        t.sort_unstable();
        t.dedup();
        let expected = t.windows(2).find(|w| w[1] > w[0] + 1).map(|w| w[0]);
        assert_eq!(
            first_internal_gap_after_from(base, &seqs),
            expected,
            "base={base} seqs={seqs:?} t={t:?}"
        );
    }
}

#[test]
fn prop_seq_repair_backoff_is_monotonic_bounded_and_formula() {
    use super::{SEQ_REPAIR_BASE_BACKOFF_MS, SEQ_REPAIR_MAX_BACKOFF_MS};
    let mut prev = 0u64;
    for attempt in 0..40u32 {
        let b = seq_repair_backoff_ms(attempt);
        assert!(
            b >= SEQ_REPAIR_BASE_BACKOFF_MS,
            "below base at {attempt}: {b}"
        );
        assert!(
            b <= SEQ_REPAIR_MAX_BACKOFF_MS,
            "above cap at {attempt}: {b}"
        );
        assert!(
            b >= prev,
            "must be non-decreasing: at {attempt} {b} < {prev}"
        );
        prev = b;
    }
    assert_eq!(seq_repair_backoff_ms(0), SEQ_REPAIR_BASE_BACKOFF_MS);
    assert_eq!(seq_repair_backoff_ms(1), SEQ_REPAIR_BASE_BACKOFF_MS);
    assert_eq!(seq_repair_backoff_ms(2), SEQ_REPAIR_BASE_BACKOFF_MS * 2);
    assert_eq!(seq_repair_backoff_ms(100), SEQ_REPAIR_MAX_BACKOFF_MS);
}

#[test]
fn waterline_ping_matches_control_type_or_kind_attribute() {
    assert!(is_waterline_ping("sync.waterline", &HashMap::new()));
    assert!(is_waterline_ping(
        "custom",
        &HashMap::from([(
            WATERLINE_ATTR_KIND.to_string(),
            "conversation.waterline".to_string()
        )])
    ));
    assert!(!is_waterline_ping("typing", &HashMap::new()));
}

#[test]
fn waterline_attributes_parse_conversation_and_seq() {
    let attrs = HashMap::from([
        (
            WATERLINE_ATTR_CONVERSATION_ID_CAMEL.to_string(),
            " c1 ".to_string(),
        ),
        (WATERLINE_ATTR_MAX_SEQ_CAMEL.to_string(), "42".to_string()),
    ]);
    assert_eq!(
        attr_non_empty(&attrs, WATERLINE_ATTR_CONVERSATION_ID_CAMEL),
        Some("c1")
    );
    assert_eq!(attr_u64(&attrs, WATERLINE_ATTR_MAX_SEQ_CAMEL), Some(42));
}

#[tokio::test]
async fn empty_event_envelope_waterline_triggers_message_sync() {
    let bus = EventBus::new();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let sync = Arc::new(RecordingSessionSyncRunner::new());
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        Some(sync.clone()),
        None,
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    );

    dispatcher
        .dispatch(DownlinkPayload::EventEnvelope(EventEnvelope {
            conversation_id: "conversation-1".to_string(),
            max_conversation_seq: 2,
            ..Default::default()
        }))
        .await
        .unwrap();

    timeout(Duration::from_millis(200), sync.notify.notified())
        .await
        .expect("event envelope waterline should trigger message sync");
    assert_eq!(
        sync.message_sync_calls().await,
        vec!["conversation-1".to_string()]
    );
}

#[tokio::test]
async fn event_envelope_messages_publish_one_batch() {
    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        None,
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    );

    dispatcher
        .dispatch(DownlinkPayload::EventEnvelope(EventEnvelope {
            conversation_id: "conversation-1".to_string(),
            max_conversation_seq: 2,
            events: vec![
                Event {
                    event_id: "event-1".to_string(),
                    conversation_id: "conversation-1".to_string(),
                    conversation_seq: 1,
                    payload: Some(ProtoEventPayload::Message(flare_proto::common::Message {
                        server_id: "server-1".to_string(),
                        client_msg_id: "client-1".to_string(),
                        conversation_id: "conversation-1".to_string(),
                        sender_id: "u2".to_string(),
                        conversation_seq: 1,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                Event {
                    event_id: "event-2".to_string(),
                    conversation_id: "conversation-1".to_string(),
                    conversation_seq: 2,
                    payload: Some(ProtoEventPayload::Message(flare_proto::common::Message {
                        server_id: "server-2".to_string(),
                        client_msg_id: "client-2".to_string(),
                        conversation_id: "conversation-1".to_string(),
                        sender_id: "u2".to_string(),
                        conversation_seq: 2,
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }))
        .await
        .unwrap();

    match timeout(Duration::from_millis(200), receiver.recv()).await {
        Ok(Ok(SdkEvent::Message(MessageEvent::ReceivedBatch { messages }))) => {
            let server_ids = messages
                .iter()
                .map(|message| message.server_id.as_str())
                .collect::<Vec<_>>();
            assert_eq!(server_ids, vec!["server-1", "server-2"]);
        }
        other => panic!("expected one ReceivedBatch from event envelope, got {other:?}"),
    }

    let stray_single = timeout(Duration::from_millis(80), receiver.recv()).await;
    assert!(
        stray_single.is_err(),
        "event envelope message batch must not replay per-message callbacks"
    );
}

#[tokio::test]
async fn standalone_event_waterline_triggers_message_sync() {
    let bus = EventBus::new();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let sync = Arc::new(RecordingSessionSyncRunner::new());
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        Some(sync.clone()),
        None,
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    );

    dispatcher
        .dispatch(DownlinkPayload::Event(Event {
            conversation_id: "conversation-1".to_string(),
            conversation_seq: 101,
            ..Default::default()
        }))
        .await
        .unwrap();

    timeout(Duration::from_millis(200), sync.notify.notified())
        .await
        .expect("standalone event waterline should trigger message sync");
    assert_eq!(
        sync.message_sync_calls().await,
        vec!["conversation-1".to_string()]
    );
}

/// 回归夹具：本地已有 conv 的消息 seq=1..=`local_max`、游标停在 `cursor`。
fn waterline_fixture() -> (
    Arc<MemoryMessageStore>,
    Arc<crate::infrastructure::persistence::MemorySyncCursorStore>,
    StoreProvider,
) {
    let message_store = Arc::new(MemoryMessageStore::new());
    let cursor_store = Arc::new(crate::infrastructure::persistence::MemorySyncCursorStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: cursor_store.clone(),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    (message_store, cursor_store, stores)
}

async fn seed_waterline_conversation(
    message_store: &Arc<MemoryMessageStore>,
    cursor_store: &Arc<crate::infrastructure::persistence::MemorySyncCursorStore>,
    conversation_id: &str,
    local_max: u64,
) {
    for seq in 1..=local_max {
        let mut message = IMMessage::new(flare_proto::common::Message {
            server_id: format!("m-{seq}"),
            client_msg_id: format!("c-{seq}"),
            conversation_id: conversation_id.to_string(),
            sender_id: "u2".to_string(),
            conversation_seq: seq,
            ..Default::default()
        });
        message.conversation_seq = seq;
        message_store.save_batch(&[message]).await.unwrap();
    }
    cursor_store
        .save_conversation_cursor(&SyncCursorVo {
            user_id: "u1".to_string(),
            conversation_id: conversation_id.to_string(),
            last_seq: local_max,
            synced_at: 1,
        })
        .await
        .unwrap();
}

fn waterline_dispatcher(
    stores: StoreProvider,
    sync: Arc<RecordingSessionSyncRunner>,
) -> Dispatcher {
    let bus = EventBus::new();
    Dispatcher::new(
        bus.clone(),
        None,
        None,
        Some(sync),
        Some(stores),
        Arc::new(RwLock::new("u1".to_string())) as CurrentUserIdStore,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    )
}

/// 操作事件与消息共享 seq 空间。本地消息到 seq=10、游标=10 时，
/// 一条 seq=11 的操作事件（如已读回执）是**连续**的下一个 seq，不得触发全量单会话同步。
/// 修复前该判定必然假阴性 → 活跃群里每条已读/reaction 都打一次同步 RPC。
#[tokio::test]
async fn applied_operation_event_seq_does_not_trigger_message_sync() {
    let (message_store, cursor_store, stores) = waterline_fixture();
    seed_waterline_conversation(&message_store, &cursor_store, "conv-op", 10).await;
    let sync = Arc::new(RecordingSessionSyncRunner::new());
    let dispatcher = waterline_dispatcher(stores, sync.clone());

    dispatcher
        .dispatch(DownlinkPayload::Event(Event {
            conversation_id: "conv-op".to_string(),
            conversation_seq: 11,
            payload: Some(EventPayload::Read(flare_proto::common::ReadReceiptEvent {
                conversation_id: "conv-op".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        }))
        .await
        .unwrap();

    // 给后台补拉任务留出窗口：不应有任何同步调用。
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        sync.message_sync_calls().await.is_empty(),
        "contiguous operation-event seq must not trigger a full conversation sync"
    );
}

/// 线上时序复现：事件占掉的 seq 由**信封**送达时，也必须计入连续窗口。
///
/// 生产实测每条实时消息都会触发一次补拉，即"每条消息每个在线端多一次读 RPC"。
/// 现场数字（会话 2AFZSMA4FJ4VPCVEC7）：游标 116 → 收到 seq 117 的已读回执信封
/// → 1.6 秒后收到 seq 118 的消息，随即
/// `实时消息 seq 出现缺口 cursor_seq=116`。库里 117 确实是一条 event_type=5，
/// 服务端也确实先 allocate_seq 再推——所以 117 是带着 seq 到客户端的。
#[tokio::test]
async fn event_seq_delivered_via_envelope_counts_toward_continuity() {
    let (message_store, cursor_store, stores) = waterline_fixture();
    seed_waterline_conversation(&message_store, &cursor_store, "conv-env", 116).await;
    let sync = Arc::new(RecordingSessionSyncRunner::new());
    let dispatcher = waterline_dispatcher(stores, sync.clone());

    // ① 已读回执占掉 117，走信封下来（线上就是这么送的）。
    dispatcher
        .dispatch(DownlinkPayload::EventEnvelope(EventEnvelope {
            conversation_id: "conv-env".to_string(),
            max_conversation_seq: 117,
            events: vec![Event {
                conversation_id: "conv-env".to_string(),
                conversation_seq: 117,
                payload: Some(EventPayload::Read(flare_proto::common::ReadReceiptEvent {
                    conversation_id: "conv-env".to_string(),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            ..Default::default()
        }))
        .await
        .unwrap();

    // ② 下一条消息落在 118：116→117(事件)→118(消息) 是连续的，不该判成缺口。
    dispatcher
        .dispatch(DownlinkPayload::MessagePush(
            flare_proto::common::MessagePush {
                messages: vec![flare_proto::common::Message {
                    server_id: "m-118".to_string(),
                    client_msg_id: "c-118".to_string(),
                    conversation_id: "conv-env".to_string(),
                    sender_id: "u2".to_string(),
                    conversation_seq: 118,
                    message_type: flare_proto::common::MessageType::Text as i32,
                    ..Default::default()
                }],
                notifications: Vec::new(),
            },
        ))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        sync.message_sync_calls().await.is_empty(),
        "事件占掉的 seq 已由信封送达并应用，不能再判成缺口——\
         否则每条实时消息都要多一次读 RPC，在线人数越多放大越狠"
    );
}

/// 反向保证：真缺口（seq 跳跃，中间既无消息行也无已应用事件）仍必须触发同步补拉。
#[tokio::test]
async fn genuine_seq_gap_still_triggers_message_sync() {
    let (message_store, cursor_store, stores) = waterline_fixture();
    seed_waterline_conversation(&message_store, &cursor_store, "conv-gap", 10).await;
    let sync = Arc::new(RecordingSessionSyncRunner::new());
    let dispatcher = waterline_dispatcher(stores, sync.clone());

    dispatcher
        .dispatch(DownlinkPayload::Event(Event {
            conversation_id: "conv-gap".to_string(),
            // 11..=19 从未到达 → 真缺口
            conversation_seq: 20,
            payload: Some(EventPayload::Read(flare_proto::common::ReadReceiptEvent {
                conversation_id: "conv-gap".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        }))
        .await
        .unwrap();

    timeout(Duration::from_millis(200), sync.notify.notified())
        .await
        .expect("a genuine seq gap must still trigger message sync");
    assert_eq!(
        sync.message_sync_calls().await,
        vec!["conv-gap".to_string()]
    );
}

/// 事件与消息交错：连续多条操作事件逐个填充 seq，连续性判定应随之推进而始终不触发同步。
#[tokio::test]
async fn interleaved_operation_events_keep_waterline_contiguous() {
    let (message_store, cursor_store, stores) = waterline_fixture();
    seed_waterline_conversation(&message_store, &cursor_store, "conv-mix", 5).await;
    let sync = Arc::new(RecordingSessionSyncRunner::new());
    let dispatcher = waterline_dispatcher(stores, sync.clone());

    for seq in 6..=9u64 {
        dispatcher
            .dispatch(DownlinkPayload::Event(Event {
                conversation_id: "conv-mix".to_string(),
                conversation_seq: seq,
                event_id: format!("e-{seq}"),
                payload: Some(EventPayload::Read(flare_proto::common::ReadReceiptEvent {
                    conversation_id: "conv-mix".to_string(),
                    ..Default::default()
                })),
                ..Default::default()
            }))
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        sync.message_sync_calls().await.is_empty(),
        "a contiguous run of operation events must stay gap-free"
    );
}

#[tokio::test]
async fn in_flight_waterline_ping_runs_follow_up_pull() {
    let bus = EventBus::new();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let sync = Arc::new(BlockingSessionSyncRunner::new());
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        Some(sync.clone()),
        None,
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    );

    dispatcher
        .maybe_trigger_waterline_pull(
            "test",
            "conversation.waterline",
            Some("conversation-1"),
            &HashMap::new(),
        )
        .await;
    timeout(Duration::from_millis(200), sync.notify.notified())
        .await
        .expect("first waterline pull should start");

    dispatcher
        .maybe_trigger_waterline_pull(
            "test",
            "conversation.waterline",
            Some("conversation-1"),
            &HashMap::new(),
        )
        .await;
    assert_eq!(
        sync.message_sync_calls().await,
        vec!["conversation-1".to_string()]
    );

    sync.release_first.notify_one();
    timeout(Duration::from_millis(200), sync.notify.notified())
        .await
        .expect("coalesced waterline ping should run a follow-up pull");
    assert_eq!(
        sync.message_sync_calls().await,
        vec!["conversation-1".to_string(), "conversation-1".to_string()]
    );
}

#[tokio::test]
async fn unread_conversation_update_without_seq_triggers_message_sync() {
    let bus = EventBus::new();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let sync = Arc::new(RecordingSessionSyncRunner::new());
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        Some(sync.clone()),
        None,
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    );

    dispatcher
        .dispatch(DownlinkPayload::Event(Event {
            conversation_id: "conversation-1".to_string(),
            conversation_seq: 0,
            payload: Some(ProtoEventPayload::Conversation(ConversationUpdateEvent {
                conversation_id: "conversation-1".to_string(),
                unread_count: 2,
                ..Default::default()
            })),
            ..Default::default()
        }))
        .await
        .unwrap();

    timeout(Duration::from_millis(200), sync.notify.notified())
        .await
        .expect("unread conversation update should trigger message sync");
    assert_eq!(
        sync.message_sync_calls().await,
        vec!["conversation-1".to_string()]
    );
}

#[tokio::test]
async fn unread_conversation_update_uses_typed_payload_conversation_id() {
    let bus = EventBus::new();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let sync = Arc::new(RecordingSessionSyncRunner::new());
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        Some(sync.clone()),
        None,
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    );

    dispatcher
        .dispatch(DownlinkPayload::Event(Event {
            conversation_id: String::new(),
            conversation_seq: 0,
            payload: Some(ProtoEventPayload::Conversation(ConversationUpdateEvent {
                conversation_id: "conversation-1".to_string(),
                unread_count: 2,
                ..Default::default()
            })),
            ..Default::default()
        }))
        .await
        .unwrap();

    timeout(Duration::from_millis(200), sync.notify.notified())
        .await
        .expect("typed conversation update should trigger message sync");
    assert_eq!(
        sync.message_sync_calls().await,
        vec!["conversation-1".to_string()]
    );
}

#[tokio::test]
async fn recall_event_without_outer_conversation_id_publishes_local_message_conversation_id() {
    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let message_store = Arc::new(MemoryMessageStore::new());
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-1".to_string();
    message.conversation_id = "conversation-1".to_string();
    message_store.save_one(&message).await.unwrap();
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store,
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores),
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    );

    dispatcher
        .dispatch(DownlinkPayload::Event(Event {
            event_id: "recall-1".to_string(),
            conversation_id: String::new(),
            payload: Some(ProtoEventPayload::Recall(MessageRecallEvent {
                server_msg_id: "server-1".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        }))
        .await
        .unwrap();

    let event = timeout(Duration::from_millis(200), receiver.recv())
        .await
        .expect("expected recall event")
        .expect("bus closed");
    match event {
        SdkEvent::Message(MessageEvent::Recalled {
            conversation_id, ..
        }) => {
            assert_eq!(conversation_id, "conversation-1");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

/// Recall 先于消息到达：事件不得被吞（去重键须回滚），消息落库后重放同一事件应生效。
#[tokio::test]
async fn recall_event_before_message_arrival_is_forgotten_and_replayable() {
    let bus = EventBus::new();
    let mut receiver = bus.subscribe_raw();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let message_store = Arc::new(MemoryMessageStore::new());
    let stores = StoreProvider {
        plugin_commands: None,
        messages: message_store.clone(),
        conversations: Arc::new(NoopConversationStore),
        conversation_participants: None,
        cursors: Arc::new(NoopSyncCursorStore),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        None,
        Some(stores),
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    );

    let recall_event = Event {
        event_id: "recall-replay-1".to_string(),
        conversation_id: "conversation-1".to_string(),
        payload: Some(ProtoEventPayload::Recall(MessageRecallEvent {
            server_msg_id: "server-1".to_string(),
            ..Default::default()
        })),
        ..Default::default()
    };

    // 本地尚无目标消息：不应发布 Recalled（事件被留待重放）。
    dispatcher
        .dispatch(DownlinkPayload::Event(recall_event.clone()))
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(100), receiver.recv())
            .await
            .is_err(),
        "recall without local message must not publish Recalled"
    );

    // 消息到达后重放同一事件（同 event_id）：若去重键未回滚将被永久吞掉。
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = "server-1".to_string();
    message.conversation_id = "conversation-1".to_string();
    message_store.save_one(&message).await.unwrap();

    dispatcher
        .dispatch(DownlinkPayload::Event(recall_event))
        .await
        .unwrap();

    let event = timeout(Duration::from_millis(200), receiver.recv())
        .await
        .expect("replayed recall should publish Recalled")
        .expect("bus closed");
    match event {
        SdkEvent::Message(MessageEvent::Recalled {
            conversation_id, ..
        }) => {
            assert_eq!(conversation_id, "conversation-1");
        }
        other => panic!("unexpected event: {other:?}"),
    }
    let stored = message_store
        .get("server-1")
        .await
        .unwrap()
        .expect("message should exist");
    assert_eq!(
        stored.status,
        crate::model::message::MessageStatus::Recalled as i32
    );
}

#[tokio::test]
async fn waterline_uses_materialized_message_seq_not_cursor_seq() {
    let bus = EventBus::new();
    let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
    let sync = Arc::new(RecordingSessionSyncRunner::new());
    let conversations =
        Arc::new(crate::infrastructure::persistence::memory_im::MemoryConversationStore::new());
    let mut conversation =
        crate::model::Conversation::from_conversation_id("conversation-1".to_string());
    conversation.max_seq = 2;
    conversations.save_one(&conversation).await.unwrap();
    let stores = StoreProvider {
        plugin_commands: None,
        messages: Arc::new(MemoryMessageStore::new()),
        conversations,
        conversation_participants: None,
        cursors: Arc::new(CursorAtSeqStore { last_seq: 2 }),
        timeline_coverage: Arc::new(crate::storage::MemoryTimelineCoverageStore::new()),
        pending_send_reader: None,
        pending_send_writer: None,
        upload_manifest_store: None,
        media_cache_store: None,
        media_cache_admin: None,
        user_file_download_store: None,
        user_profiles_reader: None,
        user_profiles_writer: None,
    };
    let dispatcher = Dispatcher::new(
        bus.clone(),
        None,
        None,
        Some(sync.clone()),
        Some(stores),
        current_user_id,
        EventDeduper::new(Some(64)),
        test_notification_pipeline(bus),
        MetricsRecorder::disabled(),
    );

    dispatcher
        .dispatch(DownlinkPayload::EventEnvelope(EventEnvelope {
            conversation_id: "conversation-1".to_string(),
            max_conversation_seq: 2,
            ..Default::default()
        }))
        .await
        .unwrap();

    timeout(Duration::from_millis(200), sync.notify.notified())
        .await
        .expect("waterline must pull when cursor reached but message rows are missing");
    assert_eq!(
        sync.message_sync_calls().await,
        vec!["conversation-1".to_string()]
    );
}

#[test]
fn internal_gap_detection_uses_local_window() {
    assert_eq!(first_internal_gap_after(&[590, 591, 596]), Some(591));
    assert_eq!(first_internal_gap_after(&[590, 591, 592]), None);
    assert_eq!(first_internal_gap_after(&[0, 590, 590, 591]), None);
}

#[test]
fn seq_repair_cursor_stops_before_existing_local_tail_gap() {
    let cursor_seq = 100;
    let local_tail_and_incoming = [101, 103, 104];

    assert_eq!(
        max_contiguous_seq(cursor_seq, &local_tail_and_incoming),
        101
    );
    assert_eq!(
        first_gap_after(cursor_seq, &local_tail_and_incoming),
        Some(101)
    );
}

#[test]
fn internal_gap_detection_ignores_historical_gaps_before_base() {
    assert_eq!(
        first_internal_gap_after_from(752, &[1, 2, 752, 766]),
        Some(752)
    );
    assert_eq!(
        first_internal_gap_after_from(771, &[1, 2, 766, 771, 777]),
        Some(771)
    );
    assert_eq!(first_internal_gap_after_from(752, &[1, 2, 752, 753]), None);
}

#[test]
fn seq_repair_gap_prefers_recent_window_gap_over_stale_prefix_gap() {
    let seqs = [520, 521, 624, 644, 645];

    assert_eq!(seq_repair_gap_after(419, 624, &seqs), Some(624));
}

#[test]
fn seq_repair_gap_falls_back_to_prefix_gap_without_recent_gap() {
    let seqs = [421, 422, 423];

    assert_eq!(seq_repair_gap_after(419, 423, &seqs), Some(419));
}

/// 游标之下是**已经证明过**的连续区间，缺口判定不能再回头翻它。
///
/// 现场数字（会话 2AFZSMA4FJ4VPCVEC7，探针实测）：
/// 已读回执占掉 136/137，游标已推进到 137，本地最后一条消息行是 135，
/// 新消息落在 138 —— 137→138 本来就是连续的，却因为"最近缺口"从
/// local_before_seq=135 起算而报出 first_gap_after=135。
/// 后果是每条实时消息都触发一次补拉：每条消息每个在线端多一次读 RPC，
/// 在线人数越多放大越狠。
#[test]
fn seq_repair_ignores_holes_below_the_proven_cursor() {
    assert_eq!(seq_repair_gap_after(137, 135, &[135, 138]), None);
}

/// 反向：游标**之上**的真缺口仍必须报出来，否则漏消息。
#[test]
fn seq_repair_still_reports_gaps_above_the_cursor() {
    assert_eq!(seq_repair_gap_after(137, 135, &[135, 138, 140]), Some(138));
}

#[test]
fn seq_repair_backoff_is_exponential_and_capped() {
    assert_eq!(seq_repair_backoff_ms(1), 1_000);
    assert_eq!(seq_repair_backoff_ms(2), 2_000);
    assert_eq!(seq_repair_backoff_ms(3), 4_000);
    assert_eq!(seq_repair_backoff_ms(30), SEQ_REPAIR_MAX_BACKOFF_MS);
}

#[test]
fn seq_repair_state_prunes_expired_idle_entries() {
    let now = 20 * 60 * 1_000;
    let mut states = HashMap::from([
        (
            "expired".to_string(),
            SeqRepairState {
                updated_at_ms: now - SEQ_REPAIR_IDLE_TTL_MS - 1,
                next_attempt_at_ms: now - 1,
                ..Default::default()
            },
        ),
        (
            "backoff".to_string(),
            SeqRepairState {
                updated_at_ms: now - SEQ_REPAIR_IDLE_TTL_MS - 1,
                next_attempt_at_ms: now + 1_000,
                ..Default::default()
            },
        ),
        (
            "in-flight".to_string(),
            SeqRepairState {
                in_flight: true,
                updated_at_ms: now - SEQ_REPAIR_IDLE_TTL_MS - 1,
                ..Default::default()
            },
        ),
    ]);

    prune_seq_repair_state(&mut states, now);

    assert!(!states.contains_key("expired"));
    assert!(states.contains_key("backoff"));
    assert!(states.contains_key("in-flight"));
}

#[test]
fn seq_repair_state_trims_oldest_idle_entries_when_over_capacity() {
    let now = 1_000;
    let mut states = HashMap::with_capacity(SEQ_REPAIR_MAX_TRACKED_CONVERSATIONS + 2);
    for index in 0..(SEQ_REPAIR_MAX_TRACKED_CONVERSATIONS + 2) {
        states.insert(
            format!("conv-{index}"),
            SeqRepairState {
                updated_at_ms: now + index as u64,
                ..Default::default()
            },
        );
    }

    prune_seq_repair_state(&mut states, now);

    assert_eq!(states.len(), SEQ_REPAIR_MAX_TRACKED_CONVERSATIONS);
    assert!(!states.contains_key("conv-0"));
    assert!(!states.contains_key("conv-1"));
    assert!(states.contains_key("conv-2"));
}
