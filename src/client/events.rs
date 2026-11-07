//! [`IMClient`] 事件订阅：一律委托 [`crate::kernel::event::EventBus`]，与消息/会话领域对齐。
//!
//! 专业 SDK 常见形态是「单入口 + 类型化 on_*」；实现保持薄转发即可，不必按域拆多个源文件。
//!
//! **注意**：若在 [`super::IMClient::login`] 的 `before_connect` 之前尚未挂上引擎，应优先在回调里用传入的 [`crate::kernel::event::EventBus`] 注册；连接后再用本文件的 `on_*` 亦可（见示例 `two_clients_chat`）。

use std::sync::Arc;

use flare_proto::common::{
    MessageRecallEvent, ReadReceiptEvent, SendAck, TypingAggregatePacket, TypingStatePacket,
};

use crate::application::notification::NotificationHandler;
use crate::client::IMClient;
use crate::kernel::SyncState;
use crate::kernel::event::{
    ConnectionStatus, CustomEventSelector, EventFilter, FilteredEventReceiver, PluginRealtime,
    SdkEventKind, SdkEventType, SharedEvent, Subscription, SyncPhase,
};
use crate::model::IMMessage;
use crate::shared::error::Result;

impl IMClient {
    // ========== 连接 ==========

    /// 连接状态（`connection://status`）：阶段、线路、质量、重试倒计时、需要用户动作的原因。
    /// 注册时已有状态的立即补收一次。这是唯一的连接事件：被踢、令牌不可续都以 `actionRequired` 出现。
    pub fn on_connection_status<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(ConnectionStatus) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_connection_status(f))
    }

    // ========== 同步引擎（会话列表 / 消息游标等任务）==========

    /// 同步状态机子状态变化（[`SyncState`]）；细粒度展示「正在同步会话 / 消息」等。
    pub fn on_sync_state_changed<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(SyncState) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_sync_state_changed(f))
    }

    /// 本轮同步任务**开始**（可能多次：冷启动、前后台切换等）。
    pub fn on_sync_started<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_sync_started(f))
    }

    /// 某一同步阶段**结束**；[`SyncPhase`] 区分前台全量、后台增量等，常用于「首屏同步完成」门禁。
    pub fn on_sync_finished<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(SyncPhase) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_sync_finished(f))
    }

    /// 同步任务**失败**；参数一般为 `(任务标识, 错误描述)`，可重试或上报。
    pub fn on_sync_failed<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(String, String) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_sync_failed(f))
    }

    /// 同步**进度**；`(任务标识, 0.0~1.0 进度, 附加上下文文案)`，用于进度条或日志。
    pub fn on_sync_progress<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(String, f32, String) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_sync_progress(f))
    }

    /// 单个同步子任务**完成**（如某一类数据拉取结束）；参数为任务名/类型字符串。
    pub fn on_sync_task_completed<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_sync_task_completed(f))
    }

    // ========== 消息（单条 / 批量 / 发送态 / 撤回 / 输入中）==========

    /// 收到**一条新消息**，内容为 [`IMMessage`]。
    ///
    /// ⚠️ **聊天消息不走这个回调，请用 [`Self::on_message_batch`]。**
    ///
    /// 入站聊天消息（推送与同步都一样）统一经 `NotificationInboundPipeline::finish_batch`
    /// 发布 `ReceivedBatch`——批量是入站的**规范路径**，且刻意不重放逐条回调
    /// （见 `finish_batch_publishes_batch_without_single_message_replay`）。
    /// 所以订阅本回调渲染聊天气泡会**一条都收不到**。
    ///
    /// 本回调实际只在「被提升为消息的通知」那条路径上触发
    /// （`should_publish_notification_as_message`）。
    ///
    /// 这里写清楚是因为它坑过人：文档原先写着「单聊/群聊共用」，
    /// 全仓消费方于是都订了它、都收不到消息，而各自的接收断言又都靠同步兜底，
    /// 测试全绿——直到有人真去数事件才发现。
    pub fn on_message<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&IMMessage) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_message(f))
    }

    /// 收到 **IM 下行 Notification**（含 `persistent=false` 的 BusinessEphemeral）；与聊天 `on_message` 分离。
    pub fn on_notification<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&IMMessage) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_notification(f))
    }

    /// 只订阅某个 `NotificationContent.notification_type` 的通知。
    pub fn on_notification_type<F>(
        &self,
        notification_type: impl Into<String>,
        f: F,
    ) -> Result<Subscription>
    where
        F: Fn(&IMMessage) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_notification_type(notification_type, f))
    }

    /// 注册一个通知处理器，处理器自身通过 [`NotificationHandler::matches`] 决定是否接收。
    pub async fn register_notification_handler(
        &self,
        handler: Arc<dyn NotificationHandler>,
    ) -> Result<()> {
        self.notification_handlers().await?.register(handler).await;
        Ok(())
    }

    /// 注册一个精确通知类型处理器；同一类型可注册多个处理器，都会被调用。
    pub async fn register_notification_type_handler(
        &self,
        notification_type: impl Into<String>,
        handler: Arc<dyn NotificationHandler>,
    ) -> Result<()> {
        self.notification_handlers()
            .await?
            .register_for_type(notification_type, handler)
            .await;
        Ok(())
    }

    /// 批量新消息（如同步一批历史）；比逐条 `on_message` 更高效，适合列表差量刷新。
    pub fn on_message_batch<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&[IMMessage]) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_message_batch(f))
    }

    /// 发送**成功回执**（服务端 ACK，含 `seq` / `server_msg_id` 等）；用于把本地发送中状态改为已送达。
    pub fn on_send_ack<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&SendAck) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_send_ack(f))
    }

    /// 发送**失败**；参数为 `(client_msg_id, 失败原因)`，用于气泡显示重试或失败态。
    pub fn on_send_failed<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str, &str) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_send_failed(f))
    }

    /// 消息被**撤回**；参数为 `(conversation_id, 撤回事件体)`，需更新本地消息列表展示。
    pub fn on_recalled<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str, &MessageRecallEvent) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_recalled(f))
    }

    /// **正在输入**；参数为 `(conversation_id, realtime_control.typing)`，用于展示「对方正在输入…」。
    pub fn on_typing<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str, &TypingStatePacket) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_typing(f))
    }

    /// **N 人正在输入**（超大群网关聚合）；参数为 `(conversation_id, typing_aggregate)`，
    /// `typing_user_ids` 为采样姓名集合、`typing_count` 为总人数，客户端按自身 user_id 过滤后展示。
    pub fn on_typing_aggregate<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str, &TypingAggregatePacket) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_typing_aggregate(f))
    }

    /// **已读回执**（已读光标）；参数为 `(conversation_id, read_receipt)`，
    /// `read_receipt.user_id` 已读至 `read_receipt.read_seq`，用于展示「对方已读」/已读人数。
    pub fn on_read_receipt<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str, &ReadReceiptEvent) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_read_receipt(f))
    }

    /// Plugin realtime packets (`im.realtime.deliver`); expired packets and
    /// non-object payloads never arrive.
    pub fn on_plugin_realtime<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&PluginRealtime) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_plugin_realtime(f))
    }

    /// The tenant's plugin catalog changed (after the SDK invalidated its
    /// catalog, a random 0-3 s later, notices of 3 s merged); the argument is
    /// the announced version.
    pub fn on_plugin_catalog_changed<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_plugin_catalog_changed(f))
    }

    // ========== 会话列表（元数据 / 未读 / 删除）==========

    /// 会话列表**同步完成**（或全量刷新）；参数为本次涉及的 `conversation_id` 列表。
    pub fn on_conversation_synced<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&[String]) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_conversation_synced(f))
    }

    /// **新建会话**（本地或远端）；参数为 `conversation_id`。
    pub fn on_conversation_created<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_conversation_created(f))
    }

    /// 会话**属性更新**（置顶、静音、草稿等）；参数为 `conversation_id`，可再拉取详情（如 `ConversationApi::get`）。
    pub fn on_conversation_updated<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_conversation_updated(f))
    }

    /// **未读数变化**；参数为 `(conversation_id, unread_count)`，用于角标与列表红点。
    pub fn on_conversation_unread_count_changed<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str, u32) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_conversation_unread_count_changed(f))
    }

    /// 会话**删除**（本地或同步删除）；参数为 `conversation_id`。
    pub fn on_conversation_deleted<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_conversation_deleted(f))
    }

    // ========== 扩展推送 / 通配监听 ==========

    /// **自定义扩展**下行；`(业务类型, 子类型, payload 字节)`，与核心消息并行，供音视频信令、运营活动等。
    pub fn on_extension<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(&str, &str, &[u8]) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_extension(f))
    }

    /// 只订阅某个扩展源和事件类型的扩展事件。
    pub fn on_extension_event<F>(
        &self,
        source: impl Into<String>,
        event_type: impl Into<String>,
        f: F,
    ) -> Result<Subscription>
    where
        F: Fn(&str, &str, &[u8]) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_extension_event(source, event_type, f))
    }

    /// 只订阅某个 durable `CustomEvent`。
    pub fn on_custom_event<F>(
        &self,
        selector: impl Into<CustomEventSelector>,
        f: F,
    ) -> Result<Subscription>
    where
        F: Fn(&str, &flare_proto::common::CustomEvent) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_custom_event(selector, f))
    }

    /// **所有**领域事件的通配入口（[`SharedEvent`] = `Arc<SdkEvent>`）；适合日志、调试或统一路由；生产环境注意性能与过滤。
    pub fn on_any<F>(&self, f: F) -> Result<Subscription>
    where
        F: Fn(SharedEvent) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_any(f))
    }

    /// 按过滤器注册事件回调。多个相同过滤器的回调都会被执行。
    pub fn on_event_filter<F>(&self, filter: impl Into<EventFilter>, f: F) -> Result<Subscription>
    where
        F: Fn(SharedEvent) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_event_filter(filter, f))
    }

    /// 按顶层事件域注册事件回调。
    pub fn on_event_kind<F>(&self, kind: SdkEventKind, f: F) -> Result<Subscription>
    where
        F: Fn(SharedEvent) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_event_kind(kind, f))
    }

    /// 按精确事件类型注册事件回调。
    pub fn on_event_type<F>(&self, event_type: SdkEventType, f: F) -> Result<Subscription>
    where
        F: Fn(SharedEvent) + Send + Sync + 'static,
    {
        self.with_engine(|e| e.bus().on_event_type(event_type, f))
    }

    /// 获取过滤后的原始事件接收器。
    pub fn subscribe_event_filter(
        &self,
        filter: impl Into<EventFilter>,
    ) -> Result<FilteredEventReceiver> {
        self.with_engine(|e| e.bus().subscribe_filter(filter))
    }

    /// 获取某个顶层事件域的原始事件接收器。
    pub fn subscribe_event_kind(&self, kind: SdkEventKind) -> Result<FilteredEventReceiver> {
        self.with_engine(|e| e.bus().subscribe_kind(kind))
    }

    /// 获取某个精确事件类型的原始事件接收器。
    pub fn subscribe_event_type(&self, event_type: SdkEventType) -> Result<FilteredEventReceiver> {
        self.with_engine(|e| e.bus().subscribe_event_type(event_type))
    }
}
