//! SDK 事件类型（按域分组，飞书级 IM 语义，便于内部扩展与跨语言绑定）
//!
//! 内部仅通过 Event 通信；对外暴露类型化回调 API（on_*），
//! 不暴露大 trait，便于 FFI / Swift / Kotlin / TypeScript 绑定。

use flare_proto::common::{
    CustomEvent, MarkEvent, MessageDeleteEvent, MessageRecallEvent, MessageRetentionExpiredEvent,
    MessageRetentionPurgedEvent, MessageRetentionScheduledEvent, PinEvent, PresenceHintPacket,
    ReadReceiptEvent, SendAck, TypingAggregatePacket, TypingStatePacket, UnmarkEvent, UnpinEvent,
};

use crate::kernel::{SdkState, SyncRunContext, SyncState};
use crate::model::{IMMessage, ViewUpdate};

// ========== 域事件枚举（Domain events） ==========

/// 连接域事件：连接生命周期、状态变更、服务端错误、被踢下线、登录凭证过期
///
/// 与飞书级 IM 对齐：支持多端登录冲突（被踢）、Token 过期需重新登录等场景。
#[derive(Clone, Debug)]
pub enum ConnectionEvent {
    /// 已连接（含认证通过）
    Connected,
    /// 断开连接（含原因描述）
    Disconnected { reason: String },
    /// 连接状态变更（Disconnected / Connecting / Connected / Ready / Reconnecting）
    StateChanged { state: SdkState },
    /// 同步状态变更（Idle / Syncing / CatchingUp / Error）
    SyncStateChanged { state: SyncState },
    /// 服务端返回错误（code + message）
    ServerError { code: i32, message: String },
    /// 重连中（attempt 为当前第几次重试）
    Reconnecting { attempt: u32 },
    /// 当前设备被服务端踢下线，会话随之终止、不自动重连，恢复要由应用重新登录。`reason` 为类型化原因：
    /// `DEVICE_CONFLICT`（同一账号在同一平台的新设备登录，即「已在别处登录」）、`TENANT_UNAVAILABLE`
    /// （所属租户已停用或正在删除），或服务端给出的其他踢线原因（如运维踢线 `DEVICE_KICKED`）
    KickedOff { reason: String },
    /// 登录凭证已过期，需要重新登录（如刷新 Token 或跳转登录页）
    TokenExpired { message: String },
}

/// 消息域事件：收到新消息、发送成功/失败、撤回、正在输入、批量新消息
///
/// 与飞书级 IM 对齐：单条与批量新消息、发送回执、撤回、Typing 等。
/// SDK 内部仅使用 IMMessage 流转。
#[derive(Clone, Debug)]
pub enum MessageEvent {
    /// 收到一条新消息（单聊/群聊推送或同步拉取）
    Received { message: Box<IMMessage> },
    /// 新消息批量（同步或批量推送时一次下发多条，减少回调次数）
    ReceivedBatch { messages: Vec<IMMessage> },
    /// 消息发送成功（服务端回执）
    SendAck { ack: Box<SendAck> },
    /// 消息发送失败（client_msg_id + 原因）
    SendFailed {
        client_msg_id: String,
        /// 原因文案（沿用至今的字符串，给人看）。
        reason: String,
        /// 结构化原因：服务端拒发时带 `ErrorDetail.reason`（`machine_reason`），本地失败给本地 code。
        error: crate::domain::SendFailure,
    },
    /// 消息被撤回（会话 id + 撤回事件体）
    Recalled {
        conversation_id: String,
        event: MessageRecallEvent,
    },
    /// 正在输入（DATA realtime_control.typing，不占用 conversation_seq）
    Typing {
        conversation_id: String,
        event: TypingStatePacket,
    },
    /// 网关聚合的"N 人正在输入"（超大群：服务端按会话聚合后下发，替代逐用户 Typing）。
    /// `event.typing_user_ids` 为采样姓名集合，`event.typing_count` 为总人数；客户端按自身 user_id 过滤显示。
    TypingAggregate {
        conversation_id: String,
        event: TypingAggregatePacket,
    },
    /// 消息正文已更新（本地编辑确认或服务端推送/同步下发后，本地库已写入新 `content`）
    Edited {
        conversation_id: String,
        server_msg_id: String,
    },
    /// 消息反应已变化（添加/移除）
    ReactionChanged {
        conversation_id: String,
        server_msg_id: String,
        user_id: String,
        emoji: String,
        /// 与 proto `ReactionAction` 对齐：1=ADD, 2=REMOVE
        action: i32,
    },
    /// 消息被删除（服务端事件）
    Deleted {
        conversation_id: String,
        event: MessageDeleteEvent,
    },
    /// 已读回执（服务端事件）
    ReadReceipt {
        conversation_id: String,
        event: ReadReceiptEvent,
    },
    /// Retention 生命周期已安排
    RetentionScheduled {
        conversation_id: String,
        event: MessageRetentionScheduledEvent,
    },
    /// Retention 生命周期已过期
    RetentionExpired {
        conversation_id: String,
        event: MessageRetentionExpiredEvent,
    },
    /// Retention 生命周期已清理
    RetentionPurged {
        conversation_id: String,
        event: MessageRetentionPurgedEvent,
    },
    /// 消息被置顶
    Pinned {
        conversation_id: String,
        event: PinEvent,
    },
    /// 消息取消置顶
    Unpinned {
        conversation_id: String,
        event: UnpinEvent,
    },
    /// 消息被标记
    Marked {
        conversation_id: String,
        event: MarkEvent,
    },
    /// 消息取消标记
    Unmarked {
        conversation_id: String,
        event: UnmarkEvent,
    },
    /// 在线状态提示（DATA realtime_control.presence，不占用 conversation_seq）
    PresenceChanged {
        conversation_id: String,
        event: PresenceHintPacket,
    },
    /// 自定义领域事件（custom）
    Custom {
        conversation_id: String,
        event: CustomEvent,
    },
    /// 插件卡片状态已写入本地消息（插件动作结果的 `messageState`）；宿主按一次消息更新重新呈现该条消息。
    /// `message_id` 是写入时使用的消息 ID（server_id 优先）。
    PluginStateChanged {
        conversation_id: String,
        message_id: String,
        version: u64,
    },
    /// 群聊里本人消息的已读摘要有了新值（核心 SDK 取回的）；打开的视图据此重读该会话，摘要随消息的
    /// `receipt.readSummary` 增量带出。`message_ids` 是摘要变了的消息（服务端 ID）；宿主自己读消息列表时
    /// 据此重读这几条。
    ReadSummariesChanged {
        conversation_id: String,
        message_ids: Vec<String>,
    },
}

/// 会话域事件：新会话、会话信息变更、未读数变化、会话删除、列表同步完成
///
/// 与飞书级 IM 对齐：会话创建、更新、未读变化、删除及全量同步完成。
#[derive(Clone, Debug)]
pub enum ConversationEvent {
    /// 会话列表全量同步完成（拉取到的会话 id 列表）
    Synced { conversation_ids: Vec<String> },
    /// 新会话（首次出现或服务端下发新建）
    Created { conversation_id: String },
    /// 会话信息变更（标题、置顶、未读等除未读数外的变更也可走此事件）
    Updated { conversation_id: String },
    /// 会话未读数量变化（本地已读或服务端推送未读更新）
    UnreadCountChanged {
        conversation_id: String,
        unread_count: u32,
    },
    /// 会话被删除
    Deleted { conversation_id: String },
}

/// 三层就绪阶段：本地优先三层收敛的可观察信号。
/// 始终对 UI 可见（即使底层 sync 为 Silent 的热启/重连），供 UI 决定何时出图 / 收起骨架。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadinessStage {
    /// T0 本地水合完成：本地缓存已可出图（会话列表 / 上次会话），无需等待网络。
    LocalReady,
    /// T1 前台新鲜度达成：首屏 top-N 与前台会话已与服务端对齐。
    ForegroundFresh,
    /// T2 环境收敛完成：长尾会话 / 已读 / 关键事件等已补齐。
    Converged,
    /// 降级：首屏部分失败，展示本地 / 空态，后台继续补偿（非永久 loading）。
    Degraded,
}

/// 同步域事件：状态、阶段、进度、任务完成/失败
///
/// 命名为 `SyncNotify`（非 `Sync`），避免与 Rust `std::marker::Sync` 及 wire 层 `flare_proto::common::Sync` 混淆。
#[derive(Clone, Debug)]
pub enum SyncNotify {
    /// Raw event subscriber queue overflowed. Consumers must refresh local
    /// read models from the SDK store because events may have been dropped.
    ResyncNeeded {
        scope: String,
        reason: String,
        dropped_events: u64,
    },
    StateChanged {
        run: SyncRunContext,
        state: SyncState,
    },
    Started {
        run: SyncRunContext,
    },
    Finished {
        run: SyncRunContext,
        phase: SyncPhase,
    },
    Failed {
        run: SyncRunContext,
        task: String,
        message: String,
    },
    Progress {
        run: SyncRunContext,
        task: String,
        progress: f32,
        detail: String,
    },
    TaskCompleted {
        run: SyncRunContext,
        task: String,
    },
    /// 三层就绪信号：始终对 UI 可见，即使底层 run 为 Silent（热启 / 重连）。
    Readiness {
        run: SyncRunContext,
        stage: ReadinessStage,
    },
}

impl SyncNotify {
    pub fn should_publish(&self) -> bool {
        match self {
            // 就绪信号与 resync 提示始终可见：UI 据此出图 / 刷新读模型，不受 sync 静默影响。
            Self::ResyncNeeded { .. } | Self::Readiness { .. } => true,
            Self::StateChanged { run, .. }
            | Self::Started { run }
            | Self::Finished { run, .. }
            | Self::Failed { run, .. }
            | Self::Progress { run, .. }
            | Self::TaskCompleted { run, .. } => run.visibility.emits_status_events(),
        }
    }

    pub fn is_user_visible(&self) -> bool {
        match self {
            // 就绪信号与 resync 提示始终可见：UI 据此出图 / 刷新读模型，不受 sync 静默影响。
            Self::ResyncNeeded { .. } | Self::Readiness { .. } => true,
            Self::StateChanged { run, .. }
            | Self::Started { run }
            | Self::Finished { run, .. }
            | Self::Failed { run, .. }
            | Self::Progress { run, .. }
            | Self::TaskCompleted { run, .. } => run.visibility.is_user_visible(),
        }
    }
}

/// 扩展域事件：业务自定义推送
#[derive(Clone, Debug)]
pub struct ExtensionEvent {
    pub source: String,
    pub event_type: String,
    pub payload: Vec<u8>,
}

/// A realtime packet a plugin delivered to this user's online connections
/// (`im.realtime.deliver`): online only, at most once, never stored. The SDK
/// drops expired packets and packets whose payload is not a JSON object.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginRealtime {
    /// The delivering plugin (`DataPacket.capability.capability_id`), e.g. `flare.call`.
    pub plugin_id: String,
    /// The plugin's packet type, e.g. `call.snapshot`.
    pub packet_type: String,
    /// The conversation the plugin delivered to.
    pub conversation_id: String,
    /// The plugin's JSON object payload.
    pub payload: serde_json::Map<String, serde_json::Value>,
    /// Core's id of this delivery.
    pub delivery_id: String,
    /// Milliseconds on the device clock after which the packet is stale: the
    /// server's expiry moved by the estimated clock offset, so hosts can
    /// compare it with their own clock.
    pub expires_at_ms: u64,
}

/// Plugin domain events.
#[derive(Clone, Debug)]
pub enum PluginEvent {
    /// A plugin realtime packet ([`PluginRealtime`]).
    Realtime(Box<PluginRealtime>),
    /// The tenant's plugin catalog changed (a plugin enabled, disabled or
    /// reconfigured). The SDK has already invalidated its catalog; hosts read
    /// their plugin entries again. Emitted after a random 0-3 s delay, with the
    /// notices of 3 s merged into one.
    CatalogChanged {
        /// The authorization projection version Core announced.
        version: String,
    },
}

/// Notification 域事件：IM 下行 `NotificationContent`（与聊天 `MessageEvent::Received` 分离）。
#[derive(Clone, Debug)]
pub enum NotificationEvent {
    Received { message: Box<IMMessage> },
}

/// 同步阶段（用于 SyncFinished）
#[derive(Clone, Debug)]
pub enum SyncPhase {
    Init,
    Background,
}

// ========== 顶层 SDK 事件（内部总线统一入口） ==========

/// 顶层 SDK 事件：按域聚合，内部总线与跨语言绑定均使用此枚举
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum SdkEvent {
    Connection(ConnectionEvent),
    Message(MessageEvent),
    Notification(NotificationEvent),
    Conversation(ConversationEvent),
    Sync(SyncNotify),
    View(ViewUpdate),
    Extension(ExtensionEvent),
    Plugin(PluginEvent),
}

#[cfg(test)]
mod tests {
    use super::{ReadinessStage, SyncNotify};
    use crate::kernel::SyncRunContext;

    #[test]
    fn readiness_is_always_user_visible_even_for_silent_run() {
        // 热启(warm_start) / 重连的 run 为 Silent，但三层就绪信号必须仍可见，
        // 否则会被 event_bus publish 的 silent-sync 丢弃规则吞掉，UI 收不到"可出图/已收敛"信号。
        for stage in [
            ReadinessStage::LocalReady,
            ReadinessStage::ForegroundFresh,
            ReadinessStage::Converged,
            ReadinessStage::Degraded,
        ] {
            let ev = SyncNotify::Readiness {
                run: SyncRunContext::warm_start(),
                stage,
            };
            assert!(
                ev.is_user_visible(),
                "readiness {stage:?} must stay visible for silent run"
            );
        }
    }
}
