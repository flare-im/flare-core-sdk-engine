//! 可靠队列 Actor — 单任务循环：enqueue / ack / timeout → 转移

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flare_proto::common::SendAck;
use rand::Rng;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::domain::{
    ConversationStore, DeliveryLocalSnapshot, InFlightReconcileDecision, MessageDeliveryService,
    MessageStore, PendingDispatchDecision, PendingSendReader, PendingSendVo, PendingSendWriter,
    REASON_ORPHAN_RECOVERED, RetryDecision,
};
use crate::infrastructure::protocol::PacketSender;
use crate::infrastructure::transport::socket::LinkProbe;
use crate::kernel::event::{EventBus, MessageEvent, SdkEvent};
use crate::kernel::{CurrentUserIdStore, ReliableSendQueuePort};
use crate::model::IMMessage;
use crate::model::message::{MessageLocalState, MessageStatus};
use crate::shared::error::{ErrorCode, FlareError, Result};
use crate::shared::util::id;
use crate::shared::util::spawn_background_task;
use crate::shared::util::time::{deadline_after, delay, is_deadline_elapsed};
use crate::shared::util::{
    RELIABLE_QUEUE_MAX_IN_FLIGHT, RELIABLE_QUEUE_MAX_RETRIES, RELIABLE_QUEUE_TIMEOUT_SECS,
};
use crate::spi::metrics::{MetricLabel, MetricsRecorder};

const ADAPTIVE_INITIAL_MAX_IN_FLIGHT: usize = 4;
const ADAPTIVE_MIN_IN_FLIGHT: usize = 1;

/// 帧交给传输层后这么久还没等到 SendAck，就怀疑连接已经半开，请求一次验活。
///
/// 正常往返远小于它；它又远短于 ACK 超时（默认 10 s）与心跳超时（前台 90 s），
/// 半开连接能在下一次重发之前被戳穿。
const SEND_STALL_PROBE_AFTER: Duration = Duration::from_secs(5);
/// 两次验活至少相隔这么久：一批消息同时卡住只验一次，连接健康而服务端慢时也不会每条都验。
/// 换了新连接（[`recover_pending_for_current_user`]）立即解除。
const SEND_STALL_PROBE_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// 「发出去迟迟没回执」触发验活的去抖状态。
#[derive(Debug, Default)]
struct SendStallProbeGate {
    /// 只看这之后发出的尝试：更早的帧要么走的是旧连接，要么已被上一次验活覆盖，
    /// 它们没回执说明不了当前连接的死活。
    since_ms: u64,
    last_probe_ms: Option<u64>,
}

impl SendStallProbeGate {
    fn should_probe(&self, now_ms: u64, sent_at_ms: impl IntoIterator<Item = u64>) -> bool {
        if let Some(last) = self.last_probe_ms
            && now_ms.saturating_sub(last) < SEND_STALL_PROBE_MIN_INTERVAL.as_millis() as u64
        {
            return false;
        }
        let stall_ms = SEND_STALL_PROBE_AFTER.as_millis() as u64;
        sent_at_ms
            .into_iter()
            .any(|sent| sent >= self.since_ms && now_ms.saturating_sub(sent) >= stall_ms)
    }

    fn record_probe(&mut self, now_ms: u64) {
        self.last_probe_ms = Some(now_ms);
        self.since_ms = now_ms;
    }

    /// 新连接建立：旧连接上的尝试不再作数，去抖窗口清零。
    fn on_new_connection(&mut self, now_ms: u64) {
        self.last_probe_ms = None;
        self.since_ms = now_ms;
    }
}

/// 队列命令（仅通过此与队列通信）
#[derive(Debug)]
enum QueueCommand {
    /// 入队发送（SDK 内部统一 IMMessage）
    Enqueue {
        message: Box<IMMessage>,
        resp: oneshot::Sender<Result<()>>,
    },
    /// 收到 SendAck（由外部收到下行后注入）
    AckReceived(Box<SendAck>),
    /// 异步发送尝试完成；结果回流到 actor,由 actor 统一处理重试/失败状态。
    SendAttemptFinished {
        entry: Box<PendingSendVo>,
        retries: u32,
        result: Result<()>,
    },
    /// 登录时清空队列（包含 in_flight + pending），并将本地消息收敛为 failed
    ResetPendingOnLogin {
        resp: oneshot::Sender<Result<Vec<String>>>,
    },
    /// 连接/冷启动后：恢复当前账号下的 pending 队列，并做自愈扫描
    RecoverPendingForCurrentUser {
        resp: oneshot::Sender<Result<Vec<String>>>,
    },
    /// 暂时性拒绝的退避到期，该条消息可以重发了（`retries` 用来识别过期的到期通知）。
    RetryBackoffElapsed { client_msg_id: String, retries: u32 },
    /// Answered once every command sent before it has been handled: the actor handles commands
    /// one at a time, in the order they were sent. Lets tests wait for the queue, not the clock.
    #[cfg(all(test, feature = "storage-sqlite"))]
    Handled(oneshot::Sender<()>),
}

/// 可靠发送队列句柄（发命令 + 持有 actor 所需依赖）
pub struct ReliableSendQueue {
    tx: mpsc::Sender<QueueCommand>,
    worker: crate::shared::util::BackgroundTask,
}

pub struct ReliableSendQueueConfig {
    pub pending_reader: Arc<dyn PendingSendReader>,
    pub pending_writer: Arc<dyn PendingSendWriter>,
    pub sender: Arc<PacketSender>,
    pub message_store: Arc<dyn MessageStore>,
    pub conversation_store: Arc<dyn ConversationStore>,
    pub current_user_id: CurrentUserIdStore,
    pub bus: EventBus,
    pub timeout_secs: Option<u64>,
    pub max_retries: Option<u32>,
    pub max_in_flight: Option<usize>,
    pub metrics: MetricsRecorder,
}

struct QueueState {
    pending_reader: Arc<dyn PendingSendReader>,
    pending_writer: Arc<dyn PendingSendWriter>,
    command_tx: mpsc::Sender<QueueCommand>,
    sender: Arc<PacketSender>,
    message_store: Arc<dyn MessageStore>,
    conversation_store: Arc<dyn ConversationStore>,
    current_user_id: CurrentUserIdStore,
    bus: EventBus,
    timeout_duration: Duration,
    max_retries: u32,
    max_in_flight: usize,
    adaptive_window: AdaptiveInFlightWindow,
    /// 当前在途消息；ACK 按 client_msg_id 精确收敛。
    in_flight: HashMap<String, InFlightSend>,
    /// 已被服务端接收但尚未持久化确认的消息；释放发送窗口，但保留 pending 以便恢复。
    awaiting_durable: HashMap<String, AwaitingDurableAck>,
    /// client_msg_id -> 已重试次数
    retry_count: HashMap<String, u32>,
    /// 被服务端暂时性拒绝、正在退避等待重发的消息：client_msg_id -> 排定重发时的重试次数。
    /// 退避期间不占发送窗口，也不会被 `try_send_next` 提前捡起。
    retry_backoff: HashMap<String, u32>,
    metrics: MetricsRecorder,
    link_probe: Option<Arc<dyn LinkProbe>>,
    stall_probe_gate: SendStallProbeGate,
}

struct InFlightSend {
    entry: PendingSendVo,
    deadline_ms: u64,
    /// 本次尝试交给传输层（或收到服务端非终态回执）的时刻，判断「发出后多久没回执」用。
    sent_at_ms: u64,
    /// 上一次发送尝试没能把帧交出去（未连接、写失败、发送超时）。连接恢复（
    /// [`recover_pending_for_current_user`]）时立即重发，不必等到 `deadline_ms`：长连接断开后
    /// 发送直接失败，不再由传输层自己重连，重连只走会话的重连监视器。
    send_failed: bool,
}

struct AwaitingDurableAck {
    entry: PendingSendVo,
    deadline_ms: u64,
}

#[derive(Debug, Clone)]
struct AdaptiveInFlightWindow {
    current: usize,
    max: usize,
    consecutive_successes: usize,
}

impl AdaptiveInFlightWindow {
    fn new(max: usize) -> Self {
        let max = max.max(ADAPTIVE_MIN_IN_FLIGHT);
        Self {
            current: max.clamp(ADAPTIVE_MIN_IN_FLIGHT, ADAPTIVE_INITIAL_MAX_IN_FLIGHT),
            max,
            consecutive_successes: 0,
        }
    }

    fn current(&self) -> usize {
        self.current
    }

    fn note_success(&mut self) -> bool {
        if self.current >= self.max {
            self.consecutive_successes = 0;
            return false;
        }
        self.consecutive_successes = self.consecutive_successes.saturating_add(1);
        if self.consecutive_successes < self.current {
            return false;
        }
        self.current = (self.current + 1).min(self.max);
        self.consecutive_successes = 0;
        true
    }

    fn note_congestion(&mut self) -> bool {
        self.consecutive_successes = 0;
        let next = (self.current / 2).max(ADAPTIVE_MIN_IN_FLIGHT);
        if next == self.current {
            return false;
        }
        self.current = next;
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckApplyResult {
    Terminal,
    KeepInFlight,
    AwaitDurable,
    /// 服务端暂时性拒绝：已排定退避后重发，调用方不再持有这一条。
    RetryScheduled,
}

impl ReliableSendQueue {
    /// 构建队列并启动后台任务；收到 ack 需由调用方往 `on_ack` 注入。
    /// reader 与 writer 通常为同一实现体（如 SqlitePendingSendRepo）的两个 Arc。
    pub fn new(config: ReliableSendQueueConfig) -> Self {
        Self::spawn(config, None)
    }

    /// 同 [`Self::new`]，另装一个验活出口：帧发出后迟迟等不到 SendAck 时请求对当前连接验活
    /// （见 [`probe_link_if_send_stalled`]）。引擎建队列时用它；不装则不做这项检查。
    pub fn with_link_probe(
        config: ReliableSendQueueConfig,
        link_probe: Arc<dyn LinkProbe>,
    ) -> Self {
        Self::spawn(config, Some(link_probe))
    }

    fn spawn(config: ReliableSendQueueConfig, link_probe: Option<Arc<dyn LinkProbe>>) -> Self {
        let ReliableSendQueueConfig {
            pending_reader,
            pending_writer,
            sender,
            message_store,
            conversation_store,
            current_user_id,
            bus,
            timeout_secs,
            max_retries,
            max_in_flight,
            metrics,
        } = config;
        static QUEUE_SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let queue_id = QUEUE_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug!(queue_id, "reliable queue instance created");
        let (tx, mut rx) = mpsc::channel::<QueueCommand>(256);
        let timeout_duration =
            Duration::from_secs(timeout_secs.unwrap_or(RELIABLE_QUEUE_TIMEOUT_SECS));
        let max_retries = max_retries.unwrap_or(RELIABLE_QUEUE_MAX_RETRIES);
        let max_in_flight = max_in_flight
            .unwrap_or(RELIABLE_QUEUE_MAX_IN_FLIGHT)
            .clamp(1, 1024);
        let adaptive_window = AdaptiveInFlightWindow::new(max_in_flight);

        let mut state = QueueState {
            pending_reader,
            pending_writer,
            command_tx: tx.clone(),
            sender,
            message_store,
            conversation_store,
            current_user_id,
            bus,
            timeout_duration,
            max_retries,
            max_in_flight,
            adaptive_window,
            in_flight: HashMap::new(),
            awaiting_durable: HashMap::new(),
            retry_count: HashMap::new(),
            retry_backoff: HashMap::new(),
            metrics,
            link_probe,
            stall_probe_gate: SendStallProbeGate::default(),
        };
        state
            .metrics
            .gauge("reliable_queue.max_in_flight", max_in_flight as i64);
        state.metrics.gauge(
            "reliable_queue.adaptive_in_flight",
            state.adaptive_window.current() as i64,
        );

        let worker = spawn_background_task(async move {
            loop {
                tokio::select! {
                    Some(cmd) = rx.recv() => {
                        debug!(queue_id, "reliable queue received command");
                        if let Err(e) = handle_command(&mut state, cmd).await {
                            warn!(%e, "reliable queue command error");
                        }
                    }
                    _ = delay(Duration::from_secs(1)) => {
                        if let Err(e) = check_timeout(&mut state).await {
                            warn!(%e, "reliable queue timeout check error");
                        }
                        probe_link_if_send_stalled(&mut state);
                    }
                    else => break,
                }
            }
        });

        Self { tx, worker }
    }

    /// 入队发送（持久化后由 worker 按序发送，SDK 内部仅 IMMessage）
    pub async fn enqueue(&self, message: IMMessage) -> Result<()> {
        let (resp, rx) = oneshot::channel();
        self.tx
            .send(QueueCommand::Enqueue {
                message: Box::new(message),
                resp,
            })
            .await
            .map_err(|_| {
                FlareError::localized(ErrorCode::InternalError, "reliable queue closed")
            })?;
        rx.await
            .map_err(|_| FlareError::localized(ErrorCode::InternalError, "reliable queue closed"))?
    }

    /// 将收到的 SendAck 注入队列（由 Dispatcher 或 Engine 在收到下行 ack 时调用）
    pub async fn on_ack(&self, ack: SendAck) -> Result<()> {
        self.tx
            .send(QueueCommand::AckReceived(Box::new(ack)))
            .await
            .map_err(|_| FlareError::localized(ErrorCode::InternalError, "reliable queue closed"))
    }

    /// 登录专用：原子清空 in_flight + pending，避免会话切换时脏队列阻塞新消息。
    pub async fn reset_pending_on_login(&self) -> Result<Vec<String>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(QueueCommand::ResetPendingOnLogin { resp: tx })
            .await
            .map_err(|_| {
                FlareError::localized(ErrorCode::InternalError, "reliable queue closed")
            })?;
        rx.await.map_err(|_| {
            FlareError::localized(
                ErrorCode::InternalError,
                "reliable queue reset response dropped",
            )
        })?
    }

    /// 冷启动/显式 connect 后：恢复当前账号下的 pending 队列，并自愈孤儿 sending / 跨账号脏 pending。
    pub async fn recover_pending_for_current_user(&self) -> Result<Vec<String>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(QueueCommand::RecoverPendingForCurrentUser { resp: tx })
            .await
            .map_err(|_| {
                FlareError::localized(ErrorCode::InternalError, "reliable queue closed")
            })?;
        rx.await.map_err(|_| {
            FlareError::localized(
                ErrorCode::InternalError,
                "reliable queue recover response dropped",
            )
        })?
    }

    pub(crate) fn shutdown(&self) {
        self.worker.abort();
    }
}

#[async_trait::async_trait]
impl ReliableSendQueuePort for ReliableSendQueue {
    async fn enqueue(&self, message: IMMessage) -> Result<()> {
        ReliableSendQueue::enqueue(self, message).await
    }

    async fn on_ack(&self, ack: SendAck) -> Result<()> {
        ReliableSendQueue::on_ack(self, ack).await
    }

    async fn reset_pending_on_login(&self) -> Result<Vec<String>> {
        ReliableSendQueue::reset_pending_on_login(self).await
    }

    async fn recover_pending_for_current_user(&self) -> Result<Vec<String>> {
        ReliableSendQueue::recover_pending_for_current_user(self).await
    }
}

impl Drop for ReliableSendQueue {
    fn drop(&mut self) {
        // Drop JoinHandle 会 detach 任务；显式 abort 才能在会话结束时停止旧队列重试。
        self.shutdown();
    }
}

async fn handle_command(st: &mut QueueState, cmd: QueueCommand) -> Result<()> {
    match cmd {
        QueueCommand::Enqueue { message: msg, resp } => {
            st.metrics.counter("reliable_queue.enqueue_total", 1);
            let msg = *msg;
            let enqueued_at_ms = id::now_millis();
            let mut optimistic = msg.clone();
            optimistic.server_id = msg.client_msg_id.clone();
            optimistic.local_state = MessageLocalState {
                sending: true,
                failed: false,
                is_local: true,
                uploading: false,
                upload_progress: 0,
                sort_ts: enqueued_at_ms,
            };
            let entry = PendingSendVo {
                client_msg_id: msg.client_msg_id.clone(),
                conversation_id: msg.conversation_id.clone(),
                message: msg,
                enqueued_at_ms,
            };
            let (message_store, pending_writer) =
                (st.message_store.clone(), st.pending_writer.clone());
            if let Err(e) = message_store
                .save_batch(std::slice::from_ref(&optimistic))
                .await
            {
                let _ = resp.send(Err(e));
                return Ok(());
            }
            if let Err(e) = pending_writer.push(entry).await {
                let _ = resp.send(Err(e));
                return Ok(());
            }
            // 写入后立刻用**队列自己的 reader** 数一遍：若这里是 0，
            // 说明 reader 与 writer 不是同一份数据（实例分裂），
            // 而不是排除/限流逻辑的问题。
            {
                let visible = st
                    .pending_reader
                    .list_oldest_excluding(&[], 100)
                    .await
                    .map(|v| v.len())
                    .unwrap_or(usize::MAX);
                debug!(
                    visible_to_reader = visible,
                    "reliable queue pending visible right after push"
                );
            }
            if let Err(e) = update_conversation_last_message(st, &optimistic).await {
                warn!(%e, conversation_id = %optimistic.conversation_id, "update optimistic conversation projection failed");
            }
            st.bus
                .publish(SdkEvent::Message(MessageEvent::ReceivedBatch {
                    messages: vec![optimistic],
                }));
            let _ = resp.send(Ok(()));
            try_send_next(st).await?;
        }
        QueueCommand::AckReceived(ack) => {
            st.metrics.counter("reliable_queue.ack_received_total", 1);
            let ack = *ack;
            if let Some(in_flight) = st.in_flight.remove(&ack.client_msg_id) {
                match apply_ack_and_publish(st, &in_flight.entry, ack).await {
                    AckApplyResult::Terminal | AckApplyResult::RetryScheduled => {}
                    AckApplyResult::KeepInFlight => {
                        // 服务端回了话（虽未终态）：连接是活的，「没回执」从现在重新计时。
                        st.in_flight.insert(
                            in_flight.entry.client_msg_id.clone(),
                            InFlightSend {
                                sent_at_ms: id::now_millis(),
                                ..in_flight
                            },
                        );
                    }
                    AckApplyResult::AwaitDurable => {
                        st.awaiting_durable.insert(
                            in_flight.entry.client_msg_id.clone(),
                            AwaitingDurableAck {
                                entry: in_flight.entry,
                                deadline_ms: deadline_after(st.timeout_duration),
                            },
                        );
                    }
                }
            } else if let Some(awaiting) = st.awaiting_durable.remove(&ack.client_msg_id) {
                match apply_ack_and_publish(st, &awaiting.entry, ack).await {
                    AckApplyResult::Terminal | AckApplyResult::RetryScheduled => {}
                    AckApplyResult::KeepInFlight | AckApplyResult::AwaitDurable => {
                        st.awaiting_durable.insert(
                            awaiting.entry.client_msg_id.clone(),
                            AwaitingDurableAck {
                                entry: awaiting.entry,
                                deadline_ms: awaiting.deadline_ms,
                            },
                        );
                    }
                }
            } else if let Some(entry) = st.pending_reader.get(&ack.client_msg_id).await? {
                // Still queued but not in flight: waiting out a retry backoff, or for a free send
                // slot. Settling it takes no slot, so it happens now rather than when the entry
                // would next be picked to send; until then the message would show as sending
                // although the server already answered for it.
                st.retry_backoff.remove(&entry.client_msg_id);
                match apply_ack_and_publish(st, &entry, ack).await {
                    AckApplyResult::Terminal | AckApplyResult::RetryScheduled => {}
                    AckApplyResult::KeepInFlight => {
                        st.in_flight.insert(
                            entry.client_msg_id.clone(),
                            InFlightSend {
                                entry,
                                deadline_ms: deadline_after(st.timeout_duration),
                                sent_at_ms: id::now_millis(),
                                send_failed: false,
                            },
                        );
                    }
                    AckApplyResult::AwaitDurable => {
                        st.awaiting_durable.insert(
                            entry.client_msg_id.clone(),
                            AwaitingDurableAck {
                                entry,
                                deadline_ms: deadline_after(st.timeout_duration),
                            },
                        );
                    }
                }
            } else if MessageDeliveryService::accepted_from_ack(&ack).is_some()
                || MessageDeliveryService::error_message_from_ack(&ack).is_some()
            {
                if let Err(error) = apply_orphan_durable_ack(st, &ack).await {
                    warn!(
                        %error,
                        client_msg_id = %ack.client_msg_id,
                        "apply orphan durable ack failed"
                    );
                }
                st.bus.publish(SdkEvent::Message(MessageEvent::SendAck {
                    ack: Box::new(ack),
                }));
            }
            try_send_next(st).await?;
        }
        QueueCommand::SendAttemptFinished {
            entry,
            retries,
            result,
        } => {
            handle_send_attempt_finished(st, *entry, retries, result).await?;
        }
        QueueCommand::ResetPendingOnLogin { resp } => {
            let result = reset_pending_on_login(st).await;
            let _ = resp.send(result);
        }
        QueueCommand::RecoverPendingForCurrentUser { resp } => {
            let result = recover_pending_for_current_user(st).await;
            let _ = resp.send(result);
        }
        QueueCommand::RetryBackoffElapsed {
            client_msg_id,
            retries,
        } => {
            // 只认最近一次排定的到期；期间被登录重置、跨账号清理过的条目已不在表里。
            if st.retry_backoff.get(&client_msg_id) == Some(&retries) {
                st.retry_backoff.remove(&client_msg_id);
                debug!(%client_msg_id, retries, "reliable queue retry backoff elapsed");
            }
            try_send_next(st).await?;
        }
        #[cfg(all(test, feature = "storage-sqlite"))]
        QueueCommand::Handled(done) => {
            let _ = done.send(());
        }
    }
    record_queue_health(st);
    Ok(())
}

fn record_queue_health(st: &QueueState) {
    st.metrics
        .gauge("reliable_queue.in_flight", st.in_flight.len() as i64);
    st.metrics.gauge(
        "reliable_queue.awaiting_durable",
        st.awaiting_durable.len() as i64,
    );
    st.metrics.gauge(
        "reliable_queue.retry_backoff",
        st.retry_backoff.len() as i64,
    );
    st.metrics.gauge(
        "reliable_queue.adaptive_in_flight",
        st.adaptive_window.current() as i64,
    );
}

fn note_send_success(st: &mut QueueState) {
    if st.adaptive_window.note_success() {
        let current = st.adaptive_window.current();
        debug!(
            current,
            max = st.max_in_flight,
            "reliable queue adaptive in-flight window increased"
        );
        st.metrics
            .gauge("reliable_queue.adaptive_in_flight", current as i64);
    }
}

fn note_send_congestion(st: &mut QueueState, reason: &'static str) {
    if st.adaptive_window.note_congestion() {
        let current = st.adaptive_window.current();
        debug!(
            current,
            max = st.max_in_flight,
            reason,
            "reliable queue adaptive in-flight window reduced"
        );
        st.metrics
            .gauge("reliable_queue.adaptive_in_flight", current as i64);
    }
}

async fn mark_send_failed_and_publish(
    st: &mut QueueState,
    entry: &PendingSendVo,
    reason: &str,
) -> Result<()> {
    let _ = st.pending_writer.pop(&entry.client_msg_id).await?;
    st.retry_count.remove(&entry.client_msg_id);

    let failed_msg = MessageDeliveryService::mark_failed(&entry.message);
    st.message_store.save_batch(&[failed_msg]).await?;
    st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
        client_msg_id: entry.client_msg_id.clone(),
        reason: reason.to_string(),
        error: crate::domain::SendFailure::local_reason(reason),
    }));
    Ok(())
}

fn dispatch_send_attempt(st: &mut QueueState, entry: PendingSendVo, retries: u32) {
    st.metrics.counter_with_labels(
        "reliable_queue.send_attempt_total",
        &[MetricLabel::new("retry", retries.to_string())],
        1,
    );
    let client_msg_id = entry.client_msg_id.clone();
    st.in_flight.insert(
        client_msg_id,
        InFlightSend {
            entry: entry.clone(),
            deadline_ms: deadline_after(st.timeout_duration),
            sent_at_ms: id::now_millis(),
            send_failed: false,
        },
    );

    let sender = st.sender.clone();
    let command_tx = st.command_tx.clone();
    let dispatch_id = entry.client_msg_id.clone();
    debug!(client_msg_id = %dispatch_id, retries, "reliable queue dispatching send attempt");
    let _ = spawn_background_task(async move {
        let result = do_send_one(&sender, &entry).await;
        debug!(
            client_msg_id = %entry.client_msg_id,
            ok = result.is_ok(),
            "reliable queue send attempt finished"
        );
        let _ = command_tx
            .send(QueueCommand::SendAttemptFinished {
                entry: Box::new(entry),
                retries,
                result,
            })
            .await;
    });
}

async fn handle_send_attempt_finished(
    st: &mut QueueState,
    entry: PendingSendVo,
    retries: u32,
    result: Result<()>,
) -> Result<()> {
    if result.is_ok() {
        return Ok(());
    }
    let current_retries = st
        .retry_count
        .get(&entry.client_msg_id)
        .copied()
        .unwrap_or(0);
    if current_retries != retries {
        return Ok(());
    }
    let Some(in_flight) = st.in_flight.remove(&entry.client_msg_id) else {
        return Ok(());
    };
    let entry = in_flight.entry;
    let Err(error) = result else {
        return Ok(());
    };
    note_send_congestion(st, "send_error");
    match MessageDeliveryService::decide_send_attempt_failure(retries, st.max_retries) {
        RetryDecision::Retry { next_retry_count } => {
            st.metrics.counter("reliable_queue.send_retry_total", 1);
            st.retry_count
                .insert(entry.client_msg_id.clone(), next_retry_count);
            warn!(
                %error,
                client_msg_id = %entry.client_msg_id,
                retries = next_retry_count,
                "send attempt failed, keep entry in pending queue for retry"
            );
            st.in_flight.insert(
                entry.client_msg_id.clone(),
                InFlightSend {
                    entry,
                    deadline_ms: deadline_after(st.timeout_duration),
                    sent_at_ms: id::now_millis(),
                    send_failed: true,
                },
            );
        }
        RetryDecision::Fail { reason } => {
            st.metrics.counter("reliable_queue.send_failed_total", 1);
            let _ = st.pending_writer.pop(&entry.client_msg_id).await;
            st.retry_count.remove(&entry.client_msg_id);
            let failed_msg = MessageDeliveryService::mark_failed(&entry.message);
            if let Err(save_err) = st.message_store.save_batch(&[failed_msg]).await {
                warn!(%save_err, "persist failed message state failed");
            }
            st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
                client_msg_id: entry.client_msg_id,
                reason: reason.to_string(),
                error: crate::domain::SendFailure::local_reason(reason),
            }));
            try_send_next(st).await?;
        }
    }
    Ok(())
}

async fn reset_pending_on_login(st: &mut QueueState) -> Result<Vec<String>> {
    let mut dropped = Vec::new();
    // 退避中的条目仍在 pending 里，下面按 pending 一并判失败；这里只撤掉它们的重发排期。
    st.retry_backoff.clear();

    let in_flight_entries = st
        .in_flight
        .drain()
        .map(|(_, in_flight)| in_flight.entry)
        .collect::<Vec<_>>();
    for in_flight in in_flight_entries {
        let id = in_flight.client_msg_id.clone();
        mark_send_failed_and_publish(
            st,
            &in_flight,
            "pending queue dropped during login session reset",
        )
        .await?;
        dropped.push(id);
    }
    let awaiting_entries = st
        .awaiting_durable
        .drain()
        .map(|(_, awaiting)| awaiting.entry)
        .collect::<Vec<_>>();
    for awaiting in awaiting_entries {
        let id = awaiting.client_msg_id.clone();
        mark_send_failed_and_publish(
            st,
            &awaiting,
            "server accepted queue dropped during login session reset",
        )
        .await?;
        dropped.push(id);
    }

    let pending_entries = st.pending_reader.list().await?;
    for entry in pending_entries {
        mark_send_failed_and_publish(
            st,
            &entry,
            "pending queue dropped during login session reset",
        )
        .await?;
        dropped.push(entry.client_msg_id);
    }

    dropped.sort();
    dropped.dedup();
    Ok(dropped)
}

async fn recover_pending_for_current_user(st: &mut QueueState) -> Result<Vec<String>> {
    // 连接（重新）建好后调用：旧连接上没回执的尝试不再作数，新连接可以立即被验活。
    st.stall_probe_gate.on_new_connection(id::now_millis());
    let current_user_id = st.current_user_id.read().await.clone();
    if current_user_id.trim().is_empty() {
        return Ok(Vec::new());
    }

    let pending_entries = st.pending_reader.list().await?;
    let pending_ids: Vec<String> = pending_entries
        .iter()
        .map(|entry| entry.client_msg_id.clone())
        .collect();

    let mut affected = Vec::new();

    let cross_account_ids = st
        .message_store
        .heal_cross_account_pending_messages(&current_user_id, &pending_ids)
        .await?;
    for client_msg_id in &cross_account_ids {
        let _ = st.pending_writer.pop(client_msg_id).await?;
        st.retry_count.remove(client_msg_id);
        st.retry_backoff.remove(client_msg_id);
        st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
            client_msg_id: client_msg_id.clone(),
            reason: crate::domain::REASON_PENDING_ANOTHER_ACCOUNT.to_string(),
            error: crate::domain::SendFailure::local_reason(
                crate::domain::REASON_PENDING_ANOTHER_ACCOUNT,
            ),
        }));
    }
    affected.extend(cross_account_ids);

    let remaining_entries = st.pending_reader.list().await?;
    let remaining_ids: Vec<String> = remaining_entries
        .iter()
        .map(|entry| entry.client_msg_id.clone())
        .collect();
    let orphan_ids = st
        .message_store
        .heal_orphan_sending_messages(&current_user_id, &remaining_ids)
        .await?;
    for client_msg_id in &orphan_ids {
        st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
            client_msg_id: client_msg_id.clone(),
            reason: REASON_ORPHAN_RECOVERED.to_string(),
            error: crate::domain::SendFailure::local_reason(REASON_ORPHAN_RECOVERED),
        }));
    }
    affected.extend(orphan_ids);

    // 上一次尝试没能发出去的消息：连接刚恢复，立即重发（重试次数照旧累计），不等到超时。
    // 只按超时重发时，断线期间发出的消息要在重连之后再等一个完整的超时（`ack_timeout_secs`，SdkConfig
    // 默认 10 s；队列没拿到配置时兜底 120 s）。
    let unsent = st
        .in_flight
        .iter()
        .filter(|(_, in_flight)| in_flight.send_failed)
        .map(|(client_msg_id, _)| client_msg_id.clone())
        .collect::<Vec<_>>();
    if !unsent.is_empty() {
        debug!(
            count = unsent.len(),
            "reliable queue: resending entries whose last attempt never left the client"
        );
        for client_msg_id in &unsent {
            st.in_flight.remove(client_msg_id);
        }
    }

    affected.sort();
    affected.dedup();
    try_send_next(st).await?;
    Ok(affected)
}

async fn check_timeout(st: &mut QueueState) -> Result<()> {
    if reconcile_in_flight_terminal_states(st).await? {
        try_send_next(st).await?;
        return Ok(());
    }
    if reconcile_awaiting_durable_terminal_states(st).await? {
        try_send_next(st).await?;
        return Ok(());
    }
    check_awaiting_durable_timeout(st).await?;
    let ids = st.in_flight.keys().cloned().collect::<Vec<_>>();
    for client_msg_id in ids {
        let Some(in_flight) = st.in_flight.remove(&client_msg_id) else {
            continue;
        };
        let entry = in_flight.entry;
        if is_deadline_elapsed(in_flight.deadline_ms) {
            note_send_congestion(st, "ack_timeout");
            let retries = st
                .retry_count
                .get(&entry.client_msg_id)
                .copied()
                .unwrap_or(0);
            match MessageDeliveryService::decide_timeout_expiry(retries, st.max_retries) {
                RetryDecision::Fail { reason } => {
                    st.metrics.counter_with_labels(
                        "reliable_queue.send_failed_total",
                        &[MetricLabel::new("reason", "timeout")],
                        1,
                    );
                    let _ = st.pending_writer.pop(&entry.client_msg_id).await;
                    st.retry_count.remove(&entry.client_msg_id);
                    let failed_msg = MessageDeliveryService::mark_failed(&entry.message);
                    if let Err(e) = st.message_store.save_batch(&[failed_msg]).await {
                        warn!(%e, "persist failed message state failed");
                    }
                    st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
                        client_msg_id: entry.client_msg_id,
                        reason: reason.to_string(),
                        error: crate::domain::SendFailure::local_reason(reason),
                    }));
                }
                RetryDecision::Retry { next_retry_count } => {
                    st.metrics.counter_with_labels(
                        "reliable_queue.send_retry_total",
                        &[MetricLabel::new("reason", "timeout")],
                        1,
                    );
                    st.retry_count
                        .insert(entry.client_msg_id.clone(), next_retry_count);
                    dispatch_send_attempt(st, entry, next_retry_count);
                }
            }
        } else {
            st.in_flight.insert(
                entry.client_msg_id.clone(),
                InFlightSend {
                    entry,
                    deadline_ms: in_flight.deadline_ms,
                    sent_at_ms: in_flight.sent_at_ms,
                    send_failed: in_flight.send_failed,
                },
            );
        }
    }
    try_send_next(st).await?;
    Ok(())
}

/// 有尝试发出 [`SEND_STALL_PROBE_AFTER`] 仍没等到 SendAck：请求对当前连接验活一次
/// （去抖见 [`SendStallProbeGate`]）。
///
/// 半开连接上帧照样「发送成功」（写进了本机缓冲），回执却永远不来；不验活就只能等 ACK 超时
/// 在同一条死连接上重发，直到重试耗尽判失败。验活在 8 s 内把死连接断开，重连后照常重发。
///
/// 只看帧确实交给了传输层的尝试（`send_failed == false`）：没连上时发送直接失败，不算进来。
/// 这里只请求验活，不改任何超时、重试与窗口状态。
fn probe_link_if_send_stalled(st: &mut QueueState) {
    let Some(probe) = st.link_probe.as_ref() else {
        return;
    };
    let now = id::now_millis();
    let unanswered = st
        .in_flight
        .values()
        .filter(|in_flight| !in_flight.send_failed)
        .map(|in_flight| in_flight.sent_at_ms);
    if !st.stall_probe_gate.should_probe(now, unanswered) {
        return;
    }
    st.stall_probe_gate.record_probe(now);
    st.metrics.counter("reliable_queue.stall_probe_total", 1);
    debug!(
        in_flight = st.in_flight.len(),
        "reliable queue: send unanswered for {:?}; probing the connection", SEND_STALL_PROBE_AFTER
    );
    probe.request_probe();
}

async fn check_awaiting_durable_timeout(st: &mut QueueState) -> Result<()> {
    let ids = st.awaiting_durable.keys().cloned().collect::<Vec<_>>();
    for client_msg_id in ids {
        let Some(awaiting) = st.awaiting_durable.remove(&client_msg_id) else {
            continue;
        };
        let entry = awaiting.entry;
        if !is_deadline_elapsed(awaiting.deadline_ms) {
            st.awaiting_durable.insert(
                entry.client_msg_id.clone(),
                AwaitingDurableAck {
                    entry,
                    deadline_ms: awaiting.deadline_ms,
                },
            );
            continue;
        }

        note_send_congestion(st, "durable_ack_timeout");
        let retries = st
            .retry_count
            .get(&entry.client_msg_id)
            .copied()
            .unwrap_or(0);
        match MessageDeliveryService::decide_timeout_expiry(retries, st.max_retries) {
            RetryDecision::Retry { next_retry_count } => {
                st.metrics.counter_with_labels(
                    "reliable_queue.send_retry_total",
                    &[MetricLabel::new("reason", "durable_ack_timeout")],
                    1,
                );
                st.retry_count
                    .insert(entry.client_msg_id.clone(), next_retry_count);
                dispatch_send_attempt(st, entry, next_retry_count);
            }
            RetryDecision::Fail { reason } => {
                st.metrics.counter_with_labels(
                    "reliable_queue.send_failed_total",
                    &[MetricLabel::new("reason", "durable_ack_timeout")],
                    1,
                );
                let _ = st.pending_writer.pop(&entry.client_msg_id).await;
                st.retry_count.remove(&entry.client_msg_id);
                let failed_msg = MessageDeliveryService::mark_failed(&entry.message);
                if let Err(e) = st.message_store.save_batch(&[failed_msg]).await {
                    warn!(%e, "persist durable-timeout failed message state failed");
                }
                st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
                    client_msg_id: entry.client_msg_id,
                    reason: reason.to_string(),
                    error: crate::domain::SendFailure::local_reason(reason),
                }));
            }
        }
    }
    Ok(())
}

/// SDK 内补偿：对账 in-flight 与本地消息终态。
///
/// 典型场景：
/// - SendAck 丢失，但下行消息已落库（status>=Sent 且 server_id/seq 已回填）；
/// - 本地消息已被其他路径收敛为 Failed。
///
/// 命中终态时在 SDK 内原子收敛队列，避免前端长期“发送中”。
async fn reconcile_in_flight_terminal_states(st: &mut QueueState) -> Result<bool> {
    let ids = st.in_flight.keys().cloned().collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(false);
    }
    // 批量读取所有 in-flight 对应本地消息，避免每条一次 DB 往返（高在途时抢占 sqlx 连接池）。
    let snapshot_by_id: HashMap<String, DeliveryLocalSnapshot> = st
        .message_store
        .get_by_client_msg_ids(&ids)
        .await?
        .iter()
        .map(|message| {
            (
                message.client_msg_id.clone(),
                DeliveryLocalSnapshot::from(message),
            )
        })
        .collect();
    let mut progressed = false;

    for client_msg_id in ids {
        let Some(in_flight) = st.in_flight.remove(&client_msg_id) else {
            continue;
        };
        let entry = in_flight.entry;
        let local_snapshot = snapshot_by_id.get(&entry.client_msg_id);

        match MessageDeliveryService::reconcile_in_flight(local_snapshot, &entry.client_msg_id) {
            InFlightReconcileDecision::KeepWaiting => {
                st.in_flight.insert(
                    entry.client_msg_id.clone(),
                    InFlightSend {
                        entry,
                        deadline_ms: in_flight.deadline_ms,
                        sent_at_ms: in_flight.sent_at_ms,
                        send_failed: in_flight.send_failed,
                    },
                );
            }
            InFlightReconcileDecision::MarkFailed { reason } => {
                let _ = st.pending_writer.pop(&entry.client_msg_id).await?;
                st.retry_count.remove(&entry.client_msg_id);
                st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
                    client_msg_id: entry.client_msg_id.clone(),
                    reason: reason.to_string(),
                    error: crate::domain::SendFailure::local_reason(reason),
                }));
                progressed = true;
            }
            InFlightReconcileDecision::SynthesizeAck { snapshot } => {
                let _ = st.pending_writer.pop(&entry.client_msg_id).await?;
                st.retry_count.remove(&entry.client_msg_id);
                st.bus.publish(SdkEvent::Message(MessageEvent::SendAck {
                    ack: Box::new(MessageDeliveryService::synthetic_ack(
                        &entry.client_msg_id,
                        &snapshot,
                    )),
                }));
                progressed = true;
            }
        }
    }

    Ok(progressed)
}

async fn reconcile_awaiting_durable_terminal_states(st: &mut QueueState) -> Result<bool> {
    let ids = st.awaiting_durable.keys().cloned().collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(false);
    }
    let snapshot_by_id: HashMap<String, DeliveryLocalSnapshot> = st
        .message_store
        .get_by_client_msg_ids(&ids)
        .await?
        .iter()
        .map(|message| {
            (
                message.client_msg_id.clone(),
                DeliveryLocalSnapshot::from(message),
            )
        })
        .collect();
    let mut progressed = false;

    for client_msg_id in ids {
        let Some(awaiting) = st.awaiting_durable.remove(&client_msg_id) else {
            continue;
        };
        let entry = awaiting.entry;
        let local_snapshot = snapshot_by_id.get(&entry.client_msg_id);

        match MessageDeliveryService::reconcile_in_flight(local_snapshot, &entry.client_msg_id) {
            InFlightReconcileDecision::KeepWaiting => {
                st.awaiting_durable.insert(
                    entry.client_msg_id.clone(),
                    AwaitingDurableAck {
                        entry,
                        deadline_ms: awaiting.deadline_ms,
                    },
                );
            }
            InFlightReconcileDecision::MarkFailed { reason } => {
                let _ = st.pending_writer.pop(&entry.client_msg_id).await?;
                st.retry_count.remove(&entry.client_msg_id);
                st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
                    client_msg_id: entry.client_msg_id.clone(),
                    reason: reason.to_string(),
                    error: crate::domain::SendFailure::local_reason(reason),
                }));
                progressed = true;
            }
            InFlightReconcileDecision::SynthesizeAck { snapshot } => {
                let _ = st.pending_writer.pop(&entry.client_msg_id).await?;
                st.retry_count.remove(&entry.client_msg_id);
                st.bus.publish(SdkEvent::Message(MessageEvent::SendAck {
                    ack: Box::new(MessageDeliveryService::synthetic_ack(
                        &entry.client_msg_id,
                        &snapshot,
                    )),
                }));
                progressed = true;
            }
        }
    }

    Ok(progressed)
}

async fn try_send_next(st: &mut QueueState) -> Result<()> {
    loop {
        let current_limit = st.adaptive_window.current();
        let available = current_limit.saturating_sub(st.in_flight.len());
        if available == 0 {
            debug!(
                window = current_limit,
                in_flight = st.in_flight.len(),
                awaiting_durable = st.awaiting_durable.len(),
                "reliable queue window full; nothing dispatched"
            );
            return Ok(());
        }
        let mut excluded = st.in_flight.keys().cloned().collect::<Vec<_>>();
        excluded.extend(st.awaiting_durable.keys().cloned());
        excluded.extend(st.retry_backoff.keys().cloned());
        let entries = st
            .pending_reader
            .list_oldest_excluding(&excluded, available)
            .await?;
        if entries.is_empty() {
            debug!(
                window = current_limit,
                in_flight = st.in_flight.len(),
                awaiting_durable = st.awaiting_durable.len(),
                available,
                "reliable queue found no dispatchable pending entry"
            );
            return Ok(());
        }
        debug!(
            window = current_limit,
            in_flight = st.in_flight.len(),
            picked = entries.len(),
            "reliable queue picked pending entries"
        );

        let connected_user_id = st.current_user_id.read().await.clone();
        let entry_ids = entries
            .iter()
            .map(|entry| entry.client_msg_id.clone())
            .collect::<Vec<_>>();
        let local_snapshots = match st.message_store.get_by_client_msg_ids(&entry_ids).await {
            Ok(messages) => messages
                .iter()
                .map(|message| {
                    (
                        message.client_msg_id.clone(),
                        DeliveryLocalSnapshot::from(message),
                    )
                })
                .collect::<HashMap<_, _>>(),
            Err(error) => {
                warn!(%error, "batch load local messages for pending dispatch failed");
                HashMap::new()
            }
        };

        let mut made_progress = false;
        for entry in entries {
            if st.in_flight.len() >= st.adaptive_window.current() {
                return Ok(());
            }
            let retries = st
                .retry_count
                .get(&entry.client_msg_id)
                .copied()
                .unwrap_or(0);
            let local_snapshot = local_snapshots.get(&entry.client_msg_id);
            match MessageDeliveryService::decide_pending_dispatch(
                &connected_user_id,
                &entry.client_msg_id,
                &entry.message.sender_id,
                local_snapshot,
                retries,
                st.max_retries,
            ) {
                PendingDispatchDecision::DropAsCrossAccount { reason } => {
                    debug!(
                        client_msg_id = %entry.client_msg_id,
                        sender_id = %entry.message.sender_id,
                        connected_user_id = %connected_user_id,
                        "reliable queue: drop cross-account pending entry"
                    );
                    let _ = st.pending_writer.pop(&entry.client_msg_id).await;
                    st.retry_count.remove(&entry.client_msg_id);
                    let failed_msg = MessageDeliveryService::mark_failed(&entry.message);
                    if let Err(e) = st.message_store.save_batch(&[failed_msg]).await {
                        warn!(%e, "persist cross-account failed message state failed");
                    }
                    st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
                        client_msg_id: entry.client_msg_id.clone(),
                        reason: reason.to_string(),
                        error: crate::domain::SendFailure::local_reason(reason),
                    }));
                    made_progress = true;
                    continue;
                }
                PendingDispatchDecision::DropAsTerminal => {
                    debug!(
                        client_msg_id = %entry.client_msg_id,
                        "reliable queue: drop stale pending entry"
                    );
                    let _ = st.pending_writer.pop(&entry.client_msg_id).await;
                    st.retry_count.remove(&entry.client_msg_id);
                    made_progress = true;
                    continue;
                }
                PendingDispatchDecision::FailMaxRetries { reason } => {
                    let _ = st.pending_writer.pop(&entry.client_msg_id).await;
                    st.retry_count.remove(&entry.client_msg_id);
                    let failed_msg = MessageDeliveryService::mark_failed(&entry.message);
                    if let Err(e) = st.message_store.save_batch(&[failed_msg]).await {
                        warn!(%e, "persist failed message state failed");
                    }
                    st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
                        client_msg_id: entry.client_msg_id.clone(),
                        reason: reason.to_string(),
                        error: crate::domain::SendFailure::local_reason(reason),
                    }));
                    made_progress = true;
                    continue;
                }
                PendingDispatchDecision::SendNow => {}
            }
            dispatch_send_attempt(st, entry, retries);
            made_progress = true;
        }
        if !made_progress {
            return Ok(());
        }
    }
}

async fn do_send_one(sender: &PacketSender, entry: &PendingSendVo) -> Result<()> {
    let proto = entry.message.to_proto();
    sender.send_message(&proto, Duration::from_secs(15)).await
}

fn conversation_projection_ts(message: &IMMessage) -> u64 {
    if message.conversation_seq > 0 {
        if message.created_at > 0 {
            return message.created_at;
        }
        if message.client_created_at > 0 {
            return message.client_created_at;
        }
    }
    message.timeline_sort_ts()
}

async fn update_conversation_last_message(st: &QueueState, message: &IMMessage) -> Result<()> {
    let conversation_id = message.conversation_id.trim();
    let last_message_id = if message.server_id.trim().is_empty() {
        message.client_msg_id.trim()
    } else {
        message.server_id.trim()
    };
    if conversation_id.is_empty() || last_message_id.is_empty() {
        return Ok(());
    }
    let preview = message.text_for_storage();
    st.conversation_store
        .update_last_message(
            conversation_id,
            last_message_id,
            message.sender_id(),
            conversation_projection_ts(message),
            preview.as_deref(),
            message.conversation_seq,
        )
        .await
}

/// Send-to-ack latency above this is logged at warn so a slow send stands out in client logs
/// without a metrics backend; normal sends stay at debug.
const SLOW_SEND_LOG_THRESHOLD_MS: u64 = 1000;

fn log_send_latency(client_msg_id: &str, latency_ms: u64, stage: &str) {
    if latency_ms > SLOW_SEND_LOG_THRESHOLD_MS {
        warn!(%client_msg_id, latency_ms, stage, "slow send: ack latency exceeded threshold");
    } else {
        debug!(%client_msg_id, latency_ms, stage, "send ack latency");
    }
}

async fn apply_ack_and_publish(
    st: &mut QueueState,
    entry: &PendingSendVo,
    ack: SendAck,
) -> AckApplyResult {
    if MessageDeliveryService::durable_accepted_from_ack(&ack).is_some() {
        let _ = st.pending_writer.pop(&ack.client_msg_id).await;
        st.retry_count.remove(&ack.client_msg_id);
        let latency_ms = id::now_millis().saturating_sub(entry.enqueued_at_ms);
        st.metrics
            .histogram("reliable_queue.send_ack_latency_ms", latency_ms as f64);
        log_send_latency(&ack.client_msg_id, latency_ms, "durable");
        note_send_success(st);
        let msg = MessageDeliveryService::mark_sent_from_ack(&entry.message, &ack);
        let cid = ack.client_msg_id.clone();
        if let Err(e) = st.message_store.update_after_ack(&cid, &msg).await {
            warn!(%e, "update_after_ack failed");
        }
        if let Err(e) = update_conversation_last_message(st, &msg).await {
            warn!(%e, conversation_id = %msg.conversation_id, "update conversation projection after ack failed");
        }
        st.bus.publish(SdkEvent::Message(MessageEvent::SendAck {
            ack: Box::new(ack),
        }));
        return AckApplyResult::Terminal;
    }
    if MessageDeliveryService::accepted_from_ack(&ack).is_some() {
        let latency_ms = id::now_millis().saturating_sub(entry.enqueued_at_ms);
        st.metrics.histogram(
            "reliable_queue.server_accepted_latency_ms",
            latency_ms as f64,
        );
        log_send_latency(&ack.client_msg_id, latency_ms, "accepted");
        note_send_success(st);
        st.bus.publish(SdkEvent::Message(MessageEvent::SendAck {
            ack: Box::new(ack),
        }));
        return AckApplyResult::AwaitDurable;
    }
    if MessageDeliveryService::is_retryable_ack_error(&ack) {
        let retries = st.retry_count.get(&ack.client_msg_id).copied().unwrap_or(0);
        if let RetryDecision::Retry { next_retry_count } =
            MessageDeliveryService::decide_retryable_ack_error(retries, st.max_retries)
        {
            schedule_retry_after_backoff(st, entry, &ack, next_retry_count);
            return AckApplyResult::RetryScheduled;
        }
    }
    let _ = st.pending_writer.pop(&ack.client_msg_id).await;
    st.retry_count.remove(&ack.client_msg_id);
    let failed_msg = MessageDeliveryService::mark_failed(&entry.message);
    if let Err(e) = st.message_store.save_batch(&[failed_msg]).await {
        warn!(%e, "persist failed message state from ack failed");
    }
    let reason = MessageDeliveryService::error_message_from_ack(&ack)
        .unwrap_or_else(|| "send ack missing accepted result".to_string());
    let error = MessageDeliveryService::send_failure_from_ack(&ack)
        .unwrap_or_else(|| crate::domain::SendFailure::local_reason(&reason));
    let client_msg_id = ack.client_msg_id.clone();
    st.bus.publish(SdkEvent::Message(MessageEvent::SendAck {
        ack: Box::new(ack),
    }));
    st.bus.publish(SdkEvent::Message(MessageEvent::SendFailed {
        client_msg_id,
        reason,
        error,
    }));
    AckApplyResult::Terminal
}

/// 服务端暂时性拒绝（见 [`MessageDeliveryService::is_retryable_ack_error`]）：消息保持发送中、
/// 留在 pending 里，退避后用同一个 `client_msg_id` 重发。不发 SendAck/SendFailed 事件——
/// 对应用来说这条消息还在发送中，没有失败。
fn schedule_retry_after_backoff(
    st: &mut QueueState,
    entry: &PendingSendVo,
    ack: &SendAck,
    next_retry_count: u32,
) {
    let (min_ms, max_ms) =
        MessageDeliveryService::retryable_ack_backoff_bounds_ms(next_retry_count);
    let backoff_ms = if min_ms == max_ms {
        min_ms
    } else {
        rand::thread_rng().gen_range(min_ms..=max_ms)
    };
    let error_code = match ack.result.as_ref() {
        Some(flare_proto::common::send_ack::Result::Error(error)) => error.code,
        _ => 0,
    };
    st.metrics.counter_with_labels(
        "reliable_queue.send_retry_total",
        &[MetricLabel::new("reason", "retryable_ack_error")],
        1,
    );
    note_send_congestion(st, "retryable_ack_error");
    st.retry_count
        .insert(entry.client_msg_id.clone(), next_retry_count);
    st.retry_backoff
        .insert(entry.client_msg_id.clone(), next_retry_count);
    warn!(
        client_msg_id = %entry.client_msg_id,
        error_code,
        retries = next_retry_count,
        max_retries = st.max_retries,
        backoff_ms,
        "send rejected for now by the server; resending the same message after backoff"
    );
    let command_tx = st.command_tx.clone();
    let client_msg_id = entry.client_msg_id.clone();
    let _ = spawn_background_task(async move {
        delay(Duration::from_millis(backoff_ms)).await;
        let _ = command_tx
            .send(QueueCommand::RetryBackoffElapsed {
                client_msg_id,
                retries: next_retry_count,
            })
            .await;
    });
}

async fn apply_orphan_durable_ack(st: &mut QueueState, ack: &SendAck) -> Result<()> {
    let Some(accepted) = MessageDeliveryService::durable_accepted_from_ack(ack) else {
        return Ok(());
    };
    let client_msg_id = ack.client_msg_id.trim();
    if client_msg_id.is_empty() {
        return Ok(());
    }
    let Some(local) = st.message_store.get_by_client_msg_id(client_msg_id).await? else {
        return Ok(());
    };
    if !orphan_durable_ack_should_update(&local, client_msg_id, accepted.conversation_seq) {
        return Ok(());
    }
    let msg = MessageDeliveryService::mark_sent_from_ack(&local, ack);
    st.message_store
        .update_after_ack(client_msg_id, &msg)
        .await?;
    update_conversation_last_message(st, &msg).await?;
    Ok(())
}

fn orphan_durable_ack_should_update(local: &IMMessage, client_msg_id: &str, ack_seq: u64) -> bool {
    let pending_like = local.server_id == client_msg_id
        || local.local_state.is_local
        || local.local_state.sending
        || local.status < MessageStatus::Sent as i32;
    pending_like || local.conversation_seq == 0 || ack_seq > local.conversation_seq
}

#[cfg(all(test, feature = "storage-sqlite"))]
mod tests {
    use super::{QueueCommand, ReliableSendQueue, ReliableSendQueueConfig};
    use crate::domain::{
        ConversationReader, ConversationWriter, MessageReader, MessageWriter, PendingSendReader,
        PendingSendVo, PendingSendWriter,
    };
    use crate::infrastructure::persistence::sqlite::{
        SqliteConversationRepo, SqliteMessageRepo, SqlitePendingSendRepo,
        init_schema as sqlite_init_schema,
    };
    use crate::infrastructure::protocol::{Codec, PacketSender, ProtobufCodec};
    use crate::kernel::CurrentUserIdStore;
    use crate::kernel::event::{EventBus, EventReceiver, MessageEvent, SdkEvent};
    use crate::model::message::MessageStatus;
    use crate::model::{Conversation, IMMessage};
    use crate::spi::metrics::MetricsRecorder;
    use flare_proto::common::{
        ErrorCode as ProtoErrorCode, ErrorDetail, SendAccepted, SendAck, SendAckDurability,
        send_ack,
    };
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tokio::sync::{Mutex, RwLock, oneshot};

    /// Resolves once the queue has handled every command sent before this call, including
    /// publishing whatever events handling them produced.
    async fn handled(queue: &ReliableSendQueue) {
        let (done, handled) = oneshot::channel();
        queue
            .tx
            .send(QueueCommand::Handled(done))
            .await
            .expect("reliable queue closed");
        handled.await.expect("reliable queue dropped the request");
    }

    /// The next event the queue published. The queue publishes while it handles a command, so
    /// the caller first awaits that command's response, or [`handled`] for a command without
    /// one: the event is then already there, and a missing event fails at once instead of being
    /// waited out.
    fn published(receiver: &mut EventReceiver, what: &str) -> SdkEvent {
        receiver
            .try_recv()
            .unwrap_or_else(|error| panic!("expected {what}, nothing was published: {error:?}"))
    }

    fn dummy_sender() -> Arc<PacketSender> {
        Arc::new(PacketSender::new(
            Arc::new(Mutex::new(None)),
            Arc::new(ProtobufCodec) as Arc<dyn Codec>,
        ))
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn enqueue_returns_after_optimistic_message_is_persisted() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store,
            sender: dummy_sender(),
            message_store: message_store.clone(),
            conversation_store,
            current_user_id,
            bus: bus.clone(),
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: MetricsRecorder::disabled(),
        });
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = "client-1".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u1".to_string();

        queue.enqueue(message).await.unwrap();

        let stored = message_store
            .get_by_client_msg_id("client-1")
            .await
            .unwrap()
            .expect("optimistic message should be visible after enqueue returns");
        assert_eq!(stored.server_id, "client-1");
        assert!(stored.local_state.sending);
        assert!(stored.local_state.is_local);

        let event = published(&mut receiver, "optimistic message event");
        match event {
            SdkEvent::Message(MessageEvent::ReceivedBatch { messages }) => {
                let optimistic = messages
                    .first()
                    .expect("optimistic message batch should not be empty");
                assert_eq!(optimistic.client_msg_id, "client-1");
                assert!(optimistic.local_state.sending);
                assert!(optimistic.local_state.is_local);
            }
            other => panic!("expected optimistic ReceivedBatch event, got {other:?}"),
        }
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn ack_updates_conversation_last_message_projection_before_publish() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        conversation_store
            .save_one(&Conversation {
                conversation_id: "conv-ack".to_string(),
                last_message_id: Some("server-old".to_string()),
                last_sender_id: Some("u2".to_string()),
                last_message_at: Some(1_000),
                last_message_preview: Some("old".to_string()),
                max_seq: 4,
                ..Default::default()
            })
            .await
            .unwrap();
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store,
            sender: dummy_sender(),
            message_store,
            conversation_store: conversation_store.clone(),
            current_user_id,
            bus: bus.clone(),
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: MetricsRecorder::disabled(),
        });
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = "client-ack".to_string();
        message.server_id = "client-ack".to_string();
        message.conversation_id = "conv-ack".to_string();
        message.sender_id = "u1".to_string();

        queue.enqueue(message).await.unwrap();
        let optimistic_event = published(&mut receiver, "optimistic message event");
        assert!(matches!(
            optimistic_event,
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));
        queue
            .on_ack(SendAck {
                client_msg_id: "client-ack".to_string(),
                conversation_id: "conv-ack".to_string(),
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: "server-new".to_string(),
                    conversation_seq: 5,
                    server_time: 0,
                    durability: SendAckDurability::Persisted as i32,
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        handled(&queue).await;

        let event = published(&mut receiver, "send ack");
        assert!(matches!(
            event,
            SdkEvent::Message(MessageEvent::SendAck { .. })
        ));
        let updated = conversation_store
            .get("conv-ack")
            .await
            .unwrap()
            .expect("conversation should exist");
        assert_eq!(updated.last_message_id.as_deref(), Some("server-new"));
        assert_eq!(updated.last_sender_id.as_deref(), Some("u1"));
        assert_eq!(updated.max_seq, 5);
        assert_ne!(updated.last_message_preview.as_deref(), Some("old"));
        assert_eq!(updated.unread_count, 0);
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn broker_ack_keeps_pending_message_recoverable() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store.clone(),
            sender: dummy_sender(),
            message_store: message_store.clone(),
            conversation_store,
            current_user_id,
            bus,
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: MetricsRecorder::disabled(),
        });
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = "client-broker".to_string();
        message.server_id = "client-broker".to_string();
        message.conversation_id = "conv-broker".to_string();
        message.sender_id = "u1".to_string();

        queue.enqueue(message).await.unwrap();
        let optimistic_event = published(&mut receiver, "optimistic message event");
        assert!(matches!(
            optimistic_event,
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));
        queue
            .on_ack(SendAck {
                client_msg_id: "client-broker".to_string(),
                conversation_id: "conv-broker".to_string(),
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: "server-broker".to_string(),
                    conversation_seq: 9,
                    server_time: 0,
                    durability: SendAckDurability::BrokerAccepted as i32,
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        handled(&queue).await;

        let event = published(&mut receiver, "broker send ack");
        assert!(matches!(
            event,
            SdkEvent::Message(MessageEvent::SendAck { .. })
        ));
        assert!(
            pending_store.get("client-broker").await.unwrap().is_some(),
            "broker ack must not clear recoverable pending state"
        );
        let local = message_store
            .get_by_client_msg_id("client-broker")
            .await
            .unwrap()
            .expect("local optimistic message should remain");
        assert!(local.local_state.sending);
        assert!(local.local_state.is_local);
        assert_eq!(local.server_id, "client-broker");
        assert_eq!(local.conversation_seq, 0);
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn broker_ack_releases_window_without_clearing_pending() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store.clone(),
            sender: dummy_sender(),
            message_store,
            conversation_store,
            current_user_id,
            bus,
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(1),
            metrics: MetricsRecorder::disabled(),
        });

        for client_msg_id in ["client-broker-window-1", "client-broker-window-2"] {
            let mut message = IMMessage::new(flare_proto::common::Message::default());
            message.client_msg_id = client_msg_id.to_string();
            message.server_id = client_msg_id.to_string();
            message.conversation_id = "conv-broker-window".to_string();
            message.sender_id = "u1".to_string();
            queue.enqueue(message).await.unwrap();
        }
        for _ in 0..2 {
            let optimistic_event = published(&mut receiver, "optimistic message event");
            assert!(matches!(
                optimistic_event,
                SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
            ));
        }

        queue
            .on_ack(SendAck {
                client_msg_id: "client-broker-window-1".to_string(),
                conversation_id: "conv-broker-window".to_string(),
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: "server-broker-window-1".to_string(),
                    conversation_seq: 1,
                    server_time: 0,
                    durability: SendAckDurability::BrokerAccepted as i32,
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        handled(&queue).await;

        let broker_event = published(&mut receiver, "broker send ack");
        assert!(matches!(
            broker_event,
            SdkEvent::Message(MessageEvent::SendAck { .. })
        ));
        assert!(
            pending_store
                .get("client-broker-window-1")
                .await
                .unwrap()
                .is_some(),
            "broker ack must keep first message recoverable"
        );

        queue
            .on_ack(SendAck {
                client_msg_id: "client-broker-window-2".to_string(),
                conversation_id: "conv-broker-window".to_string(),
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: "server-broker-window-2".to_string(),
                    conversation_seq: 2,
                    server_time: 0,
                    durability: SendAckDurability::Persisted as i32,
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        handled(&queue).await;

        let persisted_event = published(
            &mut receiver,
            "second persisted send ack after broker window release",
        );
        match persisted_event {
            SdkEvent::Message(MessageEvent::SendAck { ack }) => {
                assert_eq!(ack.client_msg_id, "client-broker-window-2");
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(
            pending_store
                .get("client-broker-window-2")
                .await
                .unwrap()
                .is_none(),
            "persisted ack should clear the second message"
        );
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn orphan_durable_ack_is_published() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store,
            sender: dummy_sender(),
            message_store,
            conversation_store,
            current_user_id,
            bus,
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: MetricsRecorder::disabled(),
        });

        queue
            .on_ack(SendAck {
                client_msg_id: "client-orphan".to_string(),
                conversation_id: "conv-orphan".to_string(),
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: "server-orphan".to_string(),
                    conversation_seq: 10,
                    server_time: 0,
                    durability: SendAckDurability::Persisted as i32,
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        handled(&queue).await;

        let event = published(&mut receiver, "orphan durable send ack");
        match event {
            SdkEvent::Message(MessageEvent::SendAck { ack }) => {
                assert_eq!(ack.client_msg_id, "client-orphan");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn orphan_later_persisted_ack_advances_local_canonical_seq() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let mut local = IMMessage::new(flare_proto::common::Message::default());
        local.server_id = "server-old".to_string();
        local.client_msg_id = "client-late-persisted".to_string();
        local.conversation_id = "conv-late-persisted".to_string();
        local.sender_id = "u1".to_string();
        local.conversation_seq = 10;
        local.status = MessageStatus::Sent as i32;
        message_store.save_batch(&[local]).await.unwrap();

        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store,
            sender: dummy_sender(),
            message_store: message_store.clone(),
            conversation_store,
            current_user_id,
            bus,
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: MetricsRecorder::disabled(),
        });

        queue
            .on_ack(SendAck {
                client_msg_id: "client-late-persisted".to_string(),
                conversation_id: "conv-late-persisted".to_string(),
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: "server-new".to_string(),
                    conversation_seq: 42,
                    server_time: 1234,
                    durability: SendAckDurability::Persisted as i32,
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        handled(&queue).await;

        let event = published(&mut receiver, "orphan persisted send ack");
        assert!(matches!(
            event,
            SdkEvent::Message(MessageEvent::SendAck { .. })
        ));
        assert!(message_store.get("server-old").await.unwrap().is_none());
        let stored = message_store
            .get_by_client_msg_id("client-late-persisted")
            .await
            .unwrap()
            .expect("canonical row should remain addressable by client id");
        assert_eq!(stored.server_id, "server-new");
        assert_eq!(stored.conversation_seq, 42);
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn pipelined_queue_accepts_out_of_order_durable_acks() {
        let bus = EventBus::new();
        let metrics = MetricsRecorder::enabled(None);
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store.clone(),
            sender: dummy_sender(),
            message_store,
            conversation_store,
            current_user_id,
            bus,
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(2),
            metrics: metrics.clone(),
        });
        // Both enqueue commands are queued before the actor handles the first. The dummy sender
        // is never connected, so a send attempt fails at once and its failure halves the window;
        // queued this way, the second message is handled before the first attempt's failure can
        // shrink the window, however the runtime schedules the actor and the failing send.
        let mut responses = Vec::new();
        for client_msg_id in ["client-pipe-1", "client-pipe-2"] {
            let mut message = IMMessage::new(flare_proto::common::Message::default());
            message.client_msg_id = client_msg_id.to_string();
            message.server_id = client_msg_id.to_string();
            message.conversation_id = "conv-pipe".to_string();
            message.sender_id = "u1".to_string();
            let (resp, response) = oneshot::channel();
            queue
                .tx
                .try_send(QueueCommand::Enqueue {
                    message: Box::new(message),
                    resp,
                })
                .expect("the command channel has room");
            responses.push(response);
        }
        for response in responses {
            response.await.expect("queue answered").expect("enqueued");
        }
        for _ in 0..2 {
            let optimistic_event = published(&mut receiver, "optimistic message event");
            assert!(matches!(
                optimistic_event,
                SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
            ));
        }
        handled(&queue).await;
        assert_eq!(
            metrics
                .snapshot()
                .counters
                .get("reliable_queue.send_attempt_total{retry=0}")
                .copied(),
            Some(2),
            "both messages are in flight before their acks arrive"
        );

        queue
            .on_ack(SendAck {
                client_msg_id: "client-pipe-2".to_string(),
                conversation_id: "conv-pipe".to_string(),
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: "server-pipe-2".to_string(),
                    conversation_seq: 2,
                    server_time: 0,
                    durability: SendAckDurability::Persisted as i32,
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        handled(&queue).await;

        let event = published(&mut receiver, "second send ack");
        match event {
            SdkEvent::Message(MessageEvent::SendAck { ack }) => {
                assert_eq!(ack.client_msg_id, "client-pipe-2");
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(
            pending_store.get("client-pipe-1").await.unwrap().is_some(),
            "first in-flight message should remain pending"
        );
        assert!(
            pending_store.get("client-pipe-2").await.unwrap().is_none(),
            "second in-flight message should be independently cleared"
        );
    }

    /// A message the server turned away for now waits out a backoff: still queued, no longer in
    /// flight. If the ack of an earlier attempt then arrives (the server persisted it after
    /// all), it settles the message at once: settling takes no send slot, and the message would
    /// otherwise show as sending until the backoff ran out and a slot was free.
    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn an_ack_for_a_message_waiting_to_be_resent_settles_it_at_once() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let metrics = MetricsRecorder::enabled(None);
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store.clone(),
            sender: dummy_sender(),
            message_store: message_store.clone(),
            conversation_store,
            current_user_id,
            bus,
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: metrics.clone(),
        });
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = "client-late".to_string();
        message.server_id = "client-late".to_string();
        message.conversation_id = "conv-late".to_string();
        message.sender_id = "u1".to_string();
        queue.enqueue(message).await.unwrap();
        assert!(matches!(
            published(&mut receiver, "optimistic message event"),
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));

        // Both acks reach the queue before it handles the first, so the backoff (a second or
        // more) cannot run out in between.
        queue
            .on_ack(SendAck {
                client_msg_id: "client-late".to_string(),
                conversation_id: "conv-late".to_string(),
                result: Some(send_ack::Result::Error(ErrorDetail {
                    code: ProtoErrorCode::Unavailable as i32,
                    reason: "overloaded".to_string(),
                    message: "try again later".to_string(),
                    track: String::new(),
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        queue
            .on_ack(SendAck {
                client_msg_id: "client-late".to_string(),
                conversation_id: "conv-late".to_string(),
                result: Some(send_ack::Result::Accepted(SendAccepted {
                    server_msg_id: "server-late".to_string(),
                    conversation_seq: 3,
                    server_time: 0,
                    durability: SendAckDurability::Persisted as i32,
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        handled(&queue).await;

        match published(&mut receiver, "send ack of the earlier attempt") {
            SdkEvent::Message(MessageEvent::SendAck { ack }) => {
                assert_eq!(ack.client_msg_id, "client-late");
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(
            pending_store.get("client-late").await.unwrap().is_none(),
            "the persisted message leaves the queue"
        );
        let stored = message_store
            .get_by_client_msg_id("client-late")
            .await
            .unwrap()
            .expect("message stays stored");
        assert_eq!(stored.server_id, "server-late");
        assert_eq!(stored.conversation_seq, 3);
        let gauges = metrics.snapshot().gauges;
        assert_eq!(gauges.get("reliable_queue.retry_backoff"), Some(&0));
        assert_eq!(gauges.get("reliable_queue.in_flight"), Some(&0));
        assert!(
            receiver.try_recv().is_err(),
            "nothing else is published for the message"
        );
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn recover_pending_for_current_user_drops_cross_account_entries() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "c1".to_string();
        message.client_msg_id = "c1".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u2".to_string();
        message.local_state.sending = true;
        message.local_state.is_local = true;
        message_store.save_batch(&[message.clone()]).await.unwrap();
        pending_store
            .push(PendingSendVo {
                client_msg_id: "c1".to_string(),
                conversation_id: "conv-1".to_string(),
                message,
                enqueued_at_ms: 1,
            })
            .await
            .unwrap();
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store.clone(),
            sender: dummy_sender(),
            message_store,
            conversation_store,
            current_user_id,
            bus: bus.clone(),
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: MetricsRecorder::disabled(),
        });

        let affected = queue.recover_pending_for_current_user().await.unwrap();
        assert_eq!(affected, vec!["c1".to_string()]);
        assert!(pending_store.list().await.unwrap().is_empty());

        let event = published(&mut receiver, "send failed");
        match event {
            SdkEvent::Message(MessageEvent::SendFailed { client_msg_id, .. }) => {
                assert_eq!(client_msg_id, "c1");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    /// A server rejection reaches the host with its typed reason: `SendFailed.error` carries
    /// `ErrorDetail.reason` as the machine reason, and the old `reason` text stays as it was.
    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn a_rejected_send_reports_the_server_reason_as_the_machine_reason() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store.clone(),
            sender: dummy_sender(),
            message_store,
            conversation_store,
            current_user_id,
            bus,
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: MetricsRecorder::disabled(),
        });
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = "client-card".to_string();
        message.server_id = "client-card".to_string();
        message.conversation_id = "conv-card".to_string();
        message.sender_id = "u1".to_string();
        queue.enqueue(message).await.unwrap();
        assert!(matches!(
            published(&mut receiver, "optimistic message event"),
            SdkEvent::Message(MessageEvent::ReceivedBatch { .. })
        ));

        queue
            .on_ack(SendAck {
                client_msg_id: "client-card".to_string(),
                conversation_id: "conv-card".to_string(),
                result: Some(send_ack::Result::Error(ErrorDetail {
                    code: ProtoErrorCode::PermissionDenied as i32,
                    reason: "SOCIAL_CARD_SCOPE_FRIENDS".to_string(),
                    message: "only friends can share this card".to_string(),
                    track: String::new(),
                })),
                ..Default::default()
            })
            .await
            .unwrap();
        handled(&queue).await;

        assert!(matches!(
            published(&mut receiver, "the rejecting ack"),
            SdkEvent::Message(MessageEvent::SendAck { .. })
        ));
        match published(&mut receiver, "send failed") {
            SdkEvent::Message(MessageEvent::SendFailed {
                client_msg_id,
                reason,
                error,
            }) => {
                assert_eq!(client_msg_id, "client-card");
                assert_eq!(reason, "only friends can share this card");
                assert_eq!(error.machine_reason, "SOCIAL_CARD_SCOPE_FRIENDS");
                assert_eq!(error.code, "PERMISSION_DENIED");
                assert_eq!(error.message, "only friends can share this card");
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[cfg(feature = "storage-sqlite")]
    #[tokio::test]
    async fn recover_pending_for_current_user_publishes_orphan_failures() {
        let bus = EventBus::new();
        let mut receiver = bus.subscribe_raw();
        let current_user_id: CurrentUserIdStore = Arc::new(RwLock::new("u1".to_string()));
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlite_init_schema(&pool).await.unwrap();
        let pending_store = Arc::new(SqlitePendingSendRepo::new(pool.clone()));
        let message_store = Arc::new(SqliteMessageRepo::new(pool.clone()));
        let conversation_store = Arc::new(SqliteConversationRepo::new(pool));
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.server_id = "orphan-1".to_string();
        message.client_msg_id = "orphan-1".to_string();
        message.conversation_id = "conv-1".to_string();
        message.sender_id = "u1".to_string();
        message.local_state.sending = true;
        message.local_state.is_local = true;
        message_store.save_batch(&[message]).await.unwrap();
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader: pending_store.clone(),
            pending_writer: pending_store,
            sender: dummy_sender(),
            message_store,
            conversation_store,
            current_user_id,
            bus: bus.clone(),
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: MetricsRecorder::disabled(),
        });

        let affected = queue.recover_pending_for_current_user().await.unwrap();
        assert_eq!(affected, vec!["orphan-1".to_string()]);

        let event = published(&mut receiver, "orphan send failed");
        match event {
            SdkEvent::Message(MessageEvent::SendFailed {
                client_msg_id,
                reason,
                error,
            }) => {
                assert_eq!(client_msg_id, "orphan-1");
                assert!(reason.contains("orphan"));
                assert_eq!(error.code, "MESSAGE_SEND_FAILED");
                assert_eq!(error.machine_reason, "");
                assert_eq!(error.message, reason);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}

#[cfg(test)]
mod adaptive_window_tests {
    use super::AdaptiveInFlightWindow;

    #[test]
    fn adaptive_window_grows_after_window_sized_success_batch() {
        let mut window = AdaptiveInFlightWindow::new(8);
        assert_eq!(window.current(), 4);

        for _ in 0..3 {
            assert!(!window.note_success());
        }
        assert!(window.note_success());
        assert_eq!(window.current(), 5);
    }

    #[test]
    fn adaptive_window_halves_on_congestion() {
        let mut window = AdaptiveInFlightWindow::new(16);
        for _ in 0..4 {
            window.note_success();
        }
        assert_eq!(window.current(), 5);

        assert!(window.note_congestion());
        assert_eq!(window.current(), 2);
        assert!(window.note_congestion());
        assert_eq!(window.current(), 1);
        assert!(!window.note_congestion());
    }
}

#[cfg(test)]
mod unsent_resend_tests {
    use super::{ReliableSendQueue, ReliableSendQueueConfig};
    use crate::infrastructure::persistence::in_memory_im_provider;
    use crate::infrastructure::protocol::{Codec, PacketSender, ProtobufCodec};
    use crate::kernel::event::EventBus;
    use crate::model::IMMessage;
    use crate::spi::metrics::MetricsRecorder;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::{Mutex, RwLock};

    fn counter(metrics: &MetricsRecorder, key: &str) -> u64 {
        metrics.snapshot().counters.get(key).copied().unwrap_or(0)
    }

    async fn wait_for_counter(metrics: &MetricsRecorder, key: &str, value: u64) {
        let started = Instant::now();
        while counter(metrics, key) < value {
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "{key} did not reach {value}: {:?}",
                metrics.snapshot().counters
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// 长连接断开时发送直接失败（传输层不再自己重连）：消息留在发送中，不立刻重试；连接恢复、
    /// 恢复当前账号的队列时立即重发，不等到超时（SdkConfig 默认 10 s，队列兜底 120 s，这里配 60 s）。
    #[tokio::test]
    async fn unsent_attempts_are_resent_as_soon_as_the_connection_is_back() {
        let stores = in_memory_im_provider();
        let (pending_reader, pending_writer) = stores.pending_sends().expect("pending sends");
        let metrics = MetricsRecorder::enabled(None);
        let queue = ReliableSendQueue::new(ReliableSendQueueConfig {
            pending_reader,
            pending_writer,
            // 没有长连接客户端：每次发送尝试都立即以「未连接」失败
            sender: Arc::new(PacketSender::new(
                Arc::new(Mutex::new(None)),
                Arc::new(ProtobufCodec) as Arc<dyn Codec>,
            )),
            message_store: stores.messages.clone(),
            conversation_store: stores.conversations.clone(),
            current_user_id: Arc::new(RwLock::new("u1".to_string())),
            bus: EventBus::new(),
            timeout_secs: Some(60),
            max_retries: Some(3),
            max_in_flight: Some(32),
            metrics: metrics.clone(),
        });
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = "client-offline-1".to_string();
        message.conversation_id = "conv-offline".to_string();
        message.sender_id = "u1".to_string();
        queue.enqueue(message).await.expect("enqueue");

        // 第一次尝试没发出去
        wait_for_counter(&metrics, "reliable_queue.send_retry_total", 1).await;
        let resend = "reliable_queue.send_attempt_total{retry=1}";
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            counter(&metrics, resend),
            0,
            "no busy retry while the connection is down"
        );

        // 连接恢复：引擎建连后恢复当前账号的队列
        queue
            .recover_pending_for_current_user()
            .await
            .expect("recover pending");
        wait_for_counter(&metrics, resend, 1).await;
    }
}

#[cfg(test)]
mod stall_probe_tests {
    use super::{
        AdaptiveInFlightWindow, InFlightSend, LinkProbe, QueueState, SEND_STALL_PROBE_AFTER,
        SEND_STALL_PROBE_MIN_INTERVAL, SendStallProbeGate, probe_link_if_send_stalled,
    };
    use crate::domain::PendingSendVo;
    use crate::infrastructure::persistence::in_memory_im_provider;
    use crate::infrastructure::protocol::{Codec, PacketSender, ProtobufCodec};
    use crate::kernel::event::EventBus;
    use crate::model::IMMessage;
    use crate::shared::util::id;
    use crate::spi::metrics::MetricsRecorder;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Mutex, RwLock, mpsc};

    const STALL_MS: u64 = SEND_STALL_PROBE_AFTER.as_millis() as u64;
    const WINDOW_MS: u64 = SEND_STALL_PROBE_MIN_INTERVAL.as_millis() as u64;

    #[derive(Default)]
    struct CountingProbe(AtomicUsize);

    impl CountingProbe {
        fn count(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl LinkProbe for CountingProbe {
        fn request_probe(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn queue_state(probe: Arc<CountingProbe>) -> QueueState {
        let stores = in_memory_im_provider();
        let (pending_reader, pending_writer) = stores.pending_sends().expect("pending sends");
        let (command_tx, _command_rx) = mpsc::channel(8);
        QueueState {
            pending_reader,
            pending_writer,
            command_tx,
            sender: Arc::new(PacketSender::new(
                Arc::new(Mutex::new(None)),
                Arc::new(ProtobufCodec) as Arc<dyn Codec>,
            )),
            message_store: stores.messages.clone(),
            conversation_store: stores.conversations.clone(),
            current_user_id: Arc::new(RwLock::new("u1".to_string())),
            bus: EventBus::new(),
            timeout_duration: Duration::from_secs(10),
            max_retries: 3,
            max_in_flight: 32,
            adaptive_window: AdaptiveInFlightWindow::new(32),
            in_flight: HashMap::new(),
            awaiting_durable: HashMap::new(),
            retry_count: HashMap::new(),
            retry_backoff: HashMap::new(),
            metrics: MetricsRecorder::disabled(),
            link_probe: Some(probe),
            stall_probe_gate: SendStallProbeGate::default(),
        }
    }

    fn in_flight(st: &mut QueueState, client_msg_id: &str, sent_ago_ms: u64, send_failed: bool) {
        let now = id::now_millis();
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.client_msg_id = client_msg_id.to_string();
        message.conversation_id = "conv-stall".to_string();
        st.in_flight.insert(
            client_msg_id.to_string(),
            InFlightSend {
                entry: PendingSendVo {
                    client_msg_id: client_msg_id.to_string(),
                    conversation_id: "conv-stall".to_string(),
                    message,
                    enqueued_at_ms: now,
                },
                deadline_ms: now + 10_000,
                sent_at_ms: now.saturating_sub(sent_ago_ms),
                send_failed,
            },
        );
    }

    /// 5 s 没回执才验活；验过之后 30 s 内不再验，也不为验活之前发出的尝试再验。
    #[test]
    fn gate_waits_for_the_stall_and_debounces() {
        let mut gate = SendStallProbeGate::default();
        let t0 = 1_000_000;
        assert!(!gate.should_probe(t0 + STALL_MS - 1, [t0]));
        assert!(gate.should_probe(t0 + STALL_MS, [t0]));
        gate.record_probe(t0 + STALL_MS);

        // 同一窗口内：新的卡住的尝试也不再验
        let later = t0 + STALL_MS + 1_000;
        assert!(!gate.should_probe(later + STALL_MS, [t0, later]));
        // 窗口过后：验活之前发出的那条不算，验活之后发出且卡住的算
        let after_window = t0 + STALL_MS + WINDOW_MS;
        assert!(!gate.should_probe(after_window, [t0]));
        assert!(gate.should_probe(after_window, [later]));
    }

    /// 换了新连接：旧连接上的尝试不再触发验活，新连接上卡住的尝试立即可以触发，不受旧窗口限制。
    #[test]
    fn gate_restarts_on_a_new_connection() {
        let mut gate = SendStallProbeGate::default();
        let t0 = 1_000_000;
        gate.record_probe(t0 + STALL_MS);
        let reconnected = t0 + STALL_MS + 2_000;
        gate.on_new_connection(reconnected);

        assert!(!gate.should_probe(reconnected + STALL_MS, [t0 + STALL_MS + 1]));
        assert!(gate.should_probe(reconnected + STALL_MS, [reconnected]));
    }

    /// 一批卡住的消息只验一次；没交给传输层（未连接、写失败）的尝试不算；回执及时的不验。
    #[tokio::test]
    async fn one_probe_for_a_batch_of_unanswered_sends() {
        let probe = Arc::new(CountingProbe::default());
        let mut st = queue_state(probe.clone());

        in_flight(&mut st, "fresh", 1_000, false);
        probe_link_if_send_stalled(&mut st);
        assert_eq!(
            probe.count(),
            0,
            "sent 1 s ago: still within a normal round trip"
        );

        in_flight(&mut st, "never-left", STALL_MS + 3_000, true);
        probe_link_if_send_stalled(&mut st);
        assert_eq!(
            probe.count(),
            0,
            "an attempt that never reached the transport says nothing about the connection"
        );

        in_flight(&mut st, "stalled-1", STALL_MS + 1_000, false);
        in_flight(&mut st, "stalled-2", STALL_MS + 2_000, false);
        for _ in 0..5 {
            probe_link_if_send_stalled(&mut st);
        }
        assert_eq!(probe.count(), 1, "one probe for the whole stalled batch");
    }

    /// 没装验活出口（如单测里的队列）时不做这项检查。
    #[tokio::test]
    async fn no_probe_without_a_link_probe() {
        let probe = Arc::new(CountingProbe::default());
        let mut st = queue_state(probe.clone());
        st.link_probe = None;
        in_flight(&mut st, "stalled", STALL_MS + 1_000, false);
        probe_link_if_send_stalled(&mut st);
        assert_eq!(probe.count(), 0);
        assert_eq!(st.stall_probe_gate.last_probe_ms, None);
    }
}
