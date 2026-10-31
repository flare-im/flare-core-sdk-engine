use crate::model::message::MessageLocalState;
use crate::model::message::MessageStatus;
use crate::model::message::{IMMessage, SendAck};
use flare_proto::common::{ErrorCode as ProtoErrorCode, SendAccepted, SendAckDurability, send_ack};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::shared::error::{ErrorCode, FlareError};

pub const REASON_PENDING_ANOTHER_ACCOUNT: &str = "pending message belongs to another account";
pub const REASON_MAX_RETRIES_EXCEEDED: &str = "max retries exceeded";
pub const REASON_TIMEOUT_AFTER_RETRIES: &str = "timeout after retries";
pub const REASON_RECONCILED_FAILED: &str = "in-flight reconciled to failed by local terminal state";
pub const REASON_SEND_FAILED_BEFORE_ACK_MAX_RETRIES: &str =
    "send failed before ack; max retries exceeded";
pub const REASON_ORPHAN_RECOVERED: &str = "orphan sending message reconciled during queue recovery";

/// 服务端暂时性拒绝后重发的退避基准与上限（毫秒）：第 n 次重发约等 `基准 × 2^(n-1)`，
/// 封顶后再取 ±20% 抖动，免得服务恢复的那一刻所有客户端同时重发。
pub const RETRYABLE_ACK_BACKOFF_BASE_MS: u64 = 1_000;
pub const RETRYABLE_ACK_BACKOFF_MAX_MS: u64 = 30_000;

/// 租户被停用或删除中的类型化原因；它不是暂时性的，重发只会再被拒。
const TENANT_UNAVAILABLE_REASON: &str = "TENANT_UNAVAILABLE";

/// The operation a send failure belongs to.
pub const SEND_FAILURE_OPERATION: &str = "message.send";

/// Why a message could not be sent, for hosts to act on without parsing text.
///
/// Wire (camelCase): `{code, machineReason, message, operation, retryable, details}`.
/// - `code`: the core error code wire name; for a server rejection it is mapped from
///   `ErrorDetail.code` (`PERMISSION_DENIED`), for a local failure it says what went
///   wrong on this side (`OPERATION_TIMEOUT`, `MESSAGE_SEND_FAILED`).
/// - `machineReason`: the server's typed reason, `ErrorDetail.reason` as sent (for
///   example a PreSend rejection `SOCIAL_CARD_SCOPE_FRIENDS`); empty for local failures.
/// - `message`: for people, never to be parsed.
/// - `details`: `remoteCode` (the server's numeric code) when the server rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SendFailure {
    pub code: String,
    #[serde(default)]
    pub machine_reason: String,
    pub message: String,
    pub operation: String,
    pub retryable: bool,
    #[serde(default)]
    pub details: BTreeMap<String, String>,
}

impl SendFailure {
    /// A failure on this side, with no server reason.
    pub fn local(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code: code.as_str().to_owned(),
            machine_reason: String::new(),
            message: message.into(),
            operation: SEND_FAILURE_OPERATION.to_owned(),
            retryable: false,
            details: BTreeMap::new(),
        }
    }

    /// The local failure behind one of the queue's reason strings: timeouts are
    /// `OPERATION_TIMEOUT`, everything else `MESSAGE_SEND_FAILED`.
    pub fn local_reason(reason: &str) -> Self {
        let code = if reason == REASON_TIMEOUT_AFTER_RETRIES || reason.contains("timeout") {
            ErrorCode::OperationTimeout
        } else {
            ErrorCode::MessageSendFailed
        };
        Self::local(code, reason)
    }

    /// The failure an SDK error stands for. A server code the error carries in its
    /// params (`machineReason`) travels as the machine reason.
    pub fn from_error(error: &FlareError) -> Self {
        let code = error.code().unwrap_or(ErrorCode::InternalError);
        let mut failure = Self::local(code, error.to_string());
        failure.retryable = error.is_retryable();
        if let FlareError::Localized {
            params: Some(params),
            ..
        } = error
            && let Some(reason) = params.get("machineReason")
        {
            failure.machine_reason = reason.trim().to_owned();
        }
        failure
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryLocalSnapshot {
    pub status: i32,
    pub server_id: String,
    pub seq: u64,
    pub conversation_id: String,
}

impl From<&IMMessage> for DeliveryLocalSnapshot {
    fn from(message: &IMMessage) -> Self {
        Self {
            status: message.status,
            server_id: message.server_id.clone(),
            seq: message.conversation_seq,
            conversation_id: message.conversation_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingDispatchDecision {
    DropAsCrossAccount { reason: &'static str },
    DropAsTerminal,
    FailMaxRetries { reason: &'static str },
    SendNow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecision {
    Retry { next_retry_count: u32 },
    Fail { reason: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InFlightReconcileDecision {
    KeepWaiting,
    MarkFailed { reason: &'static str },
    SynthesizeAck { snapshot: DeliveryLocalSnapshot },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncomingMessageConvergenceDecision {
    EmitReceived,
    MergePendingAndAck,
    DropDuplicate,
}

pub struct MessageDeliveryService;

impl MessageDeliveryService {
    pub fn normalize_incoming_server_state(message: &mut IMMessage) {
        let has_server_identity =
            !message.server_id.trim().is_empty() && message.conversation_seq > 0;
        let is_pending_wire_status = message.status <= MessageStatus::Created as i32;
        if has_server_identity && is_pending_wire_status {
            message.status = MessageStatus::Sent as i32;
        }
    }

    fn apply_send_ack_state(message: &mut IMMessage) {
        message.local_state = MessageLocalState {
            sending: false,
            failed: false,
            is_local: false,
            uploading: false,
            upload_progress: 0,
            sort_ts: message.local_state.sort_ts,
        };
        message.status = MessageStatus::Sent as i32;
        message.is_read = false;
    }

    pub fn decide_pending_dispatch(
        connected_user_id: &str,
        client_msg_id: &str,
        entry_sender_id: &str,
        local: Option<&DeliveryLocalSnapshot>,
        retries: u32,
        max_retries: u32,
    ) -> PendingDispatchDecision {
        if !connected_user_id.is_empty()
            && (entry_sender_id.trim().is_empty() || entry_sender_id != connected_user_id)
        {
            return PendingDispatchDecision::DropAsCrossAccount {
                reason: REASON_PENDING_ANOTHER_ACCOUNT,
            };
        }

        if let Some(local) = local {
            let already_final_success = local.status >= MessageStatus::Sent as i32
                && !local.server_id.trim().is_empty()
                && local.server_id != client_msg_id;
            let already_failed = local.status == MessageStatus::Failed as i32;
            if already_final_success || already_failed {
                return PendingDispatchDecision::DropAsTerminal;
            }
        }

        if retries >= max_retries {
            return PendingDispatchDecision::FailMaxRetries {
                reason: REASON_MAX_RETRIES_EXCEEDED,
            };
        }

        PendingDispatchDecision::SendNow
    }

    pub fn decide_send_attempt_failure(retries: u32, max_retries: u32) -> RetryDecision {
        let next_retry_count = retries.saturating_add(1);
        if next_retry_count >= max_retries {
            RetryDecision::Fail {
                reason: REASON_SEND_FAILED_BEFORE_ACK_MAX_RETRIES,
            }
        } else {
            RetryDecision::Retry { next_retry_count }
        }
    }

    /// SendAck 带的是服务端的暂时性拒绝（见 [`Self::is_retryable_ack_error`]）时是否再发一次。
    ///
    /// 与发送失败共用同一份重试预算（`max_retries` 是一条消息的总尝试次数），预算用完即判失败，
    /// 失败原因由调用方取服务端给的错误文本。
    pub fn decide_retryable_ack_error(retries: u32, max_retries: u32) -> RetryDecision {
        Self::decide_send_attempt_failure(retries, max_retries)
    }

    /// 第 `retry` 次（从 1 起）重发前的退避区间（毫秒，含两端）。
    pub fn retryable_ack_backoff_bounds_ms(retry: u32) -> (u64, u64) {
        let shift = retry.saturating_sub(1).min(16);
        let nominal = RETRYABLE_ACK_BACKOFF_BASE_MS
            .saturating_mul(1_u64 << shift)
            .min(RETRYABLE_ACK_BACKOFF_MAX_MS);
        let min = nominal.saturating_mul(80) / 100;
        let max = (nominal.saturating_mul(120) / 100).min(RETRYABLE_ACK_BACKOFF_MAX_MS);
        (min, max.max(min))
    }

    pub fn decide_timeout_expiry(retries: u32, max_retries: u32) -> RetryDecision {
        if retries >= max_retries {
            RetryDecision::Fail {
                reason: REASON_TIMEOUT_AFTER_RETRIES,
            }
        } else {
            RetryDecision::Retry {
                next_retry_count: retries.saturating_add(1),
            }
        }
    }

    pub fn reconcile_in_flight(
        local: Option<&DeliveryLocalSnapshot>,
        client_msg_id: &str,
    ) -> InFlightReconcileDecision {
        let Some(local) = local else {
            return InFlightReconcileDecision::KeepWaiting;
        };

        if local.status == MessageStatus::Failed as i32 {
            return InFlightReconcileDecision::MarkFailed {
                reason: REASON_RECONCILED_FAILED,
            };
        }

        let local_sent_like = local.status >= MessageStatus::Sent as i32
            && !local.server_id.trim().is_empty()
            && local.server_id != client_msg_id;
        if local_sent_like {
            return InFlightReconcileDecision::SynthesizeAck {
                snapshot: local.clone(),
            };
        }

        InFlightReconcileDecision::KeepWaiting
    }

    pub fn decide_incoming_message_convergence(
        current_user_id: &str,
        incoming: &IMMessage,
        local_by_client: Option<&IMMessage>,
        local_by_server: Option<&IMMessage>,
    ) -> IncomingMessageConvergenceDecision {
        if local_by_server.is_some() {
            return IncomingMessageConvergenceDecision::DropDuplicate;
        }

        if current_user_id.is_empty()
            || incoming.client_msg_id.trim().is_empty()
            || incoming.sender_id.trim().is_empty()
            || incoming.sender_id != current_user_id
        {
            return IncomingMessageConvergenceDecision::EmitReceived;
        }

        let Some(local) = local_by_client else {
            return IncomingMessageConvergenceDecision::EmitReceived;
        };
        let local_pending_like = local.server_id == local.client_msg_id
            || local.local_state.is_local
            || local.local_state.sending
            || local.status < MessageStatus::Sent as i32;
        if local_pending_like {
            IncomingMessageConvergenceDecision::MergePendingAndAck
        } else {
            IncomingMessageConvergenceDecision::EmitReceived
        }
    }

    pub fn mark_failed(message: &IMMessage) -> IMMessage {
        let mut failed_message = message.clone();
        failed_message.server_id = failed_message.client_msg_id.clone();
        failed_message.local_state = MessageLocalState {
            sending: false,
            failed: true,
            is_local: true,
            uploading: false,
            upload_progress: 0,
            sort_ts: failed_message.local_state.sort_ts,
        };
        failed_message.status = MessageStatus::Failed as i32;
        failed_message
    }

    pub fn mark_sent_from_ack(message: &IMMessage, ack: &SendAck) -> IMMessage {
        let mut sent_message = message.clone();
        let Some(accepted) = Self::durable_accepted_from_ack(ack) else {
            return sent_message;
        };
        if !accepted.server_msg_id.is_empty() {
            sent_message.server_id = accepted.server_msg_id.clone();
        }
        sent_message.conversation_seq = accepted.conversation_seq;
        // 与下行消息时间轴对齐；否则 ACK 后仍持客户端建消息时间，`sort_ts`/首屏排序可能弱于服务端历史而掉出 LIMIT。
        if accepted.server_time > 0 {
            sent_message.created_at = accepted.server_time as u64;
            sent_message.client_created_at = accepted.server_time as u64;
        }
        Self::apply_send_ack_state(&mut sent_message);
        sent_message
    }

    pub fn accepted_from_ack(ack: &SendAck) -> Option<&SendAccepted> {
        match ack.result.as_ref() {
            Some(send_ack::Result::Accepted(accepted)) => Some(accepted),
            _ => None,
        }
    }

    pub fn durable_accepted_from_ack(ack: &SendAck) -> Option<&SendAccepted> {
        let accepted = Self::accepted_from_ack(ack)?;
        match accepted.durability() {
            SendAckDurability::Persisted => Some(accepted),
            SendAckDurability::Unspecified
            | SendAckDurability::TransientAccepted
            | SendAckDurability::WalAccepted
            | SendAckDurability::BrokerAccepted => None,
        }
    }

    /// SendAck 的错误是否是服务端的暂时性拒绝：服务暂不可用、超时、限流或要求重试。
    ///
    /// 这类拒绝用同一个 `client_msg_id` 稍后重发即可——message-ingest 按它做摄入幂等，重发
    /// 不会多出一条消息（首发其实已成功时直接回放首发的结果；首发还在处理时回
    /// `SERVICE_UNAVAILABLE` 让客户端稍后再来）。其余错误（参数非法、无权限、租户不可用、
    /// 内部错误等）重发也不会变，按终态失败处理。
    pub fn is_retryable_ack_error(ack: &SendAck) -> bool {
        let Some(send_ack::Result::Error(error)) = ack.result.as_ref() else {
            return false;
        };
        if error.reason.contains(TENANT_UNAVAILABLE_REASON)
            || error.message.contains(TENANT_UNAVAILABLE_REASON)
        {
            return false;
        }
        matches!(
            ProtoErrorCode::try_from(error.code),
            Ok(ProtoErrorCode::Unavailable
                | ProtoErrorCode::Timeout
                | ProtoErrorCode::RateLimited
                | ProtoErrorCode::SlowDown
                | ProtoErrorCode::NeedsRetry)
        )
    }

    pub fn error_message_from_ack(ack: &SendAck) -> Option<String> {
        match ack.result.as_ref() {
            Some(send_ack::Result::Error(error)) => {
                let detail = if error.message.trim().is_empty() {
                    error.code.to_string()
                } else {
                    error.message.clone()
                };
                Some(detail)
            }
            _ => None,
        }
    }

    /// The structured failure of a rejected SendAck: the server's code, its typed reason
    /// (`ErrorDetail.reason`) and message. `None` when the ack is not a rejection.
    pub fn send_failure_from_ack(ack: &SendAck) -> Option<SendFailure> {
        let Some(send_ack::Result::Error(error)) = ack.result.as_ref() else {
            return None;
        };
        let message = Self::error_message_from_ack(ack).unwrap_or_default();
        let code = crate::shared::error::from_rpc_status(error.code, message.clone())
            .code()
            .unwrap_or(ErrorCode::GeneralError);
        let mut failure = SendFailure::local(code, message);
        failure.machine_reason = error.reason.trim().to_owned();
        failure
            .details
            .insert("remoteCode".to_owned(), error.code.to_string());
        Some(failure)
    }

    pub fn merge_incoming_as_sent(local: Option<&IMMessage>, incoming: &IMMessage) -> IMMessage {
        let mut merged = incoming.clone();
        merged.local_state = MessageLocalState {
            sending: false,
            failed: false,
            is_local: false,
            uploading: false,
            upload_progress: 0,
            sort_ts: local
                .map(|message| message.local_state.sort_ts)
                .unwrap_or(incoming.local_state.sort_ts.max(incoming.created_at)),
        };
        Self::apply_send_ack_state(&mut merged);
        merged
    }

    pub fn sanitize_send_ack_message(message: &IMMessage) -> IMMessage {
        let mut sanitized = message.clone();
        Self::apply_send_ack_state(&mut sanitized);
        sanitized
    }

    pub fn synthetic_ack(client_msg_id: &str, snapshot: &DeliveryLocalSnapshot) -> SendAck {
        SendAck {
            client_msg_id: client_msg_id.to_string(),
            conversation_id: snapshot.conversation_id.clone(),
            result: Some(send_ack::Result::Accepted(SendAccepted {
                server_msg_id: snapshot.server_id.clone(),
                conversation_seq: snapshot.seq,
                server_time: 0,
                durability: SendAckDurability::Persisted as i32,
            })),
            ..Default::default()
        }
    }

    pub fn synthetic_ack_from_incoming(incoming: &IMMessage) -> SendAck {
        SendAck {
            client_msg_id: incoming.client_msg_id.clone(),
            conversation_id: incoming.conversation_id.clone(),
            result: Some(send_ack::Result::Accepted(SendAccepted {
                server_msg_id: incoming.server_id.clone(),
                conversation_seq: incoming.conversation_seq,
                server_time: incoming.created_at as i64,
                durability: SendAckDurability::Persisted as i32,
            })),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DeliveryLocalSnapshot, InFlightReconcileDecision, IncomingMessageConvergenceDecision,
        MessageDeliveryService, PendingDispatchDecision, RetryDecision, SendFailure,
    };
    use crate::model::message::SendAck;
    use crate::model::message::{IMMessage, MessageStatus};
    use flare_proto::common::{SendAccepted, SendAckDurability, send_ack};

    fn accepted_ack(durability: SendAckDurability) -> SendAck {
        SendAck {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conv-1".to_string(),
            result: Some(send_ack::Result::Accepted(SendAccepted {
                server_msg_id: "server-1".to_string(),
                conversation_seq: 7,
                server_time: 123,
                durability: durability as i32,
            })),
            ..Default::default()
        }
    }

    #[test]
    fn durable_accepted_ack_requires_persisted_boundary() {
        assert!(
            MessageDeliveryService::durable_accepted_from_ack(&accepted_ack(
                SendAckDurability::WalAccepted
            ))
            .is_none()
        );
        assert!(
            MessageDeliveryService::durable_accepted_from_ack(&accepted_ack(
                SendAckDurability::BrokerAccepted
            ))
            .is_none()
        );
        assert!(
            MessageDeliveryService::durable_accepted_from_ack(&accepted_ack(
                SendAckDurability::Persisted
            ))
            .is_some()
        );
    }

    #[test]
    fn transient_or_unspecified_ack_is_not_durable() {
        assert!(
            MessageDeliveryService::durable_accepted_from_ack(&accepted_ack(
                SendAckDurability::TransientAccepted
            ))
            .is_none()
        );
        assert!(
            MessageDeliveryService::durable_accepted_from_ack(&accepted_ack(
                SendAckDurability::Unspecified
            ))
            .is_none()
        );
    }

    fn error_ack() -> SendAck {
        SendAck {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conv-1".to_string(),
            result: Some(send_ack::Result::Error(flare_proto::common::ErrorDetail {
                code: 13,
                reason: "MESSAGE_SEND_FAILED".to_string(),
                message: "pre_send rejected: SOCIAL_BLOCKED".to_string(),
                track: String::new(),
            })),
            ..Default::default()
        }
    }

    fn error_ack_with(code: i32, reason: &str, message: &str) -> SendAck {
        SendAck {
            client_msg_id: "client-1".to_string(),
            conversation_id: "conv-1".to_string(),
            result: Some(send_ack::Result::Error(flare_proto::common::ErrorDetail {
                code,
                reason: reason.to_string(),
                message: message.to_string(),
                track: String::new(),
            })),
            ..Default::default()
        }
    }

    /// 网关把 ingest 的服务不可用、超时、限流映射成这几个码（`proto_ack_error_code`）；
    /// 它们稍后用同一个 client_msg_id 重发即可。拒发、无权限、租户不可用、内部错误等不重发。
    #[test]
    fn only_transient_ack_errors_are_retryable() {
        use flare_proto::common::ErrorCode;
        for code in [
            ErrorCode::Unavailable,
            ErrorCode::Timeout,
            ErrorCode::RateLimited,
            ErrorCode::SlowDown,
            ErrorCode::NeedsRetry,
        ] {
            let ack = error_ack_with(
                code as i32,
                "SERVICE_UNAVAILABLE",
                "tenant runtime projection unavailable",
            );
            assert!(
                MessageDeliveryService::is_retryable_ack_error(&ack),
                "{code:?} must be retried"
            );
        }
        for code in [
            ErrorCode::Unspecified,
            ErrorCode::InvalidArgument,
            ErrorCode::UnsupportedOperation,
            ErrorCode::PermissionDenied,
            ErrorCode::ResourceExhausted,
            ErrorCode::Internal,
        ] {
            let ack = error_ack_with(code as i32, "MESSAGE_SEND_FAILED", "rejected");
            assert!(
                !MessageDeliveryService::is_retryable_ack_error(&ack),
                "{code:?} is terminal"
            );
        }
        assert!(!MessageDeliveryService::is_retryable_ack_error(&error_ack()));
        assert!(!MessageDeliveryService::is_retryable_ack_error(
            &accepted_ack(SendAckDurability::Persisted)
        ));
    }

    /// 租户停用是终态：即使某一层把它包成了可重试的码，也不重发。
    #[test]
    fn tenant_unavailable_is_never_retried() {
        use flare_proto::common::ErrorCode;
        for (reason, message) in [
            ("TENANT_UNAVAILABLE", "tenant suspended"),
            ("MESSAGE_SEND_FAILED", "TENANT_UNAVAILABLE"),
        ] {
            let ack = error_ack_with(ErrorCode::Unavailable as i32, reason, message);
            assert!(!MessageDeliveryService::is_retryable_ack_error(&ack));
        }
        let ack = error_ack_with(
            ErrorCode::PermissionDenied as i32,
            "TENANT_UNAVAILABLE",
            "TENANT_UNAVAILABLE",
        );
        assert!(!MessageDeliveryService::is_retryable_ack_error(&ack));
    }

    /// 暂时性拒绝与发送失败共用一份预算：`max_retries` 是一条消息的总尝试次数。
    #[test]
    fn retryable_ack_errors_share_the_bounded_retry_budget() {
        assert_eq!(
            MessageDeliveryService::decide_retryable_ack_error(0, 3),
            RetryDecision::Retry {
                next_retry_count: 1
            }
        );
        assert_eq!(
            MessageDeliveryService::decide_retryable_ack_error(1, 3),
            RetryDecision::Retry {
                next_retry_count: 2
            }
        );
        assert!(matches!(
            MessageDeliveryService::decide_retryable_ack_error(2, 3),
            RetryDecision::Fail { .. }
        ));
        assert!(matches!(
            MessageDeliveryService::decide_retryable_ack_error(0, 1),
            RetryDecision::Fail { .. }
        ));
    }

    #[test]
    fn retryable_ack_backoff_doubles_with_jitter_and_is_capped() {
        assert_eq!(
            MessageDeliveryService::retryable_ack_backoff_bounds_ms(1),
            (800, 1_200)
        );
        assert_eq!(
            MessageDeliveryService::retryable_ack_backoff_bounds_ms(2),
            (1_600, 2_400)
        );
        assert_eq!(
            MessageDeliveryService::retryable_ack_backoff_bounds_ms(3),
            (3_200, 4_800)
        );
        assert_eq!(
            MessageDeliveryService::retryable_ack_backoff_bounds_ms(6),
            (24_000, 30_000)
        );
        assert_eq!(
            MessageDeliveryService::retryable_ack_backoff_bounds_ms(u32::MAX),
            (24_000, 30_000)
        );
    }

    #[test]
    fn error_ack_is_terminal_failure_not_accepted() {
        // 服务端 pre_send 拒发(拉黑/非好友)→ SendAck.result = Error。客户端据此判定为失败
        // (既非 accepted 也非 durable),驱动 reliable_queue::apply_ack_and_publish 走 mark_failed 分支,
        // 而非误当已送达/AwaitDurable。回归护栏:失败发送必须能被前端渲染为「发送失败/重发」。
        let ack = error_ack();
        assert!(MessageDeliveryService::accepted_from_ack(&ack).is_none());
        assert!(MessageDeliveryService::durable_accepted_from_ack(&ack).is_none());
        assert_eq!(
            MessageDeliveryService::error_message_from_ack(&ack).as_deref(),
            Some("pre_send rejected: SOCIAL_BLOCKED")
        );
    }

    /// 服务端拒发时，结构化原因带上 `ErrorDetail.reason`：宿主按它分辨名片范围、敏感词、
    /// 插件等 PreSend 拒绝，不去解析文案。
    #[test]
    fn a_rejected_ack_keeps_the_server_reason_as_the_machine_reason() {
        let ack = error_ack_with(
            21,
            "SOCIAL_CARD_SCOPE_FRIENDS",
            "only friends can share this card",
        );
        let failure = MessageDeliveryService::send_failure_from_ack(&ack).expect("a rejection");
        assert_eq!(failure.machine_reason, "SOCIAL_CARD_SCOPE_FRIENDS");
        assert_eq!(failure.code, "PERMISSION_DENIED");
        assert_eq!(failure.message, "only friends can share this card");
        assert_eq!(failure.operation, "message.send");
        assert!(!failure.retryable);
        assert_eq!(
            failure.details.get("remoteCode").map(String::as_str),
            Some("21")
        );
        assert_eq!(
            serde_json::to_value(&failure).unwrap(),
            serde_json::json!({
                "code": "PERMISSION_DENIED",
                "machineReason": "SOCIAL_CARD_SCOPE_FRIENDS",
                "message": "only friends can share this card",
                "operation": "message.send",
                "retryable": false,
                "details": {"remoteCode": "21"}
            })
        );
        assert!(
            MessageDeliveryService::send_failure_from_ack(&accepted_ack(
                SendAckDurability::Persisted
            ))
            .is_none()
        );
    }

    /// The shared event golden is what the core emits for a server rejection.
    #[test]
    fn a_rejected_ack_matches_the_shared_send_failed_golden() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../flare-im-core-client-sdk/sdk-spec/golden/events/send_failed_with_error.json");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        let golden: serde_json::Value = serde_json::from_str(&text).expect("golden json");
        let error = &golden["error"];
        let ack = error_ack_with(
            error["details"]["remoteCode"]
                .as_str()
                .and_then(|code| code.parse().ok())
                .expect("remoteCode"),
            error["machineReason"].as_str().expect("machineReason"),
            error["message"].as_str().expect("message"),
        );
        let failure = MessageDeliveryService::send_failure_from_ack(&ack).expect("a rejection");
        assert_eq!(&serde_json::to_value(&failure).unwrap(), error);
        assert_eq!(
            MessageDeliveryService::error_message_from_ack(&ack).as_deref(),
            golden["reason"].as_str()
        );
    }

    #[test]
    fn local_send_failures_carry_a_local_code_and_no_machine_reason() {
        let timeout = SendFailure::local_reason(super::REASON_TIMEOUT_AFTER_RETRIES);
        assert_eq!(timeout.code, "OPERATION_TIMEOUT");
        assert_eq!(timeout.machine_reason, "");
        assert_eq!(timeout.message, super::REASON_TIMEOUT_AFTER_RETRIES);
        let other = SendFailure::local_reason(super::REASON_PENDING_ANOTHER_ACCOUNT);
        assert_eq!(other.code, "MESSAGE_SEND_FAILED");
        assert_eq!(other.machine_reason, "");
    }

    #[test]
    fn mark_failed_sets_failed_status_and_local_state() {
        // mark_failed 必须产出 proto MessageStatus::Failed(4) + local_state.failed,
        // 前端 messageStateToNumber 据此显示失败/重发指示(而非单勾已送达)。
        let mut msg = IMMessage::new(flare_proto::common::Message::default());
        msg.client_msg_id = "client-1".to_string();
        msg.status = MessageStatus::Sent as i32;
        msg.local_state.sending = true;

        let failed = MessageDeliveryService::mark_failed(&msg);
        assert_eq!(failed.status, MessageStatus::Failed as i32);
        assert!(failed.local_state.failed);
        assert!(!failed.local_state.sending);
    }

    #[test]
    fn pending_dispatch_drops_cross_account_entry() {
        let decision =
            MessageDeliveryService::decide_pending_dispatch("u1", "c1", "u2", None, 0, 3);

        assert!(matches!(
            decision,
            PendingDispatchDecision::DropAsCrossAccount { .. }
        ));
    }

    #[test]
    fn pending_dispatch_drops_terminal_local_entry() {
        let local = DeliveryLocalSnapshot {
            status: MessageStatus::Sent as i32,
            server_id: "s1".to_string(),
            seq: 1,
            conversation_id: "c1".to_string(),
        };
        let decision =
            MessageDeliveryService::decide_pending_dispatch("u1", "c1", "u1", Some(&local), 0, 3);

        assert_eq!(decision, PendingDispatchDecision::DropAsTerminal);
    }

    #[test]
    fn timeout_decision_retries_before_max() {
        let decision = MessageDeliveryService::decide_timeout_expiry(1, 3);

        assert_eq!(
            decision,
            RetryDecision::Retry {
                next_retry_count: 2
            }
        );
    }

    #[test]
    fn reconcile_in_flight_synthesizes_ack_for_sent_like_local() {
        let local = DeliveryLocalSnapshot {
            status: MessageStatus::Sent as i32,
            server_id: "server-1".to_string(),
            seq: 9,
            conversation_id: "c1".to_string(),
        };

        let decision = MessageDeliveryService::reconcile_in_flight(Some(&local), "client-1");

        match decision {
            InFlightReconcileDecision::SynthesizeAck { snapshot } => {
                assert_eq!(snapshot.server_id, "server-1");
                assert_eq!(snapshot.seq, 9);
            }
            _ => panic!("expected synthetic ack decision"),
        }
    }

    #[test]
    fn incoming_duplicate_by_server_id_is_dropped() {
        let mut incoming = IMMessage::new(flare_proto::common::Message::default());
        incoming.server_id = "server-1".to_string();
        incoming.sender_id = "u2".to_string();

        let decision = MessageDeliveryService::decide_incoming_message_convergence(
            "u1",
            &incoming,
            None,
            Some(&incoming),
        );

        assert_eq!(decision, IncomingMessageConvergenceDecision::DropDuplicate);
    }

    #[test]
    fn incoming_server_backed_created_status_is_normalized_to_sent() {
        let mut incoming = IMMessage::new(flare_proto::common::Message::default());
        incoming.server_id = "server-1".to_string();
        incoming.conversation_seq = 7;
        incoming.status = MessageStatus::Created as i32;

        MessageDeliveryService::normalize_incoming_server_state(&mut incoming);

        assert_eq!(incoming.status, MessageStatus::Sent as i32);

        let mut local_only = IMMessage::new(flare_proto::common::Message::default());
        local_only.status = MessageStatus::Created as i32;

        MessageDeliveryService::normalize_incoming_server_state(&mut local_only);

        assert_eq!(local_only.status, MessageStatus::Created as i32);
    }

    #[test]
    fn incoming_self_echo_is_send_ack_not_read_receipt() {
        let mut local = IMMessage::new(flare_proto::common::Message::default());
        local.client_msg_id = "client-1".to_string();
        local.server_id = "client-1".to_string();
        local.sender_id = "u1".to_string();
        local.status = MessageStatus::Created as i32;
        local.is_read = false;
        local.local_state.sending = true;
        local.local_state.is_local = true;
        local.local_state.sort_ts = 42;

        let mut incoming = IMMessage::new(flare_proto::common::Message::default());
        incoming.client_msg_id = "client-1".to_string();
        incoming.server_id = "server-1".to_string();
        incoming.sender_id = "u1".to_string();
        incoming.conversation_seq = 7;
        incoming.status = MessageStatus::Persisted as i32;
        incoming.is_read = true;

        let merged = MessageDeliveryService::merge_incoming_as_sent(Some(&local), &incoming);

        assert_eq!(merged.server_id, "server-1");
        assert_eq!(merged.conversation_seq, 7);
        assert_eq!(merged.status, MessageStatus::Sent as i32);
        assert!(!merged.is_read);
        assert!(!merged.local_state.sending);
        assert!(!merged.local_state.is_local);
        assert_eq!(merged.local_state.sort_ts, 42);
    }

    #[test]
    fn send_ack_sanitizer_never_preserves_read_state() {
        let mut message = IMMessage::new(flare_proto::common::Message::default());
        message.status = MessageStatus::Persisted as i32;
        message.is_read = true;
        message.local_state.sending = true;
        message.local_state.is_local = true;

        let sanitized = MessageDeliveryService::sanitize_send_ack_message(&message);

        assert_eq!(sanitized.status, MessageStatus::Sent as i32);
        assert!(!sanitized.is_read);
        assert!(!sanitized.local_state.sending);
        assert!(!sanitized.local_state.is_local);
    }
}
