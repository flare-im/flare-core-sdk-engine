use std::sync::Arc;
use std::time::Duration;

use crate::domain::MessageStore;
use crate::extension::middleware::{MessageMiddlewareContext, MessageOperation, MiddlewareChain};
use crate::infrastructure::protocol::PacketSender;
use crate::kernel::ReliableSendQueuePort;
use crate::model::message::{IMMessage, SendAck};
use crate::shared::error::Result;

const TIMEOUT_SECS: u64 = 15;

pub struct SendMessageCommand {
    pub message: IMMessage,
}

impl SendMessageCommand {
    pub fn new(message: IMMessage) -> Self {
        Self { message }
    }

    /// 直接发送并落库（不走可靠队列，仅当未配置 PendingSend Reader/Writer 时使用）
    ///
    /// 这条路径没有暂时性重试：发出帧即落库返回，不等 SendAck，服务端的暂时性拒绝不会重发；
    /// 长连接断开时 flare-core 客户端也不再自己重连，未连接、发送超时直接把错误还给调用方。
    /// 账号准备阶段校验发件箱端口，发送走 [`Self::execute_via_queue`]。
    /// 低层 builder 显式注入 `in_memory_empty_im_provider` 等最小仓储时可走直发路径。
    pub async fn execute(
        self,
        sender: &Arc<PacketSender>,
        store: &dyn MessageStore,
        chain: &MiddlewareChain,
    ) -> Result<SendAck> {
        let ctx = MessageMiddlewareContext::new(MessageOperation::DirectSend);
        // 消费 self：发送热路径不整体克隆 IMMessage（媒体消息 encoded_content 可达 KB 级）。
        let mut msg = self.message;
        if let Err(error) = chain.before_send(&mut msg, &ctx).await {
            chain.notify_send_error(&msg, &error, &ctx).await;
            return Err(error);
        }
        msg.materialize_encoded_content_from_elem();
        let proto = msg.to_proto();
        if let Err(error) = sender
            .send_message(&proto, Duration::from_secs(TIMEOUT_SECS))
            .await
        {
            chain.notify_send_error(&msg, &error, &ctx).await;
            return Err(error);
        }
        if let Err(error) = store.save_batch(std::slice::from_ref(&msg)).await {
            chain.notify_send_error(&msg, &error, &ctx).await;
            return Err(error);
        }
        let ack = SendAck {
            client_msg_id: msg.client_msg_id.clone(),
            conversation_id: msg.conversation_id.clone(),
            ack_id: None,
            result: None,
        };
        chain.after_send(&msg, Some(&ack), &ctx).await;
        Ok(ack)
    }

    /// 经可靠队列入队（SendAck 由调用方通过 EventBus 等待）
    pub async fn execute_via_queue(
        self,
        queue: &dyn ReliableSendQueuePort,
        chain: &MiddlewareChain,
    ) -> Result<()> {
        let ctx = MessageMiddlewareContext::new(MessageOperation::ReliableQueueEnqueue);
        // 消费 self：入队热路径不整体克隆（拦截器路径仍需一份 msg 供 after_send 使用）。
        let mut msg = self.message;
        if let Err(error) = chain.before_send(&mut msg, &ctx).await {
            chain.notify_send_error(&msg, &error, &ctx).await;
            return Err(error);
        }
        msg.materialize_encoded_content_from_elem();
        if chain.has_message_interceptors() {
            if let Err(error) = queue.enqueue(msg.clone()).await {
                chain.notify_send_error(&msg, &error, &ctx).await;
                return Err(error);
            }
            chain.after_send(&msg, None, &ctx).await;
            Ok(())
        } else {
            queue.enqueue(msg).await
        }
    }
}
