//! 下行载荷：与 flare-proto 对齐；DATA 经 `data.proto` `DataPacket.payload` oneof 解码。

use flare_proto::common::{
    CapabilityPacket, CustomData, Event, EventEnvelope, MessagePush, RealtimeControlPacket,
    SendAck, SyncRes, TenantNotice,
};

/// 服务端下行包载荷（与 proto 类型一一对应）
#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum DownlinkPayload {
    /// 消息推送（message.proto MessagePush）
    MessagePush(MessagePush),
    /// 单条事件（event.proto Event）
    Event(Event),
    /// 事件批（event.proto EventEnvelope）
    EventEnvelope(EventEnvelope),
    /// 发消息回执（ack.proto Ack.payload.send）
    SendAck(SendAck),
    /// 自定义数据（data.proto CustomData）
    CustomData(CustomData),
    /// 能力包（data.proto CapabilityPacket），不占用 conversation_seq
    Capability(CapabilityPacket),
    /// 实时控制包（typing/presence/custom），不占用 conversation_seq
    RealtimeControl(RealtimeControlPacket),
    /// 租户配置变更通知（data.proto TenantNotice），不占用 conversation_seq
    TenantNotice(TenantNotice),
    /// 单会话同步响应（sync.proto SyncRes）
    SyncResp(SyncRes),
}
