//! 协议层（编解码 + 上行发送）
//!
//! 与 **flare-proto** 对齐：message.proto Message/MessagePush、event.proto Event、ack.proto Ack、data.proto `DataPacket`。
//! Pipeline：Decode → Dispatcher → EventBus。
//!
//! ## 上下行类型（PayloadCommand.type → payload 内容）
//!
//! | 方向 | PayloadCommand.type | payload 内容 | proto |
//! |------|----------------------|--------------|-------|
//! | 上行 | Message (1) | Message | message.proto |
//! | 上行 | Event (2) | Event | event.proto |
//! | 上行 | Ack (3) | Ack | ack.proto |
//! | 上行 | Data (4) | `DataPacket`（`sync_request` / `user_custom`） | data.proto + sync.proto |
//! | 下行 | Message (1) / Data (4) | MessagePush / Event / EventEnvelope / `DataPacket`（`sync_response` / `user_custom`） | 同上 |
//! | 下行 | Ack (3) | Ack(Send) | ack.proto |

pub(crate) mod ack_ledger;
pub mod codec;
pub mod downlink;
pub use codec::{Codec, MAX_DOWNLINK_PAYLOAD_BYTES, ProtobufCodec};
pub use downlink::DownlinkPayload;
pub use packet_sender::PacketSender;

mod packet_sender;
