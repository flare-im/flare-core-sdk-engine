//! 会话模型 — SDK 内部统一使用 Conversation；从 proto ConversationSummary 获取后即转换为 Conversation。
//! 序列化用 serde camelCase JSON；client 不再做命名桥接转换。

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::content::message_elem::{
    MessagePreviewElem, message_preview_from_proto, message_preview_to_proto,
};
use crate::domain::display_name::DisplayName;

fn proto_ms_to_u64(ms: i64) -> u64 {
    if ms > 0 { ms as u64 } else { 0 }
}

fn u64_to_proto_ms(ms: u64) -> i64 {
    i64::try_from(ms).unwrap_or(i64::MAX)
}

// ---------- 会话类型（严格对齐 `flare.common.v1.ConversationType`，并与 flare_core CID 前缀一致）----------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConversationTypePolicy {
    pub conversation_type: ConversationType,
    pub wire_value: i32,
    pub wire_name: &'static str,
    pub cid_prefix: Option<&'static str>,
    pub subscriber_model: bool,
    pub member_lookup_required: bool,
}

pub const DEFAULT_READ_FANOUT_GROUP_MEMBER_THRESHOLD: u32 = 5_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConversationFanoutMode {
    FanOutOnWrite,
    FanOutOnRead,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConversationTimelinePolicy {
    pub fanout_mode: ConversationFanoutMode,
    pub shared_timeline: bool,
    pub attention_scoped_subscription: bool,
    pub member_presence_aggregation: bool,
    pub materialize_member_table: bool,
}

pub const CONVERSATION_TYPE_WIRE_ORDER: &[ConversationType] = &[
    ConversationType::Unspecified,
    ConversationType::Single,
    ConversationType::Group,
    ConversationType::Ai,
    ConversationType::System,
    ConversationType::Customer,
    ConversationType::Temp,
    ConversationType::Channel,
    ConversationType::Broadcast,
];

pub const CONVERSATION_TYPE_POLICIES: &[ConversationTypePolicy] = &[
    ConversationTypePolicy {
        conversation_type: ConversationType::Unspecified,
        wire_value: 0,
        wire_name: "unspecified",
        cid_prefix: None,
        subscriber_model: false,
        member_lookup_required: false,
    },
    ConversationTypePolicy {
        conversation_type: ConversationType::Single,
        wire_value: 1,
        wire_name: "single",
        cid_prefix: Some("1"),
        subscriber_model: false,
        member_lookup_required: false,
    },
    ConversationTypePolicy {
        conversation_type: ConversationType::Group,
        wire_value: 2,
        wire_name: "group",
        cid_prefix: Some("2"),
        subscriber_model: true,
        member_lookup_required: true,
    },
    ConversationTypePolicy {
        conversation_type: ConversationType::Ai,
        wire_value: 3,
        wire_name: "ai",
        cid_prefix: Some("3"),
        subscriber_model: true,
        member_lookup_required: true,
    },
    ConversationTypePolicy {
        conversation_type: ConversationType::System,
        wire_value: 4,
        wire_name: "system",
        cid_prefix: Some("4"),
        subscriber_model: true,
        member_lookup_required: true,
    },
    ConversationTypePolicy {
        conversation_type: ConversationType::Customer,
        wire_value: 5,
        wire_name: "customer",
        cid_prefix: Some("5"),
        subscriber_model: true,
        member_lookup_required: true,
    },
    ConversationTypePolicy {
        conversation_type: ConversationType::Temp,
        wire_value: 6,
        wire_name: "temp",
        cid_prefix: Some("6"),
        subscriber_model: false,
        member_lookup_required: false,
    },
    ConversationTypePolicy {
        conversation_type: ConversationType::Channel,
        wire_value: 7,
        wire_name: "channel",
        cid_prefix: Some("7"),
        subscriber_model: true,
        member_lookup_required: true,
    },
    ConversationTypePolicy {
        conversation_type: ConversationType::Broadcast,
        wire_value: 8,
        wire_name: "broadcast",
        cid_prefix: Some("8"),
        subscriber_model: true,
        member_lookup_required: true,
    },
];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConversationType {
    #[default]
    Unspecified,
    Single,
    Group,
    Ai,
    System,
    Customer,
    Temp,
    Channel,
    Broadcast,
}

impl ConversationType {
    pub fn policy(self) -> ConversationTypePolicy {
        match self {
            ConversationType::Unspecified => ConversationTypePolicy {
                conversation_type: ConversationType::Unspecified,
                wire_value: 0,
                wire_name: "unspecified",
                cid_prefix: None,
                subscriber_model: false,
                member_lookup_required: false,
            },
            ConversationType::Single => ConversationTypePolicy {
                conversation_type: ConversationType::Single,
                wire_value: 1,
                wire_name: "single",
                cid_prefix: Some("1"),
                subscriber_model: false,
                member_lookup_required: false,
            },
            ConversationType::Group => ConversationTypePolicy {
                conversation_type: ConversationType::Group,
                wire_value: 2,
                wire_name: "group",
                cid_prefix: Some("2"),
                subscriber_model: true,
                member_lookup_required: true,
            },
            ConversationType::Ai => ConversationTypePolicy {
                conversation_type: ConversationType::Ai,
                wire_value: 3,
                wire_name: "ai",
                cid_prefix: Some("3"),
                subscriber_model: true,
                member_lookup_required: true,
            },
            ConversationType::System => ConversationTypePolicy {
                conversation_type: ConversationType::System,
                wire_value: 4,
                wire_name: "system",
                cid_prefix: Some("4"),
                subscriber_model: true,
                member_lookup_required: true,
            },
            ConversationType::Customer => ConversationTypePolicy {
                conversation_type: ConversationType::Customer,
                wire_value: 5,
                wire_name: "customer",
                cid_prefix: Some("5"),
                subscriber_model: true,
                member_lookup_required: true,
            },
            ConversationType::Temp => ConversationTypePolicy {
                conversation_type: ConversationType::Temp,
                wire_value: 6,
                wire_name: "temp",
                cid_prefix: Some("6"),
                subscriber_model: false,
                member_lookup_required: false,
            },
            ConversationType::Channel => ConversationTypePolicy {
                conversation_type: ConversationType::Channel,
                wire_value: 7,
                wire_name: "channel",
                cid_prefix: Some("7"),
                subscriber_model: true,
                member_lookup_required: true,
            },
            ConversationType::Broadcast => ConversationTypePolicy {
                conversation_type: ConversationType::Broadcast,
                wire_value: 8,
                wire_name: "broadcast",
                cid_prefix: Some("8"),
                subscriber_model: true,
                member_lookup_required: true,
            },
        }
    }

    pub fn wire_order() -> &'static [ConversationType] {
        CONVERSATION_TYPE_WIRE_ORDER
    }

    /// 返回与 proto/API 一致的类型字符串
    pub fn as_str(self) -> &'static str {
        self.policy().wire_name
    }

    /// CID 类型前缀（与 flare_core 一致：1=单聊 2=群聊 3=AI 4=系统 5=客服 6=临时）
    pub fn prefix(self) -> Option<&'static str> {
        self.policy().cid_prefix
    }

    /// 从 CID 类型前缀解析（如 "1" -> Single）
    pub fn from_prefix(prefix: &str) -> Option<Self> {
        CONVERSATION_TYPE_POLICIES
            .iter()
            .find(|policy| policy.cid_prefix == Some(prefix))
            .map(|policy| policy.conversation_type)
    }

    /// 是否为单聊会话（与 [crate::domain::conversation::id::is_single_chat_conversation] 语义一致）
    pub fn is_single_chat_conversation(&self) -> bool {
        *self == ConversationType::Single
    }

    /// 是否为群聊会话（与 [crate::domain::conversation::id::is_group_chat_conversation] 语义一致）
    pub fn is_group_chat_conversation(&self) -> bool {
        *self == ConversationType::Group
    }

    pub fn uses_subscriber_model(self) -> bool {
        self.policy().subscriber_model
    }

    pub fn requires_member_lookup(self) -> bool {
        self.policy().member_lookup_required
    }

    pub fn fanout_mode(
        self,
        members_count: u32,
        server_hint: Option<ConversationFanoutMode>,
    ) -> ConversationFanoutMode {
        server_hint.unwrap_or_else(|| {
            self.fanout_mode_with_threshold(
                members_count,
                DEFAULT_READ_FANOUT_GROUP_MEMBER_THRESHOLD,
            )
        })
    }

    pub fn fanout_mode_with_threshold(
        self,
        members_count: u32,
        large_group_threshold: u32,
    ) -> ConversationFanoutMode {
        match self {
            ConversationType::Channel | ConversationType::Broadcast => {
                ConversationFanoutMode::FanOutOnRead
            }
            ConversationType::Group if members_count >= large_group_threshold.max(1) => {
                ConversationFanoutMode::FanOutOnRead
            }
            _ => ConversationFanoutMode::FanOutOnWrite,
        }
    }

    pub fn timeline_policy(
        self,
        members_count: u32,
        server_hint: Option<ConversationFanoutMode>,
    ) -> ConversationTimelinePolicy {
        let fanout_mode = self.fanout_mode(members_count, server_hint);
        let shared_timeline = fanout_mode == ConversationFanoutMode::FanOutOnRead;

        ConversationTimelinePolicy {
            fanout_mode,
            shared_timeline,
            attention_scoped_subscription: shared_timeline,
            member_presence_aggregation: shared_timeline,
            materialize_member_table: !shared_timeline && self.requires_member_lookup(),
        }
    }

    /// 与 [`flare_proto::common::ConversationType`] 数值一致（`Message.conversation_type`、Orchestrator 推送等）
    pub fn to_proto_int(self) -> i32 {
        self.policy().wire_value
    }

    /// 从消息 proto 的 `conversation_type` 整型解析；未知值映射为 [`Unspecified`]
    pub fn from_proto_int(v: i32) -> Self {
        use flare_proto::common::ConversationType as ProtoT;
        match ProtoT::try_from(v) {
            Ok(ProtoT::Unspecified) => ConversationType::Unspecified,
            Ok(ProtoT::Single) => ConversationType::Single,
            Ok(ProtoT::Group) => ConversationType::Group,
            Ok(ProtoT::Ai) => ConversationType::Ai,
            Ok(ProtoT::Customer) => ConversationType::Customer,
            Ok(ProtoT::System) => ConversationType::System,
            Ok(ProtoT::Temp) => ConversationType::Temp,
            Ok(ProtoT::Channel) => ConversationType::Channel,
            Ok(ProtoT::Broadcast) => ConversationType::Broadcast,
            Err(_) => ConversationType::Unspecified,
        }
    }
}

impl From<&str> for ConversationType {
    fn from(s: &str) -> Self {
        let normalized = s.trim().to_ascii_lowercase();
        for policy in CONVERSATION_TYPE_POLICIES {
            if policy.wire_name == normalized {
                return policy.conversation_type;
            }
        }
        ConversationType::Unspecified
    }
}

// ---------- 本地状态（不序列化到 JSON/DB，仅内存）----------

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConversationLocalState {
    /// 草稿光标等 UI 状态可放此处
    #[serde(default)]
    pub draft_cursor: Option<u32>,
}

/// SDK 本地会话参与者快照。单聊不依赖该结构；群聊/频道/客服等非单聊用它支撑群通话、成员面板和后续设置页。
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[serde(rename_all = "camelCase")]
pub struct ConversationParticipant {
    pub user_id: String,
    pub roles: Vec<String>,
    pub muted: bool,
    pub pinned: bool,
    pub attributes: HashMap<String, String>,
    pub joined_at: u64,
    pub nickname: String,
    /// 成员在这个会话里显示成什么（成员列表、@ 选择用）：别名、会话内昵称、资料名，
    /// 都没有时为结构化的「无名」。读取时由核心投影，不落库；SDK 交出的成员都带它。
    pub name: DisplayName,
}

impl ConversationParticipant {
    /// 成员行自己能说明的名字：会话内昵称，没有就是结构化的「无名」。
    /// 读取会话时的投影（别名、写入的会话内昵称、资料名）会覆盖它。
    pub fn nickname_name(user_id: &str, nickname: &str) -> DisplayName {
        crate::domain::resolve_person_name(
            user_id,
            &crate::domain::PersonNameCandidates {
                member_nickname: nickname,
                ..Default::default()
            },
            Some(ConversationType::Group),
        )
    }
}

impl From<flare_proto::common::ConversationParticipant> for ConversationParticipant {
    fn from(p: flare_proto::common::ConversationParticipant) -> Self {
        let nickname = p.attributes.get("nickname").cloned().unwrap_or_default();
        let name = ConversationParticipant::nickname_name(&p.user_id, &nickname);
        Self {
            user_id: p.user_id,
            roles: p.roles,
            muted: p.muted,
            pinned: p.pinned,
            attributes: p.attributes,
            joined_at: proto_ms_to_u64(p.joined_at),
            nickname,
            name,
        }
    }
}

impl From<ConversationParticipant> for flare_proto::common::ConversationParticipant {
    fn from(p: ConversationParticipant) -> Self {
        let mut attributes = p.attributes.clone();
        if !p.nickname.is_empty() {
            attributes
                .entry("nickname".to_string())
                .or_insert_with(|| p.nickname.clone());
        }
        Self {
            user_id: p.user_id,
            roles: p.roles,
            muted: p.muted,
            pinned: p.pinned,
            attributes,
            joined_at: u64_to_proto_ms(p.joined_at),
            // 历史下限由服务端持有并在读取时执行，客户端无权设置：
            // 回传 0 意为「本次请求不修改下限」（服务端对该字段只升不降）。
            visible_from_seq: 0,
            // 「只接收@我」同理：由业务侧（群通知设置）投影下来，不由客户端回传。
            mention_only: false,
        }
    }
}

/// SDK 层会话类型：内部统一使用，从 proto ConversationSummary 获取后即转换为此类型。
/// 与 message.rs 的 IMMessage 一致：扁平字段、serde camelCase。
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[serde(rename_all = "camelCase")]
pub struct Conversation {
    // ===============================
    // Identity
    // ===============================
    pub conversation_id: String,
    pub conversation_type: ConversationType,
    pub business_type: String,
    /// 会话路由 ID：单聊为对方 user_id；群/频道为业务 channel（与 proto `channel_id` 一致）
    pub channel_id: String,
    pub members_count: u32,

    // ===============================
    // Display
    // ===============================
    /// 业务名（服务端或业务 SDK 给的会话名），可为空，永不为 id；列表标题读 `title`。
    pub display_name: String,
    /// 列表与聊天头显示的会话标题：备注、单聊对端人名、业务名、无名群的成员名拼接，
    /// 都没有时为结构化的「无名」。读取时由核心投影，不落库；SDK 交出的会话都带它。
    pub title: DisplayName,
    pub avatar_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remark: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    // ===============================
    // Last Message
    // ===============================
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sender_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_message: Option<MessagePreviewElem>,
    /// 旧字段：最后一条消息发送者在身份缓存里的资料名（是真名字时），否则为本地已存的名字或空串，
    /// 永不为 id。列表读 `last_sender`。
    pub last_sender_nickname: String,
    /// 最后一条消息的发送者名（人名解析链，群会话含会话内昵称）；没有最后一条消息时缺省。
    /// 读取时由核心投影，不落库。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sender: Option<DisplayName>,
    /// 最后一条消息发送者头像 URL
    pub last_sender_avatar_url: String,

    // ===============================
    // Unread
    // ===============================
    pub unread_count: u32,
    /// 已读序列号（与 read_seq 同义）
    pub last_read_seq: u64,
    /// 对端（其他成员）最大已读序列号；用于发送方已读双勾在重连/重登后恢复。
    /// 由服务端同步摘要 `ext.peer_read_seq` 下发并持久化。
    pub peer_read_seq: u64,
    pub max_seq: u64,
    /// 当前用户的历史可见边界；seq <= visible_after_seq 的消息不可见，不参与冷启动回灌。
    pub visible_after_seq: u64,

    // ===============================
    // User Settings
    // ===============================
    pub is_pinned: bool,
    pub is_muted: bool,
    /// Notification preference. None means the sync source did not supply it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_mention_only: Option<bool>,
    /// In a large group, whether the current user turned on notifications for every message
    /// (large groups otherwise alert only on mentions). False when never turned on.
    // 读取时由核心从 `ext` 投影，不落库；核心内部读 `notify_all(conversation)`。
    pub notify_all: bool,
    pub is_archived: bool,

    // ===============================
    // Sync / Time
    // ===============================
    pub version: u64,
    pub updated_at: u64,
    pub created_at: u64,
    /// 更新时间戳（毫秒，用于排序/筛选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at_ts: Option<u64>,

    // ===============================
    // 扩展
    // ===============================
    /// 扩展键值（与 proto ext 对应）
    pub ext: HashMap<String, String>,
    /// 服务端成员读模型版本。完整成员通过独立 participants 同步拉取。
    pub participant_version: u64,
    /// 摘要级成员预览，最多少量成员，不能作为完整成员列表使用。
    pub member_preview: Vec<ConversationParticipant>,

    // ===============================
    // 草稿 / @ / 徽标 / 群角色
    // ===============================
    #[serde(skip_serializing_if = "Option::is_none")]
    pub draft: Option<String>,
    pub mention_count: u32,
    pub mention_me: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub badge: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// 已按需同步到本地的完整成员快照；会话摘要同步不会填充该字段。
    pub participants: Vec<ConversationParticipant>,

    // ===============================
    // Local State（SDK 内部，不序列化到 DB）
    // ===============================
    #[serde(skip)]
    pub local_state: ConversationLocalState,
}

impl Default for Conversation {
    fn default() -> Self {
        Self {
            conversation_id: String::new(),
            conversation_type: ConversationType::Unspecified,
            business_type: String::new(),
            channel_id: String::new(),
            members_count: 0,
            display_name: String::new(),
            title: DisplayName::default(),
            avatar_url: String::new(),
            remark: None,
            description: None,
            last_message_id: None,
            last_sender_id: None,
            last_message_at: None,
            last_message_preview: None,
            last_message: None,
            last_sender_nickname: String::new(),
            last_sender: None,
            last_sender_avatar_url: String::new(),
            unread_count: 0,
            last_read_seq: 0,
            peer_read_seq: 0,
            max_seq: 0,
            visible_after_seq: 0,
            is_pinned: false,
            is_muted: false,
            is_mention_only: None,
            notify_all: false,
            is_archived: false,
            version: 0,
            updated_at: 0,
            created_at: 0,
            updated_at_ts: None,
            ext: HashMap::new(),
            participant_version: 0,
            member_preview: Vec::new(),
            draft: None,
            mention_count: 0,
            mention_me: false,
            badge: None,
            role: None,
            participants: Vec::new(),
            local_state: ConversationLocalState::default(),
        }
    }
}

impl Conversation {
    /// 从 proto ConversationSummary 构造（获取到 proto 后应即转换）
    pub fn new(summary: flare_proto::common::ConversationSummary) -> Self {
        Self::from(summary)
    }

    /// 仅凭 conversation_id 构造（如同步事件仅带 id 时）
    pub fn from_conversation_id(conversation_id: String) -> Self {
        Self {
            conversation_id,
            ..Default::default()
        }
    }

    /// Ensure the wire-facing route id is non-empty before exposing a local
    /// conversation snapshot to platform SDKs.
    ///
    /// Authoritative summaries should always carry `channel_id`; this is a
    /// repair path for historical/local shell rows created before a full
    /// summary arrived. Keeping it here avoids weakening platform codecs while
    /// still letting session refresh render recoverable local state.
    pub fn normalize_channel_id_for_wire(&mut self) -> bool {
        if !self.channel_id.trim().is_empty() {
            return false;
        }
        let fallback = self.conversation_id.trim();
        if fallback.is_empty() {
            return false;
        }
        self.channel_id = fallback.to_string();
        true
    }

    /// 已读序列号（与 last_read_seq 同义，API 对齐）
    pub fn read_seq(&self) -> u64 {
        self.last_read_seq
    }

    /// 转为 proto ConversationSummary（持久化/回写服务端用）
    pub fn to_proto_summary(&self) -> flare_proto::common::ConversationSummary {
        let mut attributes = self.ext.clone();
        attributes.insert("peer_read_seq".to_string(), self.peer_read_seq.to_string());
        if !self.business_type.trim().is_empty() {
            attributes.insert("business_type".to_string(), self.business_type.clone());
        }
        flare_proto::common::ConversationSummary {
            conversation_id: self.conversation_id.clone(),
            conversation_type: self.conversation_type.as_str().to_string(),
            channel_id: self.channel_id.clone(),
            display_name: self.display_name.clone(),
            avatar_url: self.avatar_url.clone(),
            unread_count: self.unread_count,
            max_conversation_seq: self.max_seq,
            visible_after_conversation_seq: self.visible_after_seq,
            last_read_seq: self.last_read_seq,
            is_muted: self.is_muted,
            is_mention_only: self.is_mention_only,
            notify_all: crate::model::conversation_user_settings::notify_all_in(&self.ext),
            is_pinned: self.is_pinned,
            is_archived: self.is_archived,
            updated_at: u64_to_proto_ms(self.updated_at),
            created_at: u64_to_proto_ms(self.created_at),
            member_count: self.members_count as i32,
            participant_version: self.participant_version,
            member_preview: self
                .member_preview
                .clone()
                .into_iter()
                .map(Into::into)
                .collect(),
            draft: self.draft.clone().unwrap_or_default(),
            last_message: self.last_message.as_ref().map(message_preview_to_proto),
            attributes,
            ..Default::default()
        }
    }

    /// 填充最后一条发送者资料
    pub fn with_last_sender(
        mut self,
        display_name: impl Into<String>,
        avatar_url: impl Into<String>,
    ) -> Self {
        self.last_sender_nickname = display_name.into();
        self.last_sender_avatar_url = avatar_url.into();
        self
    }

    pub fn conversation_id(&self) -> &str {
        &self.conversation_id
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn avatar_url(&self) -> &str {
        &self.avatar_url
    }

    pub fn last_message(&self) -> Option<&MessagePreviewElem> {
        self.last_message.as_ref()
    }

    pub fn unread_count(&self) -> u32 {
        self.unread_count
    }

    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    pub fn last_read_seq(&self) -> u64 {
        self.last_read_seq
    }

    pub fn last_sender_avatar_url(&self) -> &str {
        &self.last_sender_avatar_url
    }

    pub fn draft(&self) -> Option<&str> {
        self.draft.as_deref()
    }

    pub fn mention_count(&self) -> u32 {
        self.mention_count
    }

    pub fn mention_me(&self) -> bool {
        self.mention_me
    }

    pub fn badge(&self) -> Option<&str> {
        self.badge.as_deref()
    }

    pub fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }
}

impl From<flare_proto::common::ConversationSummary> for Conversation {
    fn from(s: flare_proto::common::ConversationSummary) -> Self {
        let updated_at = proto_ms_to_u64(s.updated_at);
        let created_at = proto_ms_to_u64(s.created_at);
        let last_message = s.last_message.as_ref().map(message_preview_from_proto);
        let business_type = s
            .attributes
            .get("business_type")
            .cloned()
            .unwrap_or_default();
        let peer_read_seq = s
            .attributes
            .get("peer_read_seq")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or_default();
        let peer_read_seq = if peer_read_seq <= s.max_conversation_seq {
            peer_read_seq
        } else {
            0
        };
        let member_preview: Vec<ConversationParticipant> =
            s.member_preview.into_iter().map(Into::into).collect();
        let visible_after_seq = s.visible_after_conversation_seq;
        // 以服务端聚合后的 unread_count 为准；若服务端下发了用户历史可见边界，
        // SDK 仍在模型入口做一次硬约束，避免旧摘要/旧消息回灌成本地未读。
        let unread_count = if visible_after_seq > 0 && s.max_conversation_seq <= visible_after_seq {
            0
        } else {
            s.unread_count
        };
        let last_read_seq = s.last_read_seq.max(visible_after_seq);
        Self {
            conversation_id: s.conversation_id,
            conversation_type: ConversationType::from(s.conversation_type.as_str()),
            business_type,
            channel_id: s.channel_id,
            display_name: s.display_name,
            avatar_url: s.avatar_url,
            unread_count,
            max_seq: s.max_conversation_seq,
            visible_after_seq,
            last_read_seq,
            peer_read_seq,
            last_message_id: last_message.as_ref().and_then(|m| {
                if m.message_id.trim().is_empty() {
                    None
                } else {
                    Some(m.message_id.clone())
                }
            }),
            last_sender_id: last_message.as_ref().and_then(|m| {
                if m.sender_id.trim().is_empty() {
                    None
                } else {
                    Some(m.sender_id.clone())
                }
            }),
            last_message_at: last_message
                .as_ref()
                .and_then(|m| if m.time > 0 { Some(m.time) } else { None }),
            last_message_preview: last_message.as_ref().and_then(|m| {
                if m.text.trim().is_empty() {
                    None
                } else {
                    Some(m.text.clone())
                }
            }),
            last_message,
            updated_at,
            created_at,
            last_sender_nickname: String::new(),
            last_sender_avatar_url: String::new(),
            is_pinned: s.is_pinned,
            is_muted: s.is_muted,
            is_mention_only: s.is_mention_only,
            is_archived: s.is_archived,
            members_count: (s.member_count.max(0) as u32).max(member_preview.len() as u32),
            participant_version: s.participant_version,
            member_preview,
            participants: Vec::new(),
            draft: if s.draft.trim().is_empty() {
                None
            } else {
                Some(s.draft.clone())
            },
            mention_count: 0,
            mention_me: false,
            badge: None,
            role: None,
            ext: {
                let mut ext = s.attributes.clone();
                ext.insert("peer_read_seq".to_string(), peer_read_seq.to_string());
                crate::domain::set_local_cleared_through_seq(&mut ext, visible_after_seq);
                if s.user_settings_version > 0 {
                    ext.insert(
                        crate::model::EXT_USER_SETTINGS_VERSION.to_string(),
                        s.user_settings_version.to_string(),
                    );
                }
                // 老服务端不带这一项：不写，本地已有的记录在合并时保留。
                if let Some(notify_all) = s.notify_all {
                    crate::model::set_notify_all_in(&mut ext, notify_all);
                }
                ext.insert(
                    crate::model::EXT_SETTINGS_DIRTY.to_string(),
                    "0".to_string(),
                );
                ext
            },
            ..Default::default()
        }
    }
}

/// 仅边界使用：持久化/网络层解码得到 proto，应即转为 [Conversation]
pub use flare_proto::common::ConversationSummary;

#[cfg(test)]
mod tests {
    use super::Conversation;
    use flare_proto::common::{ConversationSummary, MessagePreview};

    #[test]
    fn summary_conversion_populates_persisted_last_message_fields() {
        let conversation = Conversation::from(ConversationSummary {
            conversation_id: "conv-1".to_string(),
            conversation_type: "single".to_string(),
            max_conversation_seq: 7,
            last_message: Some(MessagePreview {
                message_id: "msg-7".to_string(),
                sender_id: "u2".to_string(),
                text: "latest".to_string(),
                created_at: 12_345,
                ..Default::default()
            }),
            ..Default::default()
        });

        assert_eq!(conversation.last_message_id.as_deref(), Some("msg-7"));
        assert_eq!(conversation.last_sender_id.as_deref(), Some("u2"));
        assert_eq!(conversation.last_message_preview.as_deref(), Some("latest"));
        assert_eq!(conversation.last_message_at, Some(12_345));
    }

    #[test]
    fn summary_conversion_drops_impossible_peer_read_seq() {
        let mut summary = ConversationSummary {
            conversation_id: "conv-1".to_string(),
            conversation_type: "single".to_string(),
            max_conversation_seq: 7,
            ..Default::default()
        };
        summary
            .attributes
            .insert("peer_read_seq".to_string(), "999999".to_string());

        let conversation = Conversation::from(summary);

        assert_eq!(conversation.peer_read_seq, 0);
        assert_eq!(
            conversation.ext.get("peer_read_seq").map(String::as_str),
            Some("0")
        );
    }

    #[test]
    fn conversation_serializes_canonical_camel_case_identity() {
        let conversation = Conversation::from_conversation_id("1ATESTCIDVALUE001".to_string());
        let value = serde_json::to_value(&conversation).expect("serialize conversation");

        assert_eq!(value["conversationId"], "1ATESTCIDVALUE001");
        assert!(value.get("conversation_id").is_none());
    }

    #[test]
    fn conversation_repairs_blank_channel_id_for_wire_snapshots() {
        let mut conversation = Conversation::from_conversation_id("2AGROUPCIDVALUE01".to_string());

        assert!(conversation.normalize_channel_id_for_wire());
        assert_eq!(conversation.channel_id, "2AGROUPCIDVALUE01");

        let value = serde_json::to_value(&conversation).expect("serialize conversation");
        assert_eq!(value["channelId"], "2AGROUPCIDVALUE01");
    }

    #[test]
    fn conversation_serializes_absent_optional_fields_as_missing() {
        let conversation = Conversation::from_conversation_id("1ATESTCIDVALUE001".to_string());
        let value = serde_json::to_value(&conversation).expect("serialize conversation");

        for key in [
            "remark",
            "description",
            "lastMessageId",
            "lastSenderId",
            "lastMessageAt",
            "lastMessagePreview",
            "lastMessage",
            "updatedAtTs",
            "draft",
            "badge",
            "role",
        ] {
            assert!(
                value.get(key).is_none(),
                "{key} must be omitted when absent"
            );
        }
    }

    /// `title` is on every conversation and `name` on every member the SDK hands over;
    /// `lastSender` only when there is a latest message.
    #[test]
    fn conversation_wire_always_carries_the_title_and_member_names() {
        use super::ConversationParticipant;

        let mut conversation = Conversation::from_conversation_id("1ATESTCIDVALUE001".to_string());
        conversation.member_preview = vec![ConversationParticipant::from(
            flare_proto::common::ConversationParticipant {
                user_id: "u_ann".to_string(),
                attributes: [("nickname".to_string(), "Ann".to_string())].into(),
                ..Default::default()
            },
        )];
        let value = serde_json::to_value(&conversation).expect("serialize conversation");

        assert_eq!(
            value["title"],
            serde_json::json!({"kind": "unnamed", "entity": "person", "idTail": ""})
        );
        assert!(value.get("lastSender").is_none());
        assert_eq!(
            value["memberPreview"][0]["name"],
            serde_json::json!({"kind": "named", "text": "Ann", "source": "memberNickname"})
        );
    }

    #[test]
    fn conversation_type_from_str_accepts_only_wire_names() {
        use super::ConversationType;

        assert_eq!(ConversationType::from("single"), ConversationType::Single);
        assert_eq!(ConversationType::from("channel"), ConversationType::Channel);
        assert_eq!(ConversationType::from("1"), ConversationType::Unspecified);
        assert_eq!(
            ConversationType::from("conversation_type_single"),
            ConversationType::Unspecified
        );
    }

    #[test]
    fn fanout_policy_keeps_small_groups_on_write_path() {
        use super::{ConversationFanoutMode, ConversationType};

        assert_eq!(
            ConversationType::Group.fanout_mode_with_threshold(999, 1000),
            ConversationFanoutMode::FanOutOnWrite
        );
        assert_eq!(
            ConversationType::Group.fanout_mode_with_threshold(1000, 1000),
            ConversationFanoutMode::FanOutOnRead
        );
    }

    #[test]
    fn channel_and_broadcast_use_shared_read_timeline() {
        use super::{ConversationFanoutMode, ConversationType};

        for conversation_type in [ConversationType::Channel, ConversationType::Broadcast] {
            let policy = conversation_type.timeline_policy(0, None);

            assert_eq!(policy.fanout_mode, ConversationFanoutMode::FanOutOnRead);
            assert!(policy.shared_timeline);
            assert!(policy.attention_scoped_subscription);
            assert!(policy.member_presence_aggregation);
            assert!(!policy.materialize_member_table);
        }
    }
}
