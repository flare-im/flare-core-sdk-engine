//! 消息动作可用性：一条消息此刻能做什么。
//!
//! "谁能撤回""什么能编辑""撤回后还能不能复制"——这些是**产品中立的规则**，
//! 同一条消息在任何端都必须给出同一个答案，所以只能有一份，放在核心
//! （flare-im-spec 约束 4）。
//!
//! 此前这份规则散在各端：iOS 有完整判定（`MessageMenuModel`），
//! Android 的菜单则是一个**无条件全显的静态列表** —— `Pin` 与 `Unpin` 同时出现、
//! 别人的消息上也显示 `Recall`、图片上也显示 `Edit`。用户点了必然失败。
//!
//! 规则取自 iOS 那份（最完整、且已在真机上跑过）。

use crate::model::IMMessage;

/// 判定所需的上下文——消息本身给不出的那些。
#[derive(Debug, Clone, Default)]
pub struct MessageActionContext {
    pub current_user_id: String,
    pub is_connected: bool,
    /// 处于多选模式：单条动作（回复/编辑/撤回）此时应让位。
    pub multi_select_mode: bool,
    /// 本地待发（尚未拿到服务端回执）。
    pub is_pending: bool,
    /// 发送失败。
    pub is_failed: bool,
    pub is_pinned: bool,
    /// 插件卡片（`flare.plugin.message.v1`）声明了 `forwardable`。插件卡片默认绑定在所在会话里，
    /// 不能转发；只有当前目录里它的 message-content 贡献声明可转发时，调用方才置 true。
    pub plugin_forwardable: bool,
}

/// 一条消息此刻可用的动作。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageActionAvailability {
    pub can_reply: bool,
    pub can_forward: bool,
    pub can_copy: bool,
    pub can_edit: bool,
    pub can_delete: bool,
    pub can_recall: bool,
    pub can_pin: bool,
    pub can_unpin: bool,
    pub can_react: bool,
    pub can_multi_select: bool,
    pub can_save: bool,
    pub can_resend: bool,
}

const STATUS_FAILED: i32 = 4;
const STATUS_RECALLED: i32 = 5;
const STATUS_DELETED: i32 = 6;

const TYPE_TEXT: i32 = 1;
const TYPE_RICH_TEXT: i32 = 15;
const MEDIA_TYPES: [i32; 5] = [
    2,  // IMAGE
    3,  // VIDEO
    4,  // AUDIO
    5,  // FILE
    16, // IMAGE_GROUP
];

/// 一条消息本身能不能被转发：撤回或删除的不能；插件卡片（`flare.plugin.message.v1`）默认绑定在所在会话里，
/// 只有当前目录里它的 message-content 贡献声明了 `forwardable`（`plugin_forwardable`）才能。
///
/// 菜单的「转发」（[`message_action_availability`] 的 `can_forward`，另看待发）与构建转发记录
/// （`message_builder.create_forward` 拒收不能转发的源消息）读的是同一条规则。
pub fn message_forwardable(message: &IMMessage, plugin_forwardable: bool) -> bool {
    let active = message.status != STATUS_RECALLED && message.status != STATUS_DELETED;
    active && (plugin_forwardable || message.plugin_message().is_none())
}

/// 计算一条消息此刻可用的动作。
pub fn message_action_availability(
    message: &IMMessage,
    ctx: &MessageActionContext,
) -> MessageActionAvailability {
    let recalled = message.status == STATUS_RECALLED;
    let deleted = message.status == STATUS_DELETED;
    // 「活着」= 既没撤回也没删除。撤回/删除是终态，绝大多数动作到此为止。
    let active = !recalled && !deleted;

    let is_failed = ctx.is_failed || message.status == STATUS_FAILED;
    let self_sent = !ctx.current_user_id.is_empty() && message.sender_id == ctx.current_user_id;
    let editable_type = message.message_type == TYPE_TEXT || message.message_type == TYPE_RICH_TEXT;
    let media_type = MEDIA_TYPES.contains(&message.message_type);
    let single = !ctx.multi_select_mode;

    MessageActionAvailability {
        can_reply: single && active,
        can_forward: !ctx.is_pending && message_forwardable(message, ctx.plugin_forwardable),
        // 复制看的是"有没有可复制的正文"，而不是消息类型：
        // 一条没有文字的图片消息，复制什么都不会发生，不该给这个入口。
        can_copy: active
            && message
                .text_for_storage()
                .is_some_and(|text| !text.trim().is_empty()),
        can_edit: single && self_sent && active && !ctx.is_pending && !is_failed && editable_type,
        can_delete: active,
        can_recall: single && self_sent && active && !is_failed,
        can_pin: active && !ctx.is_pending && !ctx.is_pinned,
        can_unpin: active && !ctx.is_pending && ctx.is_pinned,
        can_react: active && !ctx.is_pending,
        can_multi_select: active,
        can_save: media_type && active && !ctx.is_pending,
        // 只有自己发的、确实失败的、且此刻连着的，才谈得上重发。
        can_resend: is_failed && self_sent && ctx.is_connected,
    }
}

/// 导出跨端一致性向量：把这条规则在代表性输入下的结果写成 JSON，
/// 各端用例据此断言自己的实现与核心逐位一致。
///
/// 为什么不是"各端直接调核心"：kit 的组件是纯展示的（不碰 SDK），
/// 把异步核心调用塞进展示组件会破坏分层。所以这里退一步——
/// 允许多份实现，但用生成的向量把它们钉在一起，漂移即红。
#[cfg(any(test, feature = "integration-tests"))]
pub fn action_availability_vectors() -> serde_json::Value {
    use serde_json::json;
    let mut cases = Vec::new();
    // 覆盖：自己/别人 × 文本/图片/富文本 × 正常/撤回/删除/失败 × pending/pinned/断线/多选
    for (label, sender, mtype, status, has_text, pending, pinned, connected, multi, failed) in [
        (
            "自己的文本",
            "me",
            TYPE_TEXT,
            0i32,
            true,
            false,
            false,
            true,
            false,
            false,
        ),
        (
            "别人的文本",
            "other",
            TYPE_TEXT,
            0,
            true,
            false,
            false,
            true,
            false,
            false,
        ),
        (
            "自己的图片(无正文)",
            "me",
            MEDIA_TYPES[0],
            0,
            false,
            false,
            false,
            true,
            false,
            false,
        ),
        (
            "自己的富文本",
            "me",
            TYPE_RICH_TEXT,
            0,
            true,
            false,
            false,
            true,
            false,
            false,
        ),
        (
            "已撤回",
            "me",
            TYPE_TEXT,
            STATUS_RECALLED,
            true,
            false,
            false,
            true,
            false,
            false,
        ),
        (
            "已删除",
            "me",
            TYPE_TEXT,
            STATUS_DELETED,
            true,
            false,
            false,
            true,
            false,
            false,
        ),
        (
            "发送失败",
            "me",
            TYPE_TEXT,
            STATUS_FAILED,
            true,
            false,
            false,
            true,
            false,
            true,
        ),
        (
            "发送失败且断线",
            "me",
            TYPE_TEXT,
            STATUS_FAILED,
            true,
            false,
            false,
            false,
            false,
            true,
        ),
        (
            "发送中(pending)",
            "me",
            TYPE_TEXT,
            0,
            true,
            true,
            false,
            true,
            false,
            false,
        ),
        (
            "已置顶",
            "me",
            TYPE_TEXT,
            0,
            true,
            false,
            true,
            true,
            false,
            false,
        ),
        (
            "对方已读",
            "me",
            TYPE_TEXT,
            0,
            true,
            false,
            false,
            true,
            false,
            false,
        ),
        (
            "别人发的",
            "other",
            TYPE_TEXT,
            0,
            true,
            false,
            false,
            true,
            false,
            false,
        ),
        (
            "多选模式",
            "me",
            TYPE_TEXT,
            0,
            true,
            false,
            false,
            true,
            true,
            false,
        ),
    ] {
        let mut m = IMMessage::new(flare_proto::common::Message {
            server_id: "m1".to_string(),
            sender_id: sender.to_string(),
            message_type: mtype,
            status,
            ..Default::default()
        });
        m.message_type = mtype;
        m.status = status;
        m.sender_id = sender.to_string();
        // 复制的判据是「有没有可复制的正文」，所以正文必须走真实内容而不是补个字段。
        // 已读维度：label 带「已读」的用例把 is_read 打开，用来钉双勾。
        m.is_read = label.contains("已读");
        m.content = if has_text {
            Some(crate::model::Elem::Text(
                crate::content::message_elem::TextElem {
                    text: "正文".to_string(),
                    mentions: Vec::new(),
                },
            ))
        } else {
            None
        };
        let ctx = MessageActionContext {
            current_user_id: "me".to_string(),
            is_connected: connected,
            is_pending: pending,
            is_pinned: pinned,
            is_failed: failed,
            multi_select_mode: multi,
            plugin_forwardable: false,
        };
        let a = message_action_availability(&m, &ctx);
        let delivery = message_delivery_state(&m, &ctx);
        cases.push(json!({
            "label": label,
            "input": {
                "isSelf": sender == "me", "isRead": m.is_read,
                "messageType": mtype, "status": status,
                "hasText": has_text, "isPending": pending, "isPinned": pinned,
                "isConnected": connected, "multiSelectMode": multi, "isFailed": failed,
            },
            "expected": {
                "canReply": a.can_reply, "canForward": a.can_forward, "canCopy": a.can_copy,
                "canEdit": a.can_edit, "canDelete": a.can_delete, "canRecall": a.can_recall,
                "canPin": a.can_pin, "canUnpin": a.can_unpin, "canReact": a.can_react,
                "canMultiSelect": a.can_multi_select, "canSave": a.can_save, "canResend": a.can_resend,
            },
            "deliveryState": delivery.as_str()
        }));
    }
    json!({ "note": "由 flare-im-core-sdk domain::message_actions 生成，勿手改", "cases": cases })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(sender: &str, message_type: i32, status: i32) -> IMMessage {
        let mut m = IMMessage::new(flare_proto::common::Message {
            server_id: "m1".to_string(),
            sender_id: sender.to_string(),
            message_type,
            status,
            ..Default::default()
        });
        m.message_type = message_type;
        m.status = status;
        m.sender_id = sender.to_string();
        m
    }

    fn ctx() -> MessageActionContext {
        MessageActionContext {
            current_user_id: "me".to_string(),
            is_connected: true,
            ..Default::default()
        }
    }

    #[test]
    fn pin_and_unpin_are_never_offered_together() {
        // Android 的静态菜单同时常显 Pin 与 Unpin —— 其中一个必然是错的。
        let m = message("me", TYPE_TEXT, 3);
        let unpinned = message_action_availability(&m, &ctx());
        assert!(unpinned.can_pin && !unpinned.can_unpin);

        let pinned_ctx = MessageActionContext {
            is_pinned: true,
            ..ctx()
        };
        let pinned = message_action_availability(&m, &pinned_ctx);
        assert!(!pinned.can_pin && pinned.can_unpin);
    }

    #[test]
    fn only_own_messages_can_be_recalled_or_edited() {
        let mine = message_action_availability(&message("me", TYPE_TEXT, 3), &ctx());
        assert!(mine.can_recall && mine.can_edit);

        let theirs = message_action_availability(&message("other", TYPE_TEXT, 3), &ctx());
        assert!(
            !theirs.can_recall && !theirs.can_edit,
            "别人的消息不能撤回/编辑——点了必然失败"
        );
    }

    #[test]
    fn only_text_like_messages_are_editable() {
        for t in [TYPE_TEXT, TYPE_RICH_TEXT] {
            assert!(message_action_availability(&message("me", t, 3), &ctx()).can_edit);
        }
        for t in MEDIA_TYPES {
            assert!(
                !message_action_availability(&message("me", t, 3), &ctx()).can_edit,
                "类型 {t} 不该显示编辑"
            );
        }
    }

    #[test]
    fn recalled_and_deleted_messages_are_terminal() {
        for status in [STATUS_RECALLED, STATUS_DELETED] {
            let a = message_action_availability(&message("me", TYPE_TEXT, status), &ctx());
            assert_eq!(
                a,
                MessageActionAvailability::default(),
                "status={status} 是终态，不该再有任何可用动作"
            );
        }
    }

    #[test]
    fn media_can_be_saved_but_text_cannot() {
        assert!(message_action_availability(&message("me", 2, 3), &ctx()).can_save);
        assert!(!message_action_availability(&message("me", TYPE_TEXT, 3), &ctx()).can_save);
    }

    #[test]
    fn resend_requires_own_failed_message_and_a_connection() {
        let failed = message("me", TYPE_TEXT, STATUS_FAILED);
        assert!(message_action_availability(&failed, &ctx()).can_resend);

        let offline = MessageActionContext {
            is_connected: false,
            ..ctx()
        };
        assert!(
            !message_action_availability(&failed, &offline).can_resend,
            "断线时重发只会再失败一次"
        );
        assert!(
            !message_action_availability(&message("other", TYPE_TEXT, STATUS_FAILED), &ctx())
                .can_resend,
            "别人的失败消息轮不到我重发"
        );
    }

    #[test]
    fn pending_message_blocks_actions_that_need_a_server_id() {
        let pending = MessageActionContext {
            is_pending: true,
            ..ctx()
        };
        let a = message_action_availability(&message("me", TYPE_TEXT, 1), &pending);
        assert!(!a.can_react && !a.can_pin && !a.can_forward && !a.can_edit);
        assert!(a.can_delete, "本地待发的消息仍应可以删掉");
    }

    #[test]
    fn multi_select_mode_hides_single_message_actions() {
        let multi = MessageActionContext {
            multi_select_mode: true,
            ..ctx()
        };
        let a = message_action_availability(&message("me", TYPE_TEXT, 3), &multi);
        assert!(!a.can_reply && !a.can_edit && !a.can_recall);
        assert!(a.can_forward, "多选下转发仍然成立（批量转发）");
    }

    #[test]
    fn plugin_cards_forward_only_when_their_contribution_allows_it() {
        let envelope = serde_json::json!({
            "pluginId": "example.poll", "contributionId": "example.poll.card",
            "schemaVersion": 1, "summary": "Poll", "payload": {"pollId": "poll-1"}
        });
        let mut card = message("other", 21, 3);
        card.content = Some(crate::model::Elem::Custom(
            crate::content::message_elem::CustomElem {
                r#type: "flare.plugin.message.v1".into(),
                payload: serde_json::to_vec(&envelope).unwrap(),
                description: String::new(),
                metadata: Default::default(),
            },
        ));
        let hidden = message_action_availability(&card, &ctx());
        assert!(
            !hidden.can_forward,
            "plugin cards stay in their conversation"
        );
        assert!(
            hidden.can_reply && hidden.can_react,
            "only forward is affected"
        );
        let allowed = MessageActionContext {
            plugin_forwardable: true,
            ..ctx()
        };
        assert!(message_action_availability(&card, &allowed).can_forward);
        assert!(
            message_action_availability(&message("other", TYPE_TEXT, 3), &ctx()).can_forward,
            "other messages are unaffected"
        );
    }

    /// The forward builder reads the rule the menu reads: dead content and plugin cards that do not
    /// declare `forwardable` cannot be forwarded; pending is the menu's own fact.
    #[test]
    fn message_forwardable_is_the_rule_behind_can_forward() {
        let text = message("other", TYPE_TEXT, 3);
        assert!(message_forwardable(&text, false));
        assert!(!message_forwardable(
            &message("other", TYPE_TEXT, STATUS_RECALLED),
            false
        ));
        assert!(!message_forwardable(
            &message("other", TYPE_TEXT, STATUS_DELETED),
            true
        ));
        assert!(message_forwardable(
            &message("me", TYPE_TEXT, STATUS_FAILED),
            false
        ));
        let mut card = message("other", 21, 3);
        card.content = Some(crate::model::Elem::Custom(
            crate::content::message_elem::CustomElem {
                r#type: "flare.plugin.message.v1".into(),
                payload: serde_json::to_vec(&serde_json::json!({
                    "pluginId": "flare.redpacket", "contributionId": "flare.redpacket.card",
                    "schemaVersion": 1, "summary": "恭喜发财", "payload": {"packetId": "p1"}
                }))
                .unwrap(),
                description: String::new(),
                metadata: Default::default(),
            },
        ));
        assert!(!message_forwardable(&card, false));
        assert!(message_forwardable(&card, true));
        let pending = MessageActionContext {
            is_pending: true,
            ..ctx()
        };
        assert!(!message_action_availability(&text, &pending).can_forward);
    }

    /// 把向量写到仓库，供各端一致性用例消费。
    ///
    /// 判据是生成物与已提交内容一致：规则改了而没重新生成，这条就红，
    /// 提醒你去看各端是否也要跟着改。
    #[test]
    fn action_availability_vectors_are_committed_and_current() {
        let generated = serde_json::to_string_pretty(&super::action_availability_vectors())
            .expect("序列化向量");
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../flare-core-sdk/sdk-spec/message-action-vectors.json");
        let existing = std::fs::read_to_string(&path).unwrap_or_default();
        if existing.trim() != generated.trim() {
            std::fs::create_dir_all(path.parent().expect("父目录")).ok();
            std::fs::write(&path, format!("{generated}\n")).expect("写入向量");
            panic!(
                "跨端动作可用性向量已更新（{}）——请一并检查各端实现是否需要跟着改，然后重跑",
                path.display()
            );
        }
    }
}
/// 一条**自己发出的**消息该显示什么送达状态。
///
/// 与动作可用性同理：这是产品中立的展示规则，四端必须一致，
/// 所以真源在核心，各端按 `sdk-spec/message-action-vectors.json` 对齐。
///
/// 视觉约定（与 iOS `DeliveryStatusGlyph`、kit `messageStateToNumber` 一致）：
/// - `sending` 转圈；`failed` 叹号；`delivered` 单勾；`read` 双勾；`none` 不显示。
/// - proto 的 SENT(2) 与 PERSISTED(3) **都是单勾**：对用户而言"服务端收下了"
///   和"已落库"没有区别，不值得用两个符号去区分。
/// - 撤回/删除是终态，由占位气泡接管展示，不再显示送达状态。
/// - 对方发来的消息不显示任何送达状态（那是发送方才关心的事）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageDeliveryState {
    None,
    Sending,
    Failed,
    Delivered,
    Read,
}

impl MessageDeliveryState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Sending => "sending",
            Self::Failed => "failed",
            Self::Delivered => "delivered",
            Self::Read => "read",
        }
    }
}

pub fn message_delivery_state(
    message: &IMMessage,
    ctx: &MessageActionContext,
) -> MessageDeliveryState {
    let self_sent = !ctx.current_user_id.is_empty() && message.sender_id == ctx.current_user_id;
    if !self_sent {
        return MessageDeliveryState::None;
    }
    if message.status == STATUS_RECALLED || message.status == STATUS_DELETED {
        return MessageDeliveryState::None;
    }
    if ctx.is_failed || message.status == STATUS_FAILED {
        return MessageDeliveryState::Failed;
    }
    if ctx.is_pending {
        return MessageDeliveryState::Sending;
    }
    if message.is_read {
        return MessageDeliveryState::Read;
    }
    MessageDeliveryState::Delivered
}
