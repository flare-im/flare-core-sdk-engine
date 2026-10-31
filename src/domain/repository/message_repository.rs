use async_trait::async_trait;
use flare_proto::common::{MessageRetentionPolicy, MessageRetentionState};
use std::collections::HashMap;

use flare_plugin_protocol::state::{SharedState, SharedStateOrder};
use flare_plugin_protocol::ui::PluginMessageState;

use crate::model::message::{
    PLUGIN_SHARED_ENTITY_KEY, PLUGIN_SHARED_STATE_JSON_KEY, PLUGIN_SHARED_STATE_VERSION_KEY,
    PLUGIN_STATE_BASED_ON_KEY, PLUGIN_STATE_JSON_KEY, PLUGIN_STATE_VERSION_KEY, ReactionEntry,
};
use crate::model::{IMMessage, MessageSearchQuery};
use crate::shared::error::{ErrorCode, FlareError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditApplyResult {
    Applied,
    IgnoredStale,
    NotFound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationApplyResult {
    Applied,
    IgnoredStale,
    NotFound,
}

pub(crate) fn message_attribute_seq(attributes: &HashMap<String, String>, key: &str) -> u64 {
    attributes
        .get(key)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
}

/// 插件卡片状态只由 `apply_plugin_message_state` 写入：同步、编辑、回执等整行覆盖时保留
/// 版本更大（相同则保留已存）的那一份。共享层一并按版本带过去（见 [`carry_plugin_shared_state`]）。
pub(crate) fn carry_plugin_message_state(
    incoming: &mut HashMap<String, String>,
    existing: &HashMap<String, String>,
) {
    carry_plugin_shared_state(incoming, existing);
    let Some(values) = existing.get(PLUGIN_STATE_JSON_KEY) else {
        return;
    };
    let existing_version = message_attribute_seq(existing, PLUGIN_STATE_VERSION_KEY);
    if incoming.contains_key(PLUGIN_STATE_JSON_KEY)
        && message_attribute_seq(incoming, PLUGIN_STATE_VERSION_KEY) > existing_version
    {
        return;
    }
    incoming.insert(PLUGIN_STATE_JSON_KEY.to_string(), values.clone());
    incoming.insert(
        PLUGIN_STATE_VERSION_KEY.to_string(),
        existing_version.to_string(),
    );
    match existing.get(PLUGIN_STATE_BASED_ON_KEY) {
        Some(based_on) => incoming.insert(PLUGIN_STATE_BASED_ON_KEY.to_string(), based_on.clone()),
        None => incoming.remove(PLUGIN_STATE_BASED_ON_KEY),
    };
}

/// 序列化一份待写入的插件卡片状态；不满足协议边界时拒绝。
pub(crate) fn encode_plugin_message_state(state: &PluginMessageState) -> Result<String> {
    let invalid = || {
        FlareError::localized(
            ErrorCode::InvalidParameter,
            "sdk.plugin.invalid_message_state",
        )
    };
    state.validate().map_err(|_| invalid())?;
    serde_json::to_string(&state.values).map_err(|_| invalid())
}

/// 按版本单调写入：更旧的版本返回 `false`，相同版本以后到者为准。`basedOnVersion` 随这份值一起
/// 写入（没有时删掉旧的），它描述的是这份值。
pub(crate) fn write_plugin_message_state(
    attributes: &mut HashMap<String, String>,
    state: &PluginMessageState,
    values_json: &str,
) -> bool {
    if attributes.contains_key(PLUGIN_STATE_JSON_KEY)
        && state.version < message_attribute_seq(attributes, PLUGIN_STATE_VERSION_KEY)
    {
        return false;
    }
    attributes.insert(PLUGIN_STATE_JSON_KEY.to_string(), values_json.to_string());
    attributes.insert(
        PLUGIN_STATE_VERSION_KEY.to_string(),
        state.version.to_string(),
    );
    match state.based_on_version {
        Some(based_on) => {
            attributes.insert(PLUGIN_STATE_BASED_ON_KEY.to_string(), based_on.to_string())
        }
        None => attributes.remove(PLUGIN_STATE_BASED_ON_KEY),
    };
    true
}

/// 共享层的已存版本：值与版本都在、版本合法时才算有。
fn plugin_shared_state_version(attributes: &HashMap<String, String>) -> Option<u64> {
    attributes.get(PLUGIN_SHARED_STATE_JSON_KEY)?;
    attributes
        .get(PLUGIN_SHARED_STATE_VERSION_KEY)?
        .parse()
        .ok()
        .filter(|version| *version > 0)
}

/// 整行覆盖（同步、ACK、编辑、回执）时的共享层：保留版本更大的一份，相同版本保留已存的。
/// 共享层可以随 wire 到达（Core 在历史与同步的 extra 上带最新快照），所以要比较两边；
/// 实体 id 对一条消息不变，incoming 没带时沿用已存的。
pub(crate) fn carry_plugin_shared_state(
    incoming: &mut HashMap<String, String>,
    existing: &HashMap<String, String>,
) {
    if !incoming.contains_key(PLUGIN_SHARED_ENTITY_KEY)
        && let Some(entity) = existing.get(PLUGIN_SHARED_ENTITY_KEY)
    {
        incoming.insert(PLUGIN_SHARED_ENTITY_KEY.to_string(), entity.clone());
    }
    let Some(existing_version) = plugin_shared_state_version(existing) else {
        return;
    };
    if plugin_shared_state_version(incoming).is_some_and(|incoming| incoming > existing_version) {
        return;
    }
    for key in [
        PLUGIN_SHARED_STATE_JSON_KEY,
        PLUGIN_SHARED_STATE_VERSION_KEY,
    ] {
        if let Some(value) = existing.get(key) {
            incoming.insert(key.to_string(), value.clone());
        }
    }
}

/// 按版本单调写入共享层：只有更新的版本落盘（[`SharedStateOrder::Newer`]），相同版本是重复、
/// 更旧的是乱序，都不改动。调用方先校验 `state`。
pub(crate) fn write_plugin_shared_state(
    attributes: &mut HashMap<String, String>,
    state: &SharedState,
) -> SharedStateOrder {
    let order = state.order_against(plugin_shared_state_version(attributes));
    if order == SharedStateOrder::Newer {
        attributes.insert(
            PLUGIN_SHARED_STATE_JSON_KEY.to_string(),
            state.values_json(),
        );
        attributes.insert(
            PLUGIN_SHARED_STATE_VERSION_KEY.to_string(),
            state.version.to_string(),
        );
        attributes.insert(PLUGIN_SHARED_ENTITY_KEY.to_string(), state.entity.clone());
    }
    order
}

/// 校验一份待写入的共享层；不满足协议边界时拒绝。
pub(crate) fn validate_plugin_shared_state(state: &SharedState) -> Result<()> {
    state.validate().map_err(|_| {
        FlareError::localized(
            ErrorCode::InvalidParameter,
            "sdk.plugin.invalid_shared_state",
        )
    })
}

fn preserve_attribute_if_missing(
    incoming: &mut HashMap<String, String>,
    existing: &HashMap<String, String>,
    key: &str,
) {
    if incoming.contains_key(key) {
        return;
    }
    if let Some(value) = existing.get(key) {
        incoming.insert(key.to_string(), value.clone());
    }
}

pub(crate) fn merge_message_event_attributes(
    mut incoming: HashMap<String, String>,
    existing: HashMap<String, String>,
) -> HashMap<String, String> {
    let existing_pin_seq = message_attribute_seq(&existing, "lastPinEventSeq");
    let incoming_pin_seq = message_attribute_seq(&incoming, "lastPinEventSeq");
    if existing_pin_seq > incoming_pin_seq {
        if let Some(pinned) = existing.get("pinned") {
            incoming.insert("pinned".to_string(), pinned.clone());
        } else {
            incoming.remove("pinned");
        }
        incoming.insert("lastPinEventSeq".to_string(), existing_pin_seq.to_string());
    } else {
        preserve_attribute_if_missing(&mut incoming, &existing, "pinned");
        preserve_attribute_if_missing(&mut incoming, &existing, "lastPinEventSeq");
    }

    let existing_mark_seq_keys = existing
        .keys()
        .filter(|key| key.starts_with("lastMarkEventSeq:"))
        .cloned()
        .collect::<Vec<_>>();
    for seq_key in existing_mark_seq_keys {
        let existing_seq = message_attribute_seq(&existing, &seq_key);
        let incoming_seq = message_attribute_seq(&incoming, &seq_key);
        if existing_seq > incoming_seq {
            incoming.insert(seq_key.clone(), existing_seq.to_string());
            if let Some(mark_type) = existing.get("markType") {
                incoming.insert("markType".to_string(), mark_type.clone());
            } else {
                incoming.remove("markType");
            }
            if let Some(mark_color) = existing.get("markColor") {
                incoming.insert("markColor".to_string(), mark_color.clone());
            } else {
                incoming.remove("markColor");
            }
        } else {
            preserve_attribute_if_missing(&mut incoming, &existing, &seq_key);
        }
    }
    preserve_attribute_if_missing(&mut incoming, &existing, "markType");
    preserve_attribute_if_missing(&mut incoming, &existing, "markColor");

    preserve_attribute_if_missing(&mut incoming, &existing, "reactionsJson");
    for seq_key in existing
        .keys()
        .filter(|key| key.starts_with("lastReactionEventSeq:"))
        .cloned()
        .collect::<Vec<_>>()
    {
        preserve_attribute_if_missing(&mut incoming, &existing, &seq_key);
    }

    for seq_key in [
        "lastRetentionScheduledEventSeq",
        "lastRetentionExpiredEventSeq",
        "lastRetentionPurgedEventSeq",
    ] {
        preserve_attribute_if_missing(&mut incoming, &existing, seq_key);
    }
    preserve_attribute_if_missing(&mut incoming, &existing, "retention_event");
    preserve_attribute_if_missing(&mut incoming, &existing, "retention_expired_at");
    preserve_attribute_if_missing(&mut incoming, &existing, "retention_purged_at");

    carry_plugin_message_state(&mut incoming, &existing);

    incoming
}

/// 消息查询（只读）
#[async_trait]
pub trait MessageReader: Send + Sync {
    async fn get(&self, message_id: &str) -> Result<Option<IMMessage>>;
    /// 批量按 server_id 查询。默认逐条兜底；SQLite 实现覆盖为 `IN (...)` 单次查询，
    /// 降低大批量下行收敛时的 O(n) DB 往返。
    async fn get_by_message_ids(&self, message_ids: &[String]) -> Result<Vec<IMMessage>> {
        let mut out = Vec::with_capacity(message_ids.len());
        for id in message_ids {
            if let Some(message) = self.get(id).await? {
                out.push(message);
            }
        }
        Ok(out)
    }
    /// 按 client_msg_id 查询（发送中/待 ACK 时可能仅有 client_msg_id）
    async fn get_by_client_msg_id(&self, client_msg_id: &str) -> Result<Option<IMMessage>>;
    /// 批量按 client_msg_id 查询。默认逐条兜底；SQLite 实现覆盖为 `IN (...)` 单次查询，
    /// 降低可靠队列对账每 tick 的 O(n) DB 往返与连接池争用。
    async fn get_by_client_msg_ids(&self, client_msg_ids: &[String]) -> Result<Vec<IMMessage>> {
        let mut out = Vec::with_capacity(client_msg_ids.len());
        for id in client_msg_ids {
            if let Some(message) = self.get_by_client_msg_id(id).await? {
                out.push(message);
            }
        }
        Ok(out)
    }
    /// `before_seq == 0`：首屏，返回该会话最新 `limit` 条（`seq` 降序）。
    /// `before_seq > 0`：返回满足 `seq < before_seq` 的更早消息。
    async fn get_by_conversation(
        &self,
        conversation_id: &str,
        before_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>>;
    /// 往新的方向读：`seq > after_seq` 的已分配序列号的消息，按 seq **升序**最多 `limit` 条
    /// （时间线视图往新翻页、围绕某条消息打开时用）。与 [`Self::get_by_conversation`] 的翻页读法
    /// 一样不含待发送行、遵守本地清空水位。
    ///
    /// 默认实现借 [`Self::get_by_conversation`] 按 seq 跨度分段往上读，跨度逐段翻倍；
    /// SQLite 与内存实现覆盖为单次查询。
    async fn get_by_conversation_after(
        &self,
        conversation_id: &str,
        after_seq: u64,
        limit: u32,
    ) -> Result<Vec<IMMessage>> {
        let limit = limit as usize;
        let mut out: Vec<IMMessage> = Vec::with_capacity(limit);
        if limit == 0 {
            return Ok(out);
        }
        // 翻页读法（不是最新窗口）取最新一条已分配序列号的行，得到往上读的终点。
        let paging_top = (i64::MAX as u64) - 1;
        let Some(newest) = self
            .get_by_conversation(conversation_id, paging_top, 1)
            .await?
            .first()
            .map(|message| message.conversation_seq)
        else {
            return Ok(out);
        };
        let mut low = after_seq;
        let mut span = limit as u64;
        while out.len() < limit && low < newest {
            let high = low.saturating_add(span).min(newest);
            // (low, high] 里每个 seq 至多一行，所以读 `high - low` 行就能取全这一段。
            let rows = self
                .get_by_conversation(
                    conversation_id,
                    high.saturating_add(1),
                    u32::try_from(high - low).unwrap_or(u32::MAX),
                )
                .await?;
            let mut chunk = rows
                .into_iter()
                .filter(|message| {
                    message.conversation_seq > low && message.conversation_seq <= high
                })
                .collect::<Vec<_>>();
            chunk.sort_by_key(|message| message.conversation_seq);
            out.extend(chunk);
            low = high;
            span = span.saturating_mul(2);
        }
        out.truncate(limit);
        Ok(out)
    }
    /// 本地已落库的最早服务端会话序列号；忽略 `conversation_seq == 0` 的本地待发送消息。
    async fn oldest_conversation_seq(&self, conversation_id: &str) -> Result<Option<u64>> {
        let messages = self
            .get_by_conversation(conversation_id, 0, 100_000)
            .await?;
        Ok(messages
            .into_iter()
            .filter_map(|message| {
                let seq = message.conversation_seq;
                (seq > 0).then_some(seq)
            })
            .min())
    }
    async fn search(&self, keyword: &str, limit: u32) -> Result<Vec<IMMessage>>;
    /// Stores must implement the complete query contract; never silently discard filters.
    async fn search_by_query(&self, _query: &MessageSearchQuery) -> Result<Vec<IMMessage>> {
        Err(crate::shared::error::FlareError::localized(
            crate::shared::error::ErrorCode::OperationNotSupported,
            "Message store does not implement full search queries",
        ))
    }
    /// 在指定会话内按正文关键字搜索（本地 SQLite）。
    async fn search_in_conversation(
        &self,
        conversation_id: &str,
        keyword: &str,
        limit: u32,
    ) -> Result<Vec<IMMessage>>;
}

/// 消息写操作
#[async_trait]
pub trait MessageWriter: Send + Sync {
    async fn save_batch(&self, messages: &[IMMessage]) -> Result<()>;
    async fn save_one(&self, message: &IMMessage) -> Result<()>;
    /// 更新消息状态；返回受影响行数（`0` 表示本地尚无该消息，调用方可据此保留事件重放）。
    async fn update_status(&self, message_id: &str, status: i32) -> Result<u64>;
    /// 更新消息正文；`Ok(true)` 表示至少更新了一行（`server_id` 或 `client_msg_id` 命中）。
    async fn update_content(&self, message_id: &str, new_content: Vec<u8>) -> Result<bool>;
    async fn delete(&self, message_id: &str) -> Result<()>;

    /// 将本地存量消息从错误会话 ID 迁移到规范会话 ID。
    async fn rewrite_conversation_id(
        &self,
        from_conversation_id: &str,
        to_conversation_id: &str,
    ) -> Result<u64>;

    /// 发送 ACK 后更新：删除以 client_msg_id 为 server_id 的乐观写入行，再写入带 server_msg_id/seq 的终态消息（原子化，保证主键从 client_msg_id 迁移到 server_msg_id）
    async fn update_after_ack(&self, client_msg_id: &str, message: &IMMessage) -> Result<()>;
}

/// 消息统一端口（读写聚合）
#[async_trait]
pub trait MessageStore: MessageReader + MessageWriter {
    /// 应用编辑事件到存储层；实现基于权威 `event_seq`（conversation_seq）做收敛，避免旧编辑覆盖新内容、防止乱序/重复。
    async fn apply_edit_event(
        &self,
        message_id: &str,
        new_content: Vec<u8>,
        _event_seq: Option<u64>,
    ) -> Result<EditApplyResult> {
        let updated = self.update_content(message_id, new_content).await?;
        Ok(if updated {
            EditApplyResult::Applied
        } else {
            EditApplyResult::NotFound
        })
    }

    /// 应用删除事件到存储层；实现可基于 `event_seq/seq` 做顺序收敛，避免旧删除覆盖更新状态。
    async fn apply_delete_event(
        &self,
        message_id: &str,
        _event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        self.delete(message_id).await?;
        Ok(OperationApplyResult::Applied)
    }

    /// 应用置顶/取消置顶事件到存储层；实现可基于 `event_seq/seq` 防止旧状态覆盖新状态。
    async fn apply_pin_event(
        &self,
        message_id: &str,
        enabled: bool,
        _event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        self.set_message_flag(message_id, "pinned", enabled).await?;
        Ok(OperationApplyResult::Applied)
    }

    /// 应用标记/取消标记事件到存储层；实现可基于 `event_seq/seq` 防止旧状态覆盖新状态。
    async fn apply_mark_event(
        &self,
        message_id: &str,
        mark_type: i32,
        color: Option<&str>,
        set_mark: bool,
        _event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        if set_mark {
            self.set_message_mark(message_id, mark_type, color).await?;
        } else {
            self.clear_message_mark(message_id, mark_type).await?;
        }
        Ok(OperationApplyResult::Applied)
    }

    /// 写入插件卡片状态（`attributes.pluginStateJson` / `pluginStateVersion`），`message_id` 接受
    /// server_id 或 client_msg_id。只接受不小于当前的版本（相同版本以后到者为准），更旧返回
    /// `IgnoredStale`，找不到消息返回 `NotFound`。
    async fn apply_plugin_message_state(
        &self,
        _message_id: &str,
        _state: &PluginMessageState,
    ) -> Result<OperationApplyResult> {
        Ok(OperationApplyResult::NotFound)
    }

    /// 写入插件实体的共享层（`attributes.pluginSharedStateJson` / `pluginSharedStateVersion` /
    /// `pluginSharedEntity`），`message_id` 接受 server_id 或 client_msg_id。只接受更新的版本：
    /// 相同版本是重复、更旧的是乱序，都返回 `IgnoredStale`；找不到消息返回 `NotFound`（事件先于
    /// 消息到达，调用方回滚去重键等重放）。
    async fn apply_plugin_shared_state(
        &self,
        _message_id: &str,
        _state: &SharedState,
    ) -> Result<OperationApplyResult> {
        Ok(OperationApplyResult::NotFound)
    }

    /// 应用 retention「已安排」事件到存储层；实现可基于 `conversation_seq` 防旧事件回放覆盖新状态。
    async fn apply_retention_scheduled_event(
        &self,
        _message_id: &str,
        _policy: &MessageRetentionPolicy,
        _state: &MessageRetentionState,
        _scheduled_at: i64,
        _event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        Ok(OperationApplyResult::NotFound)
    }

    /// 应用 retention「已过期」事件到存储层；实现可基于 `conversation_seq` 防旧事件回放覆盖新状态。
    async fn apply_retention_expired_event(
        &self,
        _message_id: &str,
        _state: &MessageRetentionState,
        _expired_at: i64,
        _event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        Ok(OperationApplyResult::NotFound)
    }

    /// 应用 retention「已清理」事件到存储层；实现可基于 `conversation_seq` 防旧事件回放覆盖新状态。
    async fn apply_retention_purged_event(
        &self,
        _message_id: &str,
        _state: &MessageRetentionState,
        _purged_at: i64,
        _event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        Ok(OperationApplyResult::NotFound)
    }

    /// 对方已读回执落地：将当前用户在该会话中 `seq <= read_seq` 的已发送消息标记为已读。
    async fn mark_outgoing_read_upto_seq(
        &self,
        _conversation_id: &str,
        _sender_user_id: &str,
        _read_seq: u64,
    ) -> Result<()> {
        Ok(())
    }

    /// 以服务端会话摘要中的对端已读位点为权威，修正当前用户发出消息的已读投影。
    ///
    /// `peer_read_seq` 之前的消息可被标记为已读；其后的消息如果本地曾被 ACK/回显污染成已读，
    /// 需要回退到已发送，避免“对方离线但立刻双对号”。
    async fn reconcile_outgoing_read_by_peer_seq(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        peer_read_seq: u64,
    ) -> Result<()> {
        if peer_read_seq > 0 {
            self.mark_outgoing_read_upto_seq(conversation_id, sender_user_id, peer_read_seq)
                .await?;
        }
        Ok(())
    }

    /// 应用一条 reaction 事件到存储层（独立 reaction 表）。
    async fn apply_reaction(
        &self,
        _conversation_id: &str,
        _message_server_id: &str,
        _user_id: &str,
        _emoji: &str,
        _action: i32,
    ) -> Result<()> {
        Ok(())
    }

    /// 应用 reaction 事件到存储层；实现可基于 `(message,user,emoji)` 与 `event_seq/seq`
    /// 做顺序收敛，避免旧反应事件覆盖新状态。
    async fn apply_reaction_event(
        &self,
        conversation_id: &str,
        message_server_id: &str,
        user_id: &str,
        emoji: &str,
        action: i32,
        _event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        self.apply_reaction(conversation_id, message_server_id, user_id, emoji, action)
            .await?;
        Ok(OperationApplyResult::Applied)
    }

    /// 批量查询消息 reactions，key=message_server_id。
    async fn list_reactions(
        &self,
        _message_server_ids: &[String],
    ) -> Result<HashMap<String, Vec<ReactionEntry>>> {
        Ok(HashMap::new())
    }

    /// 设置消息 extra 中的布尔标记（如 pinned=true/false）。
    async fn set_message_flag(
        &self,
        _message_id: &str,
        _flag_key: &str,
        _enabled: bool,
    ) -> Result<()> {
        Ok(())
    }

    /// 设置消息标记（extra.markType / extra.markColor）。
    async fn set_message_mark(
        &self,
        _message_id: &str,
        _mark_type: i32,
        _color: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }

    /// 取消消息标记（清理 extra.markType / extra.markColor）。
    async fn clear_message_mark(&self, _message_id: &str, _mark_type: i32) -> Result<()> {
        Ok(())
    }

    /// 会话恢复自愈：将本地 `sending=1` 但不在 pending 队列中的孤儿消息改为 failed。
    async fn heal_orphan_sending_messages(
        &self,
        _sender_user_id: &str,
        _pending_client_msg_ids: &[String],
    ) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    /// 会话恢复自检：将 pending 队列里不属于当前账号的历史条目标记失败并移除。
    async fn heal_cross_account_pending_messages(
        &self,
        _sender_user_id: &str,
        _pending_client_msg_ids: &[String],
    ) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attributes(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn event_merge_keeps_the_newer_plugin_card_state() {
        let stored = attributes(&[
            (PLUGIN_STATE_JSON_KEY, r#"{"status":"completed"}"#),
            (PLUGIN_STATE_VERSION_KEY, "3"),
        ]);
        let merged = merge_message_event_attributes(attributes(&[("x", "1")]), stored.clone());
        assert_eq!(merged[PLUGIN_STATE_VERSION_KEY], "3");
        assert_eq!(merged["x"], "1");

        let older = attributes(&[
            (PLUGIN_STATE_JSON_KEY, r#"{"status":"open"}"#),
            (PLUGIN_STATE_VERSION_KEY, "3"),
        ]);
        let merged = merge_message_event_attributes(older, stored.clone());
        assert_eq!(merged[PLUGIN_STATE_JSON_KEY], r#"{"status":"completed"}"#);

        let newer = attributes(&[
            (PLUGIN_STATE_JSON_KEY, r#"{"status":"refunded"}"#),
            (PLUGIN_STATE_VERSION_KEY, "4"),
        ]);
        let merged = merge_message_event_attributes(newer, stored);
        assert_eq!(merged[PLUGIN_STATE_JSON_KEY], r#"{"status":"refunded"}"#);

        let state = |version: u64, based_on: Option<u64>| PluginMessageState {
            version,
            based_on_version: based_on,
            values: Default::default(),
        };
        let mut target = HashMap::new();
        assert!(write_plugin_message_state(
            &mut target,
            &state(2, Some(5)),
            "{}"
        ));
        assert_eq!(target[PLUGIN_STATE_BASED_ON_KEY], "5");
        assert!(!write_plugin_message_state(
            &mut target,
            &state(1, Some(9)),
            r#"{"a":1}"#
        ));
        assert_eq!(target[PLUGIN_STATE_BASED_ON_KEY], "5");
        assert!(write_plugin_message_state(
            &mut target,
            &state(2, None),
            r#"{"a":2}"#
        ));
        assert_eq!(target[PLUGIN_STATE_JSON_KEY], r#"{"a":2}"#);
        assert!(
            !target.contains_key(PLUGIN_STATE_BASED_ON_KEY),
            "basedOnVersion describes the values written with it"
        );
    }

    #[test]
    fn overwrites_carry_the_local_layer_with_its_based_on_version() {
        let stored = attributes(&[
            (PLUGIN_STATE_JSON_KEY, r#"{"canClose":true}"#),
            (PLUGIN_STATE_VERSION_KEY, "3"),
            (PLUGIN_STATE_BASED_ON_KEY, "7"),
        ]);
        let mut incoming = attributes(&[("x", "1")]);
        carry_plugin_message_state(&mut incoming, &stored);
        assert_eq!(incoming[PLUGIN_STATE_BASED_ON_KEY], "7");
        assert_eq!(incoming[PLUGIN_STATE_VERSION_KEY], "3");
        let mut unversioned = stored.clone();
        unversioned.remove(PLUGIN_STATE_BASED_ON_KEY);
        let mut incoming = attributes(&[(PLUGIN_STATE_BASED_ON_KEY, "9")]);
        carry_plugin_message_state(&mut incoming, &unversioned);
        assert!(!incoming.contains_key(PLUGIN_STATE_BASED_ON_KEY));
    }

    #[test]
    fn plugin_shared_state_keeps_the_higher_version_across_overwrites() {
        let state = |version: u64| SharedState {
            entity: "poll-1".into(),
            version,
            values: serde_json::json!({"n": version})
                .as_object()
                .unwrap()
                .clone(),
        };
        let mut stored = HashMap::new();
        assert_eq!(
            write_plugin_shared_state(&mut stored, &state(3)),
            SharedStateOrder::Newer
        );
        assert_eq!(
            write_plugin_shared_state(&mut stored, &state(3)),
            SharedStateOrder::Same
        );
        assert_eq!(
            write_plugin_shared_state(&mut stored, &state(2)),
            SharedStateOrder::Older
        );
        assert_eq!(stored[PLUGIN_SHARED_STATE_JSON_KEY], r#"{"n":3}"#);

        // A row without the layer (an edit, a receipt) keeps the stored one, entity included.
        let merged = merge_message_event_attributes(attributes(&[("x", "1")]), stored.clone());
        assert_eq!(merged[PLUGIN_SHARED_STATE_VERSION_KEY], "3");
        assert_eq!(merged[PLUGIN_SHARED_ENTITY_KEY], "poll-1");
        // History with the same or an older version keeps the stored one.
        for version in ["3", "1"] {
            let wire = attributes(&[
                (PLUGIN_SHARED_STATE_JSON_KEY, r#"{"n":0}"#),
                (PLUGIN_SHARED_STATE_VERSION_KEY, version),
            ]);
            let merged = merge_message_event_attributes(wire, stored.clone());
            assert_eq!(merged[PLUGIN_SHARED_STATE_JSON_KEY], r#"{"n":3}"#);
            assert_eq!(merged[PLUGIN_SHARED_STATE_VERSION_KEY], "3");
        }
        // A newer snapshot from history replaces it.
        let wire = attributes(&[
            (PLUGIN_SHARED_STATE_JSON_KEY, r#"{"n":5}"#),
            (PLUGIN_SHARED_STATE_VERSION_KEY, "5"),
        ]);
        let merged = merge_message_event_attributes(wire, stored);
        assert_eq!(merged[PLUGIN_SHARED_STATE_JSON_KEY], r#"{"n":5}"#);
        assert_eq!(merged[PLUGIN_SHARED_ENTITY_KEY], "poll-1");
    }
}
