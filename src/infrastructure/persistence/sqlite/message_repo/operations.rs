use super::*;

#[async_trait]
impl MessageStore for SqliteMessageRepo {
    async fn apply_edit_event(
        &self,
        message_id: &str,
        new_content: Vec<u8>,
        event_seq: Option<u64>,
    ) -> Result<EditApplyResult> {
        // 读改写必须同事务：SELECT（lastEditEventSeq 判定）与 UPDATE 之间若允许并发
        // 编辑事件交错，旧事件可能覆盖新事件。WAL 下并发写会以 BUSY 失败并走事件重放。
        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        let row = sqlx::query(&format!(
            r#"SELECT attributes, retention_state
               FROM messages
               WHERE {MESSAGE_ID_MATCH}
               LIMIT 1"#
        ))
        .bind(message_id)
        .bind(message_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(sqlx_err)?;

        let Some(row) = row else {
            return Ok(EditApplyResult::NotFound);
        };

        let extra_raw: Option<String> = row.try_get("attributes").map_err(sqlx_err)?;
        let mut attributes = parse_extra(extra_raw.as_deref());

        // 陈旧/乱序防护：用权威 event_seq（conversation_seq）判定，与反应路径一致。
        // 不再以 edit_version 排序——服务端无法在发布前同步分配版本，曾恒为 1，
        // 导致同一消息的第二次及以后编辑被误判陈旧、在客户端静默丢弃。
        if let Some(seq) = event_seq {
            let last_seq = attributes
                .get("lastEditEventSeq")
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            if seq <= last_seq {
                return Ok(EditApplyResult::IgnoredStale);
            }
            attributes.insert("lastEditEventSeq".to_string(), seq.to_string());
        }

        let retention: Option<MessageRetentionState> =
            decode_optional_proto(row.try_get("retention_state").map_err(sqlx_err)?);
        let new_content = if retention.as_ref().is_some_and(retention_hides_content) {
            Vec::new()
        } else {
            new_content
        };
        let now_ms = now_ms_i64();
        let text = text_for_sqlite_from_content_bytes(&new_content);
        let search_text = search_text_for_content_bytes(&new_content).or_else(|| text.clone());
        // currentEditVersion 仅作「已编辑」布尔信号（见 model::message::is_edited_from_attributes），
        // 精确计数对客户端无意义；本地单调自增即可，顺序已由 event_seq 保证。
        let current_edit_version = attributes
            .get("currentEditVersion")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(0);
        attributes.insert(
            "currentEditVersion".to_string(),
            (current_edit_version + 1).to_string(),
        );
        attributes.insert("messageFsmState".to_string(), "EDITED".to_string());
        attributes.insert("lastEditedAt".to_string(), now_ms.to_string());

        let rows = sqlx::query(&format!(
            r#"UPDATE messages
               SET encoded_content = ?, is_edited = 1, text = ?, updated_at = ?, attributes = ?
               WHERE {MESSAGE_ID_MATCH}"#
        ))
        .bind(&new_content)
        .bind(text.as_deref())
        .bind(now_ms)
        .bind(extra_to_json(&attributes))
        .bind(message_id)
        .bind(message_id)
        .execute(&mut *tx)
        .await
        .map_err(sqlx_err)?
        .rows_affected();
        if rows > 0 {
            let rows = sqlx::query(&format!(
                r#"SELECT server_id, conversation_id
                   FROM messages
                   WHERE {MESSAGE_ID_MATCH}"#
            ))
            .bind(message_id)
            .bind(message_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(sqlx_err)?;
            for row in rows {
                let server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
                let conversation_id: String = row.try_get("conversation_id").map_err(sqlx_err)?;
                upsert_message_fts_tx(
                    &mut tx,
                    &server_id,
                    &conversation_id,
                    search_text.as_deref(),
                )
                .await?;
            }
        }
        tx.commit().await.map_err(sqlx_err)?;

        Ok(if rows > 0 {
            EditApplyResult::Applied
        } else {
            EditApplyResult::NotFound
        })
    }

    async fn mark_outgoing_read_upto_seq(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        read_seq: u64,
    ) -> Result<()> {
        if conversation_id.trim().is_empty() || sender_user_id.trim().is_empty() || read_seq == 0 {
            return Ok(());
        }
        let created = MessageStatus::Created as i32;
        let sent = MessageStatus::Sent as i32;
        let persisted = MessageStatus::Persisted as i32;
        sqlx::query(
            r#"UPDATE messages
               SET status = CASE WHEN status = ? THEN ? ELSE status END,
                   is_read = 1
               WHERE conversation_id = ?
                 AND sender_id = ?
                 AND conversation_seq > 0
                 AND conversation_seq <= ?
                 AND status IN (?, ?, ?)"#,
        )
        .bind(created)
        .bind(sent)
        .bind(conversation_id)
        .bind(sender_user_id)
        .bind(read_seq as i64)
        .bind(created)
        .bind(sent)
        .bind(persisted)
        .execute(&self.pool)
        .await
        .map_err(sqlx_err)?;
        Ok(())
    }

    async fn reconcile_outgoing_read_by_peer_seq(
        &self,
        conversation_id: &str,
        sender_user_id: &str,
        peer_read_seq: u64,
    ) -> Result<()> {
        if conversation_id.trim().is_empty() || sender_user_id.trim().is_empty() {
            return Ok(());
        }
        // 读态单调:对端已读回执只前进（peer_read_seq 单调增），一旦某条被对方读过就恒为已读。
        // 早先这里会对 seq>peer_read_seq 的消息 is_read=0 回退——冷启/陈旧同步下发的偏低
        // （甚至 0，此时上面 mark 被跳过而 un-read 仍会抹掉全部已读）peer_read_seq 会把已读消息
        // 退回未读，表现为「双勾过一下变单勾」。只前进不回退，不再 un-read。
        if peer_read_seq > 0 {
            self.mark_outgoing_read_upto_seq(conversation_id, sender_user_id, peer_read_seq)
                .await?;
        }
        Ok(())
    }

    async fn apply_reaction(
        &self,
        conversation_id: &str,
        message_server_id: &str,
        user_id: &str,
        emoji: &str,
        action: i32,
    ) -> Result<()> {
        self.apply_reaction_event(
            conversation_id,
            message_server_id,
            user_id,
            emoji,
            action,
            None,
        )
        .await?;
        Ok(())
    }

    async fn apply_reaction_event(
        &self,
        conversation_id: &str,
        message_server_id: &str,
        user_id: &str,
        emoji: &str,
        action: i32,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        if message_server_id.is_empty() || user_id.is_empty() || emoji.is_empty() {
            return Ok(OperationApplyResult::NotFound);
        }
        let add_action = ReactionAction::Add as i32;
        let remove_action = ReactionAction::Remove as i32;
        if action != add_action && action != remove_action {
            return Ok(OperationApplyResult::NotFound);
        }

        let sequence = event_seq
            .filter(|seq| *seq > 0)
            .map(i64::try_from)
            .transpose()
            .map_err(|_| {
                FlareError::localized(
                    ErrorCode::InvalidParameter,
                    "reaction event sequence exceeds SQLite range",
                )
            })?;
        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        let row = sqlx::query(&format!("SELECT * FROM messages WHERE {MESSAGE_ID_MATCH}"))
            .bind(message_server_id)
            .bind(message_server_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(sqlx_err)?;
        let exists = row.is_some();
        let mut message = match row {
            Some(row) => self.row_to_immessage(&row)?,
            None => {
                let mut message = IMMessage::new(Default::default());
                message.server_id = message_server_id.to_owned();
                message.conversation_id = conversation_id.to_owned();
                message
            }
        };
        if message.conversation_id != conversation_id {
            return Ok(OperationApplyResult::NotFound);
        }
        if !exists {
            let previous: Option<i64> = sqlx::query_scalar("SELECT event_seq FROM message_reactions WHERE message_server_id = ? AND emoji = ? AND user_id = ?")
                .bind(message_server_id).bind(emoji).bind(user_id).fetch_optional(&mut *tx).await.map_err(sqlx_err)?;
            let previous = previous.unwrap_or(0);
            if sequence.is_some_and(|seq| seq <= previous) {
                return Ok(OperationApplyResult::IgnoredStale);
            }
            let now = now_ms_i64();
            sqlx::query("INSERT INTO message_reactions(message_server_id, conversation_id, emoji, user_id, created_at, updated_at, event_seq, is_removed) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(message_server_id, emoji, user_id) DO UPDATE SET conversation_id=excluded.conversation_id, updated_at=excluded.updated_at, event_seq=excluded.event_seq, is_removed=excluded.is_removed")
                .bind(message_server_id).bind(conversation_id).bind(emoji).bind(user_id).bind(now).bind(now)
                .bind(sequence.unwrap_or(previous)).bind(action == remove_action)
                .execute(&mut *tx).await.map_err(sqlx_err)?;
            tx.commit().await.map_err(sqlx_err)?;
            return Ok(OperationApplyResult::Applied);
        }
        let pending = merge_orphan_reactions_tx(&mut tx, &mut message).await?;
        let seq_key = format!("lastReactionEventSeq:{user_id}:{emoji}");
        if !advance_message_event(&mut message.attributes, &seq_key, event_seq) {
            return Ok(OperationApplyResult::IgnoredStale);
        }
        message.apply_reaction_change(user_id, emoji, action);
        if action == remove_action {
            sqlx::query("DELETE FROM message_reactions WHERE message_server_id = ? AND emoji = ? AND user_id = ?")
                .bind(&message.server_id).bind(emoji).bind(user_id).execute(&mut *tx).await.map_err(sqlx_err)?;
        } else {
            let now = now_ms_i64();
            sqlx::query("INSERT OR REPLACE INTO message_reactions(message_server_id, conversation_id, emoji, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)")
                .bind(&message.server_id).bind(&message.conversation_id).bind(emoji).bind(user_id).bind(now).bind(now)
                .execute(&mut *tx).await.map_err(sqlx_err)?;
        }
        sqlx::query("UPDATE messages SET attributes = ? WHERE server_id = ?")
            .bind(extra_to_json(&message.attributes))
            .bind(&message.server_id)
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;
        if pending {
            sqlx::query(
                "DELETE FROM message_reactions WHERE message_server_id = ? AND is_removed != 0",
            )
            .bind(&message.server_id)
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;
            sqlx::query("UPDATE message_reactions SET event_seq = 0 WHERE message_server_id = ? AND event_seq > 0")
                .bind(&message.server_id).execute(&mut *tx).await.map_err(sqlx_err)?;
        }
        tx.commit().await.map_err(sqlx_err)?;
        debug!(conversation_id, message_server_id = %message.server_id, user_id, emoji, action, "apply_reaction_event");
        Ok(OperationApplyResult::Applied)
    }

    async fn apply_delete_event(
        &self,
        message_id: &str,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let row = sqlx::query(&format!(
            r#"SELECT server_id, attributes
               FROM messages
               WHERE {MESSAGE_ID_MATCH}
               LIMIT 1"#
        ))
        .bind(message_id)
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(sqlx_err)?;

        let Some(row) = row else {
            return Ok(OperationApplyResult::NotFound);
        };

        let extra_raw: Option<String> = row.try_get("attributes").map_err(sqlx_err)?;
        let attributes = parse_extra(extra_raw.as_deref());
        if is_stale_operation(&attributes, "lastDeleteEventSeq", event_seq) {
            return Ok(OperationApplyResult::IgnoredStale);
        }

        self.delete(message_id).await?;
        Ok(OperationApplyResult::Applied)
    }

    async fn apply_pin_event(
        &self,
        message_id: &str,
        enabled: bool,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        apply_message_extra_with_seq(
            &self.pool,
            message_id,
            "lastPinEventSeq",
            event_seq,
            |attributes| {
                attributes.insert(
                    "pinned".to_string(),
                    if enabled { "true" } else { "false" }.to_string(),
                );
            },
        )
        .await
    }

    async fn apply_mark_event(
        &self,
        message_id: &str,
        mark_type: i32,
        color: Option<&str>,
        set_mark: bool,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        apply_message_extra_with_seq(
            &self.pool,
            message_id,
            "lastMarkEventSeq",
            event_seq,
            |attributes| {
                write_message_mark(attributes, mark_type, color, set_mark);
            },
        )
        .await
    }

    async fn apply_plugin_message_state(
        &self,
        message_id: &str,
        state: &PluginMessageState,
    ) -> Result<OperationApplyResult> {
        let values_json = encode_plugin_message_state(state)?;
        let message_id = message_id.trim();
        if message_id.is_empty() {
            return Ok(OperationApplyResult::NotFound);
        }
        // 版本比较与 UPDATE 同事务，避免两个并发动作结果交错时旧版本覆盖新版本。
        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        let rows = sqlx::query(&format!(
            r#"SELECT server_id, attributes FROM messages
               WHERE {MESSAGE_ACTION_ID_MATCH}"#
        ))
        .bind(message_id)
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(sqlx_err)?;
        if rows.is_empty() {
            return Ok(OperationApplyResult::NotFound);
        }
        let mut applied_any = false;
        for row in rows {
            let server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
            let extra_raw: Option<String> = row.try_get("attributes").map_err(sqlx_err)?;
            let mut attributes = parse_extra(extra_raw.as_deref());
            if !write_plugin_message_state(&mut attributes, state, &values_json) {
                continue;
            }
            sqlx::query("UPDATE messages SET attributes = ? WHERE server_id = ?")
                .bind(extra_to_json(&attributes))
                .bind(server_id)
                .execute(&mut *tx)
                .await
                .map_err(sqlx_err)?;
            applied_any = true;
        }
        tx.commit().await.map_err(sqlx_err)?;
        Ok(if applied_any {
            OperationApplyResult::Applied
        } else {
            OperationApplyResult::IgnoredStale
        })
    }

    async fn apply_plugin_shared_state(
        &self,
        message_id: &str,
        state: &SharedState,
    ) -> Result<OperationApplyResult> {
        validate_plugin_shared_state(state)?;
        let message_id = message_id.trim();
        if message_id.is_empty() {
            return Ok(OperationApplyResult::NotFound);
        }
        // 版本比较与 UPDATE 同事务：实时事件、同步重放和本人调用结果可能并发到达，旧版本不能盖住新版本。
        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        let rows = sqlx::query(&format!(
            r#"SELECT server_id, attributes FROM messages
               WHERE {MESSAGE_ACTION_ID_MATCH}"#
        ))
        .bind(message_id)
        .bind(message_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(sqlx_err)?;
        if rows.is_empty() {
            return Ok(OperationApplyResult::NotFound);
        }
        let mut applied_any = false;
        for row in rows {
            let server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
            let extra_raw: Option<String> = row.try_get("attributes").map_err(sqlx_err)?;
            let mut attributes = parse_extra(extra_raw.as_deref());
            if write_plugin_shared_state(&mut attributes, state) != SharedStateOrder::Newer {
                continue;
            }
            sqlx::query("UPDATE messages SET attributes = ? WHERE server_id = ?")
                .bind(extra_to_json(&attributes))
                .bind(server_id)
                .execute(&mut *tx)
                .await
                .map_err(sqlx_err)?;
            applied_any = true;
        }
        tx.commit().await.map_err(sqlx_err)?;
        Ok(if applied_any {
            OperationApplyResult::Applied
        } else {
            OperationApplyResult::IgnoredStale
        })
    }

    async fn apply_retention_scheduled_event(
        &self,
        message_id: &str,
        policy: &MessageRetentionPolicy,
        state: &MessageRetentionState,
        scheduled_at: i64,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        self.apply_retention(
            message_id,
            RetentionEventKind::Scheduled,
            Some(policy),
            state,
            scheduled_at,
            event_seq,
        )
        .await
    }

    async fn apply_retention_expired_event(
        &self,
        message_id: &str,
        state: &MessageRetentionState,
        expired_at: i64,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        self.apply_retention(
            message_id,
            RetentionEventKind::Expired,
            None,
            state,
            expired_at,
            event_seq,
        )
        .await
    }

    async fn apply_retention_purged_event(
        &self,
        message_id: &str,
        state: &MessageRetentionState,
        purged_at: i64,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        self.apply_retention(
            message_id,
            RetentionEventKind::Purged,
            None,
            state,
            purged_at,
            event_seq,
        )
        .await
    }

    async fn list_reactions(
        &self,
        message_server_ids: &[String],
    ) -> Result<HashMap<String, Vec<ReactionEntry>>> {
        if message_server_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut id_keys: Vec<String> = message_server_ids
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if id_keys.is_empty() {
            return Ok(HashMap::new());
        }
        id_keys.sort();
        id_keys.dedup();

        let mut alias_qb = QueryBuilder::<Sqlite>::new(
            "SELECT server_id, client_msg_id, is_local FROM messages WHERE server_id IN (",
        );
        let mut alias_sep_a = alias_qb.separated(", ");
        for id in &id_keys {
            alias_sep_a.push_bind(id);
        }
        alias_qb.push(") OR client_msg_id IN (");
        let mut alias_sep_b = alias_qb.separated(", ");
        for id in &id_keys {
            alias_sep_b.push_bind(id);
        }
        alias_qb.push(") AND is_local = 1");
        let alias_rows = alias_qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(sqlx_err)?;

        let mut servers = HashMap::new();
        let mut clients: HashMap<String, Option<String>> = HashMap::new();
        for row in alias_rows {
            let sid: String = row.try_get("server_id").map_err(sqlx_err)?;
            let cid: String = row.try_get("client_msg_id").map_err(sqlx_err)?;
            servers.insert(sid.clone(), sid.clone());
            if row.try_get::<i32, _>("is_local").map_err(sqlx_err)? != 0 && !cid.is_empty() {
                clients
                    .entry(cid)
                    .and_modify(|target| *target = None)
                    .or_insert(Some(sid));
            }
        }
        let resolved: HashMap<_, _> = id_keys
            .iter()
            .map(|id| {
                let target = servers
                    .get(id)
                    .or_else(|| clients.get(id).and_then(Option::as_ref))
                    .unwrap_or(id);
                (id.clone(), target.clone())
            })
            .collect();
        let mut qb = QueryBuilder::<Sqlite>::new(
            "SELECT message_server_id, emoji, user_id FROM message_reactions WHERE is_removed = 0 AND message_server_id IN (",
        );
        let mut separated = qb.separated(", ");
        for id in resolved.values() {
            separated.push_bind(id);
        }
        qb.push(") ORDER BY updated_at ASC");
        let rows = qb.build().fetch_all(&self.pool).await.map_err(sqlx_err)?;

        let mut grouped: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        for row in rows {
            let msg_id: String = row.try_get("message_server_id").map_err(sqlx_err)?;
            let emoji: String = row.try_get("emoji").map_err(sqlx_err)?;
            let user_id: String = row.try_get("user_id").map_err(sqlx_err)?;
            let resolved_id = msg_id.trim().to_string();
            if resolved_id.is_empty() {
                continue;
            }
            grouped
                .entry(resolved_id)
                .or_default()
                .entry(emoji)
                .or_default()
                .push(user_id);
        }

        let mut out: HashMap<String, Vec<ReactionEntry>> = HashMap::new();
        for (msg_id, emoji_map) in grouped {
            let mut reactions = Vec::with_capacity(emoji_map.len());
            for (emoji, mut user_ids) in emoji_map {
                user_ids.sort();
                user_ids.dedup();
                reactions.push(ReactionEntry {
                    emoji,
                    count: user_ids.len() as u32,
                    user_ids,
                });
            }
            reactions.sort_by(|a, b| a.emoji.cmp(&b.emoji));
            out.insert(msg_id, reactions);
        }
        Ok(resolved
            .into_iter()
            .filter_map(|(requested, canonical)| {
                out.get(&canonical)
                    .cloned()
                    .map(|reactions| (requested, reactions))
            })
            .collect())
    }

    async fn set_message_flag(
        &self,
        message_id: &str,
        flag_key: &str,
        enabled: bool,
    ) -> Result<()> {
        if message_id.trim().is_empty() || flag_key.trim().is_empty() {
            return Ok(());
        }
        let rows = sqlx::query(&format!(
            r#"SELECT server_id, attributes FROM messages
               WHERE {MESSAGE_ACTION_ID_MATCH}"#
        ))
        .bind(message_id)
        .bind(message_id)
        .fetch_all(&self.pool)
        .await
        .map_err(sqlx_err)?;

        if rows.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        for row in rows {
            let server_id: String = row.try_get("server_id").map_err(sqlx_err)?;
            let extra_raw: Option<String> = row.try_get("attributes").map_err(sqlx_err)?;
            let mut attributes = parse_extra(extra_raw.as_deref());
            attributes.insert(
                flag_key.to_string(),
                if enabled { "true" } else { "false" }.to_string(),
            );
            sqlx::query("UPDATE messages SET attributes = ? WHERE server_id = ?")
                .bind(extra_to_json(&attributes))
                .bind(server_id)
                .execute(&mut *tx)
                .await
                .map_err(sqlx_err)?;
        }
        tx.commit().await.map_err(sqlx_err)?;
        Ok(())
    }

    async fn set_message_mark(
        &self,
        message_id: &str,
        mark_type: i32,
        color: Option<&str>,
    ) -> Result<()> {
        self.apply_mark_event(message_id, mark_type, color, true, None)
            .await?;
        Ok(())
    }

    async fn clear_message_mark(&self, message_id: &str, mark_type: i32) -> Result<()> {
        self.apply_mark_event(message_id, mark_type, None, false, None)
            .await?;
        Ok(())
    }

    async fn heal_orphan_sending_messages(
        &self,
        sender_user_id: &str,
        pending_client_msg_ids: &[String],
    ) -> Result<Vec<String>> {
        if sender_user_id.trim().is_empty() {
            return Ok(Vec::new());
        }
        let mut select_qb = QueryBuilder::<Sqlite>::new(
            "SELECT client_msg_id FROM messages WHERE sending = 1 AND failed = 0 AND is_local = 1 AND sender_id = ",
        );
        select_qb.push_bind(sender_user_id);
        if !pending_client_msg_ids.is_empty() {
            select_qb.push(" AND client_msg_id NOT IN (");
            let mut separated = select_qb.separated(", ");
            for id in pending_client_msg_ids {
                separated.push_bind(id);
            }
            select_qb.push(")");
        }
        let orphan_rows = select_qb
            .build_query_as::<(String,)>()
            .fetch_all(&self.pool)
            .await
            .map_err(sqlx_err)?;
        if orphan_rows.is_empty() {
            return Ok(Vec::new());
        }

        let orphan_client_ids: Vec<String> = orphan_rows.into_iter().map(|(id,)| id).collect();
        let mut update_qb = QueryBuilder::<Sqlite>::new(
            "UPDATE messages SET sending = 0, failed = 1, uploading = 0, status = ",
        );
        update_qb.push_bind(MessageStatus::Failed as i32);
        update_qb.push(", updated_at = ");
        update_qb.push_bind(now_ms_i64());
        update_qb.push(" WHERE client_msg_id IN (");
        let mut separated = update_qb.separated(", ");
        for id in &orphan_client_ids {
            separated.push_bind(id);
        }
        update_qb.push(")");
        let query = update_qb.build();
        query.execute(&self.pool).await.map_err(sqlx_err)?;
        Ok(orphan_client_ids)
    }

    async fn heal_cross_account_pending_messages(
        &self,
        sender_user_id: &str,
        pending_client_msg_ids: &[String],
    ) -> Result<Vec<String>> {
        if sender_user_id.trim().is_empty() || pending_client_msg_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut select_qb = QueryBuilder::<Sqlite>::new(
            "SELECT client_msg_id FROM messages WHERE client_msg_id IN (",
        );
        {
            let mut separated = select_qb.separated(", ");
            for id in pending_client_msg_ids {
                separated.push_bind(id);
            }
        }
        select_qb.push(") AND (sender_id = '' OR sender_id != ");
        select_qb.push_bind(sender_user_id);
        select_qb.push(")");
        let mismatched_rows = select_qb
            .build_query_as::<(String,)>()
            .fetch_all(&self.pool)
            .await
            .map_err(sqlx_err)?;
        if mismatched_rows.is_empty() {
            return Ok(Vec::new());
        }
        let mismatched_client_ids: Vec<String> =
            mismatched_rows.into_iter().map(|(id,)| id).collect();

        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        let mut update_qb = QueryBuilder::<Sqlite>::new(
            "UPDATE messages SET sending = 0, failed = 1, uploading = 0, status = ",
        );
        update_qb.push_bind(MessageStatus::Failed as i32);
        update_qb.push(", updated_at = ");
        update_qb.push_bind(now_ms_i64());
        update_qb.push(" WHERE client_msg_id IN (");
        {
            let mut separated = update_qb.separated(", ");
            for id in &mismatched_client_ids {
                separated.push_bind(id);
            }
        }
        update_qb.push(")");
        update_qb
            .build()
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;

        let mut delete_qb =
            QueryBuilder::<Sqlite>::new("DELETE FROM pending_sends WHERE client_msg_id IN (");
        {
            let mut separated = delete_qb.separated(", ");
            for id in &mismatched_client_ids {
                separated.push_bind(id);
            }
        }
        delete_qb.push(")");
        delete_qb
            .build()
            .execute(&mut *tx)
            .await
            .map_err(sqlx_err)?;

        tx.commit().await.map_err(sqlx_err)?;
        Ok(mismatched_client_ids)
    }
}

impl SqliteMessageRepo {
    async fn apply_retention(
        &self,
        message_id: &str,
        kind: RetentionEventKind,
        policy: Option<&MessageRetentionPolicy>,
        state: &MessageRetentionState,
        at: i64,
        event_seq: Option<u64>,
    ) -> Result<OperationApplyResult> {
        let mut tx = self.pool.begin().await.map_err(sqlx_err)?;
        let rows = sqlx::query(&format!("SELECT * FROM messages WHERE {MESSAGE_ID_MATCH}"))
            .bind(message_id)
            .bind(message_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(sqlx_err)?;
        if rows.is_empty() {
            return Ok(OperationApplyResult::NotFound);
        }
        let mut applied = false;
        for row in rows {
            let mut message = self.row_to_immessage(&row)?;
            if apply_retention_event(&mut message, kind, policy, state, at, event_seq)
                != OperationApplyResult::Applied
            {
                continue;
            }
            sqlx::query("UPDATE messages SET attributes = ?, retention_policy = ?, retention_state = ?, encoded_content = ?, text = ?, quote_preview = ?, extensions = ?, updated_at = ? WHERE server_id = ?")
                .bind(extra_to_json(&message.attributes))
                .bind(encode_optional_proto(&message.retention_policy))
                .bind(encode_optional_proto(&message.retention_state))
                .bind(&message.encoded_content)
                .bind(message_preview_for_storage(&message))
                .bind(&message.quote_preview)
                .bind(extensions_to_json(&message.extensions))
                .bind(message.updated_at as i64)
                .bind(&message.server_id)
                .execute(&mut *tx).await.map_err(sqlx_err)?;
            if message.encoded_content.is_empty() {
                delete_message_fts_by_server_id_tx(&mut tx, &message.server_id).await?;
            }
            if message
                .retention_state
                .as_ref()
                .is_some_and(retention_hides_content)
            {
                sqlx::query("UPDATE conversations SET last_message_preview = NULL WHERE conversation_id = ? AND last_message_id = ?")
                    .bind(&message.conversation_id)
                    .bind(if message.server_id.is_empty() { &message.client_msg_id } else { &message.server_id })
                    .execute(&mut *tx).await.map_err(sqlx_err)?;
            }
            applied = true;
        }
        tx.commit().await.map_err(sqlx_err)?;
        Ok(if applied {
            OperationApplyResult::Applied
        } else {
            OperationApplyResult::IgnoredStale
        })
    }
}
