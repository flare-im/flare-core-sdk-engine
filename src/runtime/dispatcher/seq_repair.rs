use super::*;

impl Dispatcher {
    pub(super) async fn local_tail_seqs_before_incoming(
        &self,
        conversation_id: &str,
        min_incoming_seq: u64,
    ) -> Vec<u64> {
        let Some(stores) = &self.stores else {
            return Vec::new();
        };
        if min_incoming_seq <= 1 {
            return Vec::new();
        }
        match stores
            .messages
            .get_by_conversation(conversation_id, min_incoming_seq, 100)
            .await
        {
            Ok(messages) => {
                let mut seqs = messages
                    .into_iter()
                    .map(|message| message.conversation_seq)
                    .filter(|seq| *seq > 0)
                    .collect::<Vec<_>>();
                seqs.sort_unstable();
                seqs.dedup();
                seqs
            }
            Err(error) => {
                warn!(
                    conversation_id = %conversation_id,
                    min_incoming_seq,
                    error = %error,
                    "读取实时消息前置本地 seq 失败，降级使用同步游标"
                );
                Vec::new()
            }
        }
    }

    /// 持久化一批 durable 消息（save_batch → 会话投影），成功后推进同步游标。
    /// 返回是否 durable：事件批据此完成去重，实时下行据此发送送达确认。
    /// A2：已与视图投递（finish_batch）解耦；win#2 将把对本方法的调用移到有界串行后台 worker，
    /// 使 socket 读循环不再被持久化 I/O 头阻塞。单 worker 串行可保持游标推进无竞态。
    pub(super) async fn persist_durable_batch(
        &self,
        durable_messages: &[IMMessage],
        current_user_id: &str,
    ) -> bool {
        let Some(inbound) = &self.inbound else {
            return true;
        };
        if durable_messages.is_empty() {
            return true;
        }
        if let Err(error) = inbound
            .persist(
                durable_messages,
                current_user_id,
                InboundSource::RealtimePush,
            )
            .await
        {
            warn!(%error, "inbound message commit failed");
            self.repair_message_seq_after_persist(durable_messages)
                .await;
            return false;
        }
        self.repair_message_seq_after_persist(durable_messages)
            .await;
        true
    }

    pub(super) async fn repair_message_seq_after_persist(&self, messages: &[IMMessage]) {
        let Some(stores) = &self.stores else {
            return;
        };
        let user_id = self.current_user_id.read().await.clone();
        if user_id.trim().is_empty() {
            return;
        }

        let mut by_conversation = HashMap::<String, Vec<u64>>::new();
        for message in messages {
            if message.conversation_id.trim().is_empty() || message.conversation_seq == 0 {
                continue;
            }
            by_conversation
                .entry(message.conversation_id.clone())
                .or_default()
                .push(message.conversation_seq);
        }

        for (conversation_id, mut seqs) in by_conversation {
            seqs.sort_unstable();
            seqs.dedup();
            let cursor_seq = match stores
                .cursors
                .get_conversation_cursor(&user_id, &conversation_id)
                .await
            {
                Ok(cursor) => cursor.map(|c| c.last_seq).unwrap_or(0),
                Err(error) => {
                    warn!(
                        conversation_id = %conversation_id,
                        error = %error,
                        "读取会话消息游标失败，跳过实时消息 seq 补偿"
                    );
                    continue;
                }
            };
            // 纵深防御：本端新发消息若拿到 ≤ 已同步高水位的回退 seq，说明服务端序列分配异常
            // （见 flare-im-seq floor 自愈）。仅告警使异常可观测，不改排序。
            let regressed_seqs = detect_outgoing_seq_regressions(messages, &user_id, cursor_seq)
                .into_iter()
                .filter(|seq| {
                    messages.iter().any(|message| {
                        message.conversation_id == conversation_id
                            && message.conversation_seq == *seq
                    })
                })
                .collect::<Vec<_>>();
            if !regressed_seqs.is_empty() {
                warn!(
                    conversation_id = %conversation_id,
                    cursor_seq,
                    regressed_seqs = ?regressed_seqs,
                    "检测到本端发出的消息拿到回退 conversation_seq（服务端序列分配异常）；\
                     时间线可能错位，请检查服务端 seq floor 自愈"
                );
                self.metrics.counter_with_labels(
                    "sdk.seq.outgoing_regression_detected",
                    &[MetricLabel::new("conversation", conversation_id.clone())],
                    regressed_seqs.len() as u64,
                );
            }

            let min_incoming_seq = seqs.first().copied().unwrap_or_default();
            let local_tail_seqs = self
                .local_tail_seqs_before_incoming(&conversation_id, min_incoming_seq)
                .await;
            let local_before_seq = local_tail_seqs.last().copied().unwrap_or(0);
            let base_seq = cursor_seq;
            let mut continuity_window = local_tail_seqs;
            continuity_window.extend(seqs.iter().copied());
            let coverage = match stores.conversations.get(&conversation_id).await {
                Ok(Some(conversation)) => crate::domain::message_projection_coverage(&conversation),
                Ok(None) => Ok(crate::domain::TimelineCoverage::default()),
                Err(error) => Err(error),
            };
            let coverage = match coverage {
                Ok(coverage) => coverage,
                Err(error) => {
                    warn!(%conversation_id, %error, "读取会话投影完成记录失败，跳过游标推进");
                    continue;
                }
            };
            // A stored row is durable only after its conversation projection commits.
            continuity_window.retain(|seq| coverage.range_containing(*seq).is_some());
            // 操作事件与消息共享会话 seq 分配器：已应用的操作事件占据的 seq
            // 在消息序列里表现为「空洞」。不计入连续窗口会让每条 reaction/已读都被判成缺口，
            // 触发冗余补拉与 warn 噪音，且游标被卡在事件 seq 之前。
            continuity_window.extend(
                self.applied_event_seqs
                    .seqs_in_range(
                        &conversation_id,
                        base_seq,
                        seqs.last().copied().unwrap_or(u64::MAX),
                    )
                    .await,
            );
            continuity_window.sort_unstable();
            continuity_window.dedup();
            let contiguous_seq = max_contiguous_seq(base_seq, &continuity_window);
            let window_gap_after =
                seq_repair_gap_after(base_seq, local_before_seq, &continuity_window).or_else(
                    || {
                        (contiguous_seq < seqs.last().copied().unwrap_or_default())
                            .then_some(contiguous_seq)
                    },
                );
            if contiguous_seq > cursor_seq
                && let Err(error) = stores
                    .cursors
                    .save_conversation_cursor(&SyncCursorVo {
                        user_id: user_id.clone(),
                        conversation_id: conversation_id.clone(),
                        last_seq: contiguous_seq,
                        synced_at: now_ms(),
                    })
                    .await
            {
                warn!(
                    conversation_id = %conversation_id,
                    contiguous_seq,
                    error = %error,
                    "保存实时消息连续游标失败"
                );
            }

            if let Some(first_gap_after) = window_gap_after {
                warn!(
                    conversation_id = %conversation_id,
                    cursor_seq,
                    local_before_seq,
                    base_seq,
                    first_gap_after,
                    max_incoming_seq = seqs.last().copied().unwrap_or_default(),
                    "实时消息 seq 出现缺口，后台触发单会话补拉"
                );
                if let Some(sync) = self.session_sync.clone() {
                    let now = now_ms();
                    let mut guard = self.seq_repair_state.lock().await;
                    prune_seq_repair_state(&mut guard, now);
                    if !guard.contains_key(&conversation_id)
                        && guard.len() >= SEQ_REPAIR_MAX_TRACKED_CONVERSATIONS
                    {
                        warn!(
                            conversation_id = %conversation_id,
                            tracked = guard.len(),
                            limit = SEQ_REPAIR_MAX_TRACKED_CONVERSATIONS,
                            "实时消息缺口补拉状态已达上限，跳过本次新会话补拉触发"
                        );
                        continue;
                    }
                    let state = guard.entry(conversation_id.clone()).or_default();
                    state.updated_at_ms = now;
                    let has_progress = contiguous_seq > state.last_progress_seq;
                    if state.in_flight {
                        continue;
                    }
                    if state.last_gap_after == first_gap_after
                        && !has_progress
                        && now < state.next_attempt_at_ms
                    {
                        warn!(
                            conversation_id = %conversation_id,
                            first_gap_after,
                            next_attempt_at_ms = state.next_attempt_at_ms,
                            "实时消息缺口补拉处于退避窗口，跳过本次重复触发"
                        );
                        continue;
                    }
                    if state.last_gap_after != first_gap_after || has_progress {
                        state.attempts = 0;
                    }
                    state.in_flight = true;
                    state.last_gap_after = first_gap_after;
                    state.last_progress_seq = state.last_progress_seq.max(contiguous_seq);
                    state.attempts = state.attempts.saturating_add(1);
                    let attempt = state.attempts;
                    state.next_attempt_at_ms = now + seq_repair_backoff_ms(attempt);
                    drop(guard);

                    let repair_state = self.seq_repair_state.clone();
                    spawn_background(async move {
                        let repair_result = sync
                            .request_message_sync_from_seq(
                                &conversation_id,
                                first_gap_after,
                                DEFAULT_SYNC_LIMIT,
                            )
                            .await;
                        if let Err(error) = &repair_result {
                            warn!(
                                conversation_id = %conversation_id,
                                first_gap_after,
                                error = %error,
                                "实时消息缺口补拉失败"
                            );
                        }
                        let mut guard = repair_state.lock().await;
                        if repair_result.is_ok() {
                            guard.remove(&conversation_id);
                        } else if let Some(state) = guard.get_mut(&conversation_id) {
                            state.in_flight = false;
                            state.updated_at_ms = now_ms();
                        }
                    });
                } else {
                    warn!(
                        conversation_id = %conversation_id,
                        first_gap_after,
                        "实时消息出现 seq 缺口，但 SDK 未配置单会话同步器，无法自动补拉"
                    );
                }
            } else {
                self.seq_repair_state.lock().await.remove(&conversation_id);
            }
        }
    }
}
