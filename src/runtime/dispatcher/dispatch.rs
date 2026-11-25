use super::*;

/// Inbound delivery lag above this is logged at warn so a slow-to-arrive message stands out in
/// client logs; normal deliveries stay at debug.
const SLOW_DELIVERY_LOG_THRESHOLD_MS: u64 = 1000;

impl Dispatcher {
    pub(super) fn event_conversation_id(event: &flare_proto::common::Event) -> &str {
        let outer = event.conversation_id.trim();
        if !outer.is_empty() {
            return outer;
        }
        match event.payload.as_ref() {
            Some(EventPayload::Conversation(conversation)) => conversation.conversation_id.trim(),
            _ => "",
        }
    }

    pub(super) async fn message_operation_conversation_id(
        &self,
        event_conversation_id: &str,
        server_msg_id: &str,
    ) -> String {
        let event_conversation_id = event_conversation_id.trim();
        if !event_conversation_id.is_empty() {
            return event_conversation_id.to_string();
        }
        let server_msg_id = server_msg_id.trim();
        if server_msg_id.is_empty() {
            return String::new();
        }
        let Some(stores) = self.stores.as_ref() else {
            return String::new();
        };
        match stores.messages.get(server_msg_id).await {
            Ok(Some(message)) => message.conversation_id.trim().to_string(),
            Ok(None) => String::new(),
            Err(error) => {
                warn!(error = %error, server_msg_id, "resolve message operation conversation_id failed");
                String::new()
            }
        }
    }

    pub async fn dispatch(
        &self,
        payload: DownlinkPayload,
    ) -> flare_core::common::error::Result<()> {
        self.bus
            .lifetime()
            .until_retired(self.dispatch_active(payload, None))
            .await
            .unwrap_or(Ok(()))
    }

    pub(crate) async fn dispatch_from_link(
        &self,
        payload: DownlinkPayload,
        link: crate::runtime::link::epoch::ConnectionLease,
    ) -> flare_core::common::error::Result<()> {
        let work = self.dispatch_active(payload, Some(link.clone()));
        self.bus
            .lifetime()
            .until_retired(link.while_current(work))
            .await
            .flatten()
            .unwrap_or(Ok(()))
    }

    async fn dispatch_active(
        &self,
        payload: DownlinkPayload,
        link: Option<crate::runtime::link::epoch::ConnectionLease>,
    ) -> flare_core::common::error::Result<()> {
        match payload {
            DownlinkPayload::MessagePush(push) => {
                self.metrics.counter_with_labels(
                    "dispatcher.payload_total",
                    &[MetricLabel::new("type", "message_push")],
                    1,
                );
                // payload 已按值传入，直接 move 入站 proto，避免在接收热路径上深拷贝整批消息。
                let mut all = Vec::with_capacity(push.messages.len() + push.notifications.len());
                all.extend(push.messages);
                all.extend(push.notifications);
                self.metrics
                    .counter("dispatcher.message_push_items_total", all.len() as u64);
                // 送达确认在帧到达时定下，处理完再发（见 `delivery_due`）。
                let arrived = all
                    .iter()
                    .map(|m| (m.conversation_id.as_str(), m.conversation_seq));
                let delivery_due = self.delivery_due(arrived);
                let messages: Vec<IMMessage> = all.into_iter().map(IMMessage::new).collect();
                let current_user_id = self.current_user_id.read().await.clone();
                let batch = self
                    .prepare_inbound(&current_user_id, messages)
                    .await
                    .map_err(|e| flare_core::common::error::FlareError::system(e.to_string()))?;
                let durable_for_persist = batch.durable.clone();
                let inbound = batch.into_notifications();
                if !inbound.is_empty() {
                    // A2/BX-02：先投递到可观察视图（下一帧上屏），再持久化，使「收到→上屏」延迟脱离磁盘 I/O。
                    // 持久化失败时同步游标不前进，既有 seq-gap 补拉（repair_message_seq_after_persist）会重取，
                    // 无需额外对账路径。仅克隆 durable 子集供持久化借用；durable+ephemeral move 进 finish_batch。
                    self.notification_pipeline
                        .finish_batch(inbound, &current_user_id)
                        .await;
                    // win#2：装载了 worker 则投递到有界串行后台 worker（满则在 send().await 背压，
                    // 只压上行读取——视图已先行投递）；未装载（测试 harness）则内联持久化，保持同步语义。
                    let job = PersistJob {
                        link,
                        messages: durable_for_persist,
                        user_id: current_user_id,
                        delivery_due,
                    };
                    if let Some(tx) = self.persist_tx.get() {
                        if let Err(err) = tx.send(job).await {
                            self.persist_and_confirm(err.0).await;
                        }
                    } else {
                        self.persist_and_confirm(job).await;
                    }
                } else {
                    // Convergence already proved the messages were projected.
                    self.confirm_delivered(delivery_due);
                }
            }
            DownlinkPayload::Event(ev) => {
                self.metrics.counter_with_labels(
                    "dispatcher.payload_total",
                    &[MetricLabel::new("type", "event")],
                    1,
                );
                let delivery_due = self.delivery_due(arrived_message(&ev));
                self.dispatch_single_event(&ev, delivery_due).await;
            }
            DownlinkPayload::EventEnvelope(mut env) => {
                self.metrics.counter_with_labels(
                    "dispatcher.payload_total",
                    &[MetricLabel::new("type", "event_envelope")],
                    1,
                );
                self.metrics.counter(
                    "dispatcher.event_envelope_items_total",
                    env.events.len() as u64,
                );
                tracing::debug!(
                    event_count = env.events.len(),
                    conversation_id = %env.conversation_id,
                    max_conversation_seq = env.max_conversation_seq,
                    "dispatch EventEnvelope (push/sync)"
                );
                // 信封内的**消息事件**合并为一个 ReceivedBatch（避免逐条回调，与 MessagePush 一致）；
                // 非消息事件（recall/edit/reaction/read 等）仍逐条处理。
                // 消息 payload 按值 move（不再 message+Event 双份深克隆）；失败回滚只保留小去重键。
                // 送达确认在帧到达时定下：信封里全部消息事件，含下面去重掉的重复事件。
                let arrived = env.events.iter().filter_map(arrived_message);
                let mut delivery_due = self.delivery_due(arrived);
                let current_user_id = self.current_user_id.read().await.clone();
                let mut batch_messages: Vec<IMMessage> = Vec::new();
                let mut batched_reservations: Vec<EventReservation> = Vec::new();
                for mut ev in std::mem::take(&mut env.events) {
                    if matches!(&ev.payload, Some(EventPayload::Message(_))) {
                        match self.event_deduper.claim(&ev) {
                            EventClaim::New(reservation) => {
                                batched_reservations.push(reservation);
                                if let Some(EventPayload::Message(m)) = ev.payload.take() {
                                    batch_messages.push(IMMessage::new(m));
                                }
                            }
                            EventClaim::Applied => {}
                            EventClaim::InFlight => delivery_due.clear(),
                        }
                    } else {
                        self.dispatch_single_event(&ev, Vec::new()).await;
                    }
                }
                if batch_messages.is_empty() {
                    self.confirm_delivered(delivery_due);
                } else {
                    self.dispatch_inbound_message_event_batch(
                        batch_messages,
                        batched_reservations,
                        &current_user_id,
                        delivery_due,
                    )
                    .await;
                }
                self.maybe_trigger_event_envelope_waterline(&env).await;
            }
            DownlinkPayload::SendAck(ack) => {
                self.metrics.counter_with_labels(
                    "dispatcher.payload_total",
                    &[MetricLabel::new("type", "send_ack")],
                    1,
                );
                if ack.conversation_id.trim().is_empty()
                    && crate::domain::MessageDeliveryService::accepted_from_ack(&ack).is_some()
                {
                    warn!(client_msg_id = %ack.client_msg_id, "discarding unscoped send acceptance");
                    return Ok(());
                }
                let mut ack = ack;
                if ack.conversation_id.trim().is_empty()
                    && let Some(ref stores) = self.stores
                {
                    match stores
                        .messages
                        .get_by_client_msg_id(&ack.client_msg_id)
                        .await
                    {
                        Ok(Some(local)) if !local.conversation_id.trim().is_empty() => {
                            ack.conversation_id = local.conversation_id.clone();
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(error = %e, client_msg_id = %ack.client_msg_id, "enrich send ack conversation_id failed");
                        }
                    }
                }
                if let Some(q) = &self.reliable_queue {
                    let _ = q.on_ack(ack.clone()).await;
                } else {
                    self.bus.publish(SdkEvent::Message(MessageEvent::SendAck {
                        ack: Box::new(ack),
                    }));
                }
            }
            DownlinkPayload::CustomData(data) => {
                self.metrics.counter_with_labels(
                    "dispatcher.payload_total",
                    &[MetricLabel::new("type", "custom_data")],
                    1,
                );
                self.maybe_trigger_waterline_pull(
                    "custom_data",
                    &data.r#type,
                    None,
                    &data.attributes,
                )
                .await;
                self.bus.publish(SdkEvent::Extension(ExtensionEvent {
                    source: "custom".to_string(),
                    event_type: data.r#type.clone(),
                    payload: data.payload.clone(),
                }));
            }
            DownlinkPayload::Capability(packet) => {
                self.metrics.counter_with_labels(
                    "dispatcher.payload_total",
                    &[MetricLabel::new("type", "capability")],
                    1,
                );
                let capability_id = packet.capability_id.clone();
                // The packet's expiry is the server's clock: compare it with the server time
                // estimate, not the device clock.
                match plugin_packets::classify(packet, self.server_clock().now_millis()) {
                    Ok(mut realtime) => {
                        // Hosts compare the expiry with their own clock: hand it over on it.
                        realtime.expires_at_ms =
                            self.server_clock().to_local_millis(realtime.expires_at_ms);
                        self.bus
                            .publish(SdkEvent::Plugin(PluginEvent::Realtime(realtime)));
                    }
                    Err(reason) => {
                        tracing::debug!(
                            target: "flare_sdk.plugin",
                            %capability_id,
                            ?reason,
                            "plugin capability packet dropped"
                        );
                    }
                }
            }
            DownlinkPayload::TenantNotice(notice) => {
                self.metrics.counter_with_labels(
                    "dispatcher.payload_total",
                    &[MetricLabel::new("type", "tenant_notice")],
                    1,
                );
                if let Some(notices) = &self.tenant_notices {
                    notices.notice(notice);
                }
            }
            DownlinkPayload::RealtimeControl(control) => {
                self.metrics.counter_with_labels(
                    "dispatcher.payload_total",
                    &[MetricLabel::new("type", "realtime_control")],
                    1,
                );
                let conversation_id = control.conversation_id.clone().unwrap_or_default();
                self.maybe_trigger_waterline_pull(
                    "realtime_control",
                    &control.control_type,
                    Some(&conversation_id),
                    &control.attributes,
                )
                .await;
                match control.payload {
                    Some(flare_proto::common::realtime_control_packet::Payload::Typing(typing)) => {
                        // Feed the presence tracker (per-user -> unified TypingAggregate with TTL);
                        // keep the raw per-user event for existing consumers.
                        self.apply_typing_user(&conversation_id, &typing.user_id, typing.typing);
                        self.bus.publish(SdkEvent::Message(MessageEvent::Typing {
                            conversation_id,
                            event: typing,
                        }));
                    }
                    Some(flare_proto::common::realtime_control_packet::Payload::Presence(
                        presence,
                    )) => {
                        self.bus
                            .publish(SdkEvent::Message(MessageEvent::PresenceChanged {
                                conversation_id,
                                event: presence,
                            }));
                    }
                    Some(
                        flare_proto::common::realtime_control_packet::Payload::TypingAggregate(
                            aggregate,
                        ),
                    ) => {
                        self.bus
                            .publish(SdkEvent::Message(MessageEvent::TypingAggregate {
                                conversation_id,
                                event: aggregate,
                            }));
                    }
                    Some(flare_proto::common::realtime_control_packet::Payload::Custom(data))
                        if data.r#type == USER_SETTINGS_CHANGED_TYPE =>
                    {
                        // 只认自己 sync 收件箱上的这一帧：网关丢弃客户端发往 `sync:` 收件箱的实时帧，
                        // 它只可能来自服务端。信号只给引擎，不作为扩展事件外发。
                        let user_id = self.current_user_id.read().await.clone();
                        if !user_id.is_empty() && conversation_id == format!("sync:{user_id}") {
                            self.notification_pipeline
                                .user_settings_changed(&data.payload);
                        }
                    }
                    Some(flare_proto::common::realtime_control_packet::Payload::Custom(data)) => {
                        self.bus.publish(SdkEvent::Extension(ExtensionEvent {
                            source: "realtime_control".to_string(),
                            event_type: data.r#type,
                            payload: data.payload,
                        }));
                    }
                    None => {}
                }
            }
            DownlinkPayload::SyncResp(resp) => {
                self.metrics.counter_with_labels(
                    "dispatcher.payload_total",
                    &[MetricLabel::new("type", "sync_response")],
                    1,
                );
                if let Some(h) = &self.sync_response_handler {
                    h.handle_sync_response(resp).await;
                }
            }
        }
        Ok(())
    }

    /// 把信封内的消息事件作为**一个批次**汇聚→落库→投递（一个 ReceivedBatch）。镜像 `dispatch_single_event`
    /// 的消息处理路径但批量化；任一步失败则忘记这些事件（解除去重）以便后续重投。
    /// 处理成功后发出 `delivery_due`（这一帧到达时定下的送达确认）；失败不发。
    async fn dispatch_inbound_message_event_batch(
        &self,
        messages: Vec<IMMessage>,
        reservations: Vec<EventReservation>,
        current_user_id: &str,
        delivery_due: Vec<(String, u64)>,
    ) {
        // Delivery-lag visibility (recv side): server create time vs local receipt. On a
        // co-located dev stack clock skew is ~0, so this approximates end-to-end delivery latency
        // and surfaces the "message arrives late" case that server logs cannot show. Uses the
        // newest created_at (smallest lag) so historical sync-backfill batches are not flagged.
        if let Some(newest_created_at) = messages.iter().map(|m| m.created_at).max()
            && newest_created_at > 0
        {
            let lag_ms = crate::shared::util::time::now_millis().saturating_sub(newest_created_at);
            if lag_ms > SLOW_DELIVERY_LOG_THRESHOLD_MS {
                warn!(
                    lag_ms,
                    count = messages.len(),
                    "slow delivery: inbound message batch lag exceeded threshold"
                );
            } else {
                tracing::debug!(
                    lag_ms,
                    count = messages.len(),
                    "inbound message batch delivery lag"
                );
            }
        }
        let batch = match self.prepare_inbound(current_user_id, messages).await {
            Ok(batch) => batch,
            Err(error) => {
                warn!(%error, "event envelope message batch converge failed");
                return;
            }
        };
        // 复用统一持久化管线（save_batch → 会话投影 → seq 补偿）；失败回滚去重键，
        // 事件下次重放（幂等）。
        if !self
            .persist_durable_batch(&batch.durable, current_user_id)
            .await
        {
            return;
        }
        self.notification_pipeline
            .finish_batch(batch.into_notifications(), current_user_id)
            .await;
        for reservation in reservations {
            reservation.complete();
        }
        self.confirm_delivered(delivery_due);
    }

    /// 分发单条 Event（从 EventEnvelope 或单条 Event 下行复用）。`delivery_due` 是这一帧到达时定下的
    /// 送达确认（只有消息事件有），消息处理成功后发出。
    pub(super) async fn dispatch_single_event(
        &self,
        ev: &flare_proto::common::Event,
        delivery_due: Vec<(String, u64)>,
    ) {
        let reservation = match self.event_deduper.claim(ev) {
            EventClaim::Applied => {
                self.confirm_delivered(delivery_due);
                return;
            }
            EventClaim::InFlight => return,
            EventClaim::New(reservation) => reservation,
        };
        if self.apply_single_event(ev).await {
            reservation.complete();
            if !matches!(&ev.payload, Some(EventPayload::Message(_))) {
                let conversation_id = Self::event_conversation_id(ev);
                if !conversation_id.is_empty() {
                    self.applied_event_seqs
                        .record(conversation_id, ev.conversation_seq)
                        .await;
                }
                self.maybe_trigger_event_waterline(ev).await;
            }
            self.confirm_delivered(delivery_due);
        }
    }

    async fn apply_single_event(&self, ev: &flare_proto::common::Event) -> bool {
        let mut messages: Vec<IMMessage> = Vec::new();
        if let Some(EventPayload::Message(m)) = &ev.payload {
            messages.push(IMMessage::new(m.clone()));
        }
        if !messages.is_empty() {
            let current_user_id = self.current_user_id.read().await.clone();
            let batch = match self.prepare_inbound(&current_user_id, messages).await {
                Ok(batch) => batch,
                Err(error) => {
                    warn!(%error, "single event message converge failed");
                    return false;
                }
            };
            if !self
                .persist_durable_batch(&batch.durable, &current_user_id)
                .await
            {
                return false;
            }
            self.notification_pipeline
                .finish_batch(batch.into_notifications(), &current_user_id)
                .await;
        }
        let conversation_id = Self::event_conversation_id(ev);
        if let Some(p) = &ev.payload {
            match p {
                EventPayload::Recall(recall) => {
                    if let Some(ref stores) = self.stores {
                        match stores
                            .messages
                            .update_status(&recall.server_msg_id, MessageStatus::Recalled as i32)
                            .await
                        {
                            Ok(0) => {
                                // 目标消息尚未落库（事件先于消息到达）：与 Edit/Reaction/Delete
                                // 的 NotFound 路径一致，回滚去重键保留事件可重放。
                                warn!(
                                    server_msg_id = %recall.server_msg_id,
                                    "Recall: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    server_msg_id = %recall.server_msg_id,
                                    "Recall: update_status failed; event will be retried"
                                );
                                return false;
                            }
                        }
                    }
                    let conversation_id = self
                        .message_operation_conversation_id(conversation_id, &recall.server_msg_id)
                        .await;
                    self.bus.publish(SdkEvent::Message(MessageEvent::Recalled {
                        conversation_id,
                        event: recall.clone(),
                    }));
                }
                EventPayload::Edit(edit) => {
                    let mut should_publish = true;
                    let Some(new_content) = edit.new_content.as_ref() else {
                        warn!(
                            server_msg_id = %edit.server_msg_id,
                            "Event Edit missing new_content; event will be retried"
                        );
                        return false;
                    };
                    if let Some(ref stores) = self.stores {
                        // Core never edits a plugin card nor turns a message into one; such an
                        // event is consumed without effect. The check needs the stored message:
                        // when it cannot be read the edit is not applied either, and the event is
                        // rolled back to be replayed by sync (as the replay path does).
                        let stored = match stores.messages.get(&edit.server_msg_id).await {
                            Ok(stored) => stored,
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    server_msg_id = %edit.server_msg_id,
                                    "Event Edit: the stored message could not be read; event will be retried"
                                );
                                return false;
                            }
                        };
                        if let Some(stored) = stored
                            && !crate::domain::inbound_edit_allowed(&stored, new_content)
                        {
                            warn!(
                                server_msg_id = %edit.server_msg_id,
                                "Event Edit touches a plugin card; ignored"
                            );
                            return true;
                        }
                        match stores
                            .messages
                            .apply_edit_event(
                                &edit.server_msg_id,
                                new_content.encode_to_vec(),
                                operation_seq(ev),
                            )
                            .await
                        {
                            Ok(crate::domain::EditApplyResult::Applied) => {}
                            Ok(crate::domain::EditApplyResult::IgnoredStale) => {
                                should_publish = false;
                            }
                            Ok(crate::domain::EditApplyResult::NotFound) => {
                                warn!(
                                    server_msg_id = %edit.server_msg_id,
                                    "Event Edit: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Err(e) => {
                                warn!(error = %e, server_msg_id = %edit.server_msg_id, "Event Edit apply_edit_event failed");
                                return false;
                            }
                        }
                    }
                    if should_publish {
                        self.bus.publish(SdkEvent::Message(MessageEvent::Edited {
                            conversation_id: conversation_id.to_string(),
                            server_msg_id: edit.server_msg_id.clone(),
                        }));
                    }
                }
                EventPayload::Reaction(reaction) => {
                    let mut should_publish = true;
                    if let Some(ref stores) = self.stores {
                        match stores
                            .messages
                            .apply_reaction_event(
                                conversation_id,
                                &reaction.server_msg_id,
                                &reaction.user_id,
                                &reaction.emoji,
                                reaction.action,
                                operation_seq(ev),
                            )
                            .await
                        {
                            Ok(crate::domain::OperationApplyResult::IgnoredStale) => {
                                should_publish = false;
                            }
                            Ok(crate::domain::OperationApplyResult::Applied) => {}
                            Ok(crate::domain::OperationApplyResult::NotFound) => {
                                warn!(
                                    server_msg_id = %reaction.server_msg_id,
                                    "Event Reaction: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Err(error) => {
                                warn!(error = %error, server_msg_id = %reaction.server_msg_id, "Event Reaction apply failed");
                                return false;
                            }
                        }
                    }
                    if should_publish {
                        self.bus
                            .publish(SdkEvent::Message(MessageEvent::ReactionChanged {
                                conversation_id: conversation_id.to_string(),
                                server_msg_id: reaction.server_msg_id.clone(),
                                user_id: reaction.user_id.clone(),
                                emoji: reaction.emoji.clone(),
                                action: reaction.action,
                            }));
                    }
                }
                EventPayload::Delete(delete) => {
                    if self.should_apply_delete_for_current_user(delete).await {
                        let mut should_publish = true;
                        if let Some(ref stores) = self.stores {
                            match stores
                                .messages
                                .apply_delete_event(&delete.server_msg_id, operation_seq(ev))
                                .await
                            {
                                Ok(crate::domain::OperationApplyResult::IgnoredStale) => {
                                    should_publish = false;
                                }
                                Ok(crate::domain::OperationApplyResult::Applied) => {}
                                Ok(crate::domain::OperationApplyResult::NotFound) => {
                                    warn!(
                                        server_msg_id = %delete.server_msg_id,
                                        "Event Delete: no local row matched; event will be retried after message sync"
                                    );
                                    return false;
                                }
                                Err(error) => {
                                    warn!(error = %error, server_msg_id = %delete.server_msg_id, "Event Delete apply failed");
                                    return false;
                                }
                            }
                        }
                        if should_publish {
                            self.bus.publish(SdkEvent::Message(MessageEvent::Deleted {
                                conversation_id: conversation_id.to_string(),
                                event: delete.clone(),
                            }));
                            self.bus.publish(SdkEvent::Extension(ExtensionEvent {
                                source: "event".to_string(),
                                event_type: "message_delete".to_string(),
                                payload: delete.encode_to_vec(),
                            }));
                        }
                    }
                }
                EventPayload::Read(read) => {
                    if let Some(ref stores) = self.stores {
                        let current_user_id = self.current_user_id.read().await.clone();
                        if !current_user_id.is_empty()
                            && read.user_id == current_user_id
                            && read.read_seq > 0
                            && let Err(error) =
                                crate::application::usecases::SyncApplyUseCase::apply_remote_read(
                                    stores,
                                    &current_user_id,
                                    conversation_id,
                                    read.read_seq,
                                    None,
                                )
                                .await
                        {
                            warn!(%error, "failed to confirm own read watermark");
                            return false;
                        }
                        // 对方已读回执：将「自己发送且 seq<=read_seq」的消息落库为已读，
                        // 避免仅靠前端内存态导致会话切换/重启后双对号丢失。
                        if !current_user_id.is_empty()
                            && !read.user_id.is_empty()
                            && read.user_id != current_user_id
                            && read.read_seq > 0
                            && let Err(error) = stores
                                .messages
                                .mark_outgoing_read_upto_seq(
                                    conversation_id,
                                    &current_user_id,
                                    read.read_seq,
                                )
                                .await
                        {
                            warn!(%error, "Event Read: local read receipt update failed");
                            return false;
                        }
                    }
                    self.bus
                        .publish(SdkEvent::Message(MessageEvent::ReadReceipt {
                            conversation_id: conversation_id.to_string(),
                            event: read.clone(),
                        }));
                }
                EventPayload::RetentionScheduled(retention_scheduled) => {
                    let mut should_publish = true;
                    let (Some(policy), Some(state)) = (
                        retention_scheduled.policy.as_ref(),
                        retention_scheduled.state.as_ref(),
                    ) else {
                        warn!(
                            message_id = %retention_scheduled.server_msg_id,
                            "Event RetentionScheduled missing policy/state; event will be retried"
                        );
                        return false;
                    };
                    if let Some(ref stores) = self.stores {
                        match stores
                            .messages
                            .apply_retention_scheduled_event(
                                &retention_scheduled.server_msg_id,
                                policy,
                                state,
                                retention_scheduled.scheduled_at,
                                operation_seq(ev),
                            )
                            .await
                        {
                            Ok(crate::domain::OperationApplyResult::IgnoredStale) => {
                                should_publish = false;
                            }
                            Ok(crate::domain::OperationApplyResult::Applied) => {}
                            Ok(crate::domain::OperationApplyResult::NotFound) => {
                                warn!(
                                    message_id = %retention_scheduled.server_msg_id,
                                    "Event RetentionScheduled: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Err(error) => {
                                warn!(error = %error, message_id = %retention_scheduled.server_msg_id, "Event RetentionScheduled apply failed");
                                return false;
                            }
                        }
                    }
                    if should_publish {
                        self.bus
                            .publish(SdkEvent::Message(MessageEvent::RetentionScheduled {
                                conversation_id: conversation_id.to_string(),
                                event: retention_scheduled.clone(),
                            }));
                    }
                }
                EventPayload::RetentionExpired(retention_expired) => {
                    let mut should_publish = true;
                    let Some(state) = retention_expired.state.as_ref() else {
                        warn!(
                            message_id = %retention_expired.server_msg_id,
                            "Event RetentionExpired missing state; event will be retried"
                        );
                        return false;
                    };
                    if let Some(ref stores) = self.stores {
                        match stores
                            .messages
                            .apply_retention_expired_event(
                                &retention_expired.server_msg_id,
                                state,
                                retention_expired.expired_at,
                                operation_seq(ev),
                            )
                            .await
                        {
                            Ok(crate::domain::OperationApplyResult::IgnoredStale) => {
                                should_publish = false;
                            }
                            Ok(crate::domain::OperationApplyResult::Applied) => {}
                            Ok(crate::domain::OperationApplyResult::NotFound) => {
                                warn!(
                                    message_id = %retention_expired.server_msg_id,
                                    "Event RetentionExpired: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Err(error) => {
                                warn!(error = %error, message_id = %retention_expired.server_msg_id, "Event RetentionExpired apply failed");
                                return false;
                            }
                        }
                    }
                    if should_publish {
                        self.bus
                            .publish(SdkEvent::Message(MessageEvent::RetentionExpired {
                                conversation_id: conversation_id.to_string(),
                                event: retention_expired.clone(),
                            }));
                    }
                }
                EventPayload::RetentionPurged(retention_purged) => {
                    let mut should_publish = true;
                    let Some(state) = retention_purged.state.as_ref() else {
                        warn!(
                            message_id = %retention_purged.server_msg_id,
                            "Event RetentionPurged missing state; event will be retried"
                        );
                        return false;
                    };
                    if let Some(ref stores) = self.stores {
                        match stores
                            .messages
                            .apply_retention_purged_event(
                                &retention_purged.server_msg_id,
                                state,
                                retention_purged.purged_at,
                                operation_seq(ev),
                            )
                            .await
                        {
                            Ok(crate::domain::OperationApplyResult::IgnoredStale) => {
                                should_publish = false;
                            }
                            Ok(crate::domain::OperationApplyResult::Applied) => {}
                            Ok(crate::domain::OperationApplyResult::NotFound) => {
                                warn!(
                                    message_id = %retention_purged.server_msg_id,
                                    "Event RetentionPurged: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Err(error) => {
                                warn!(error = %error, message_id = %retention_purged.server_msg_id, "Event RetentionPurged apply failed");
                                return false;
                            }
                        }
                    }
                    if should_publish {
                        self.bus
                            .publish(SdkEvent::Message(MessageEvent::RetentionPurged {
                                conversation_id: conversation_id.to_string(),
                                event: retention_purged.clone(),
                            }));
                    }
                }
                EventPayload::Pin(pin) => {
                    let mut should_publish = true;
                    if let Some(ref stores) = self.stores {
                        match stores
                            .messages
                            .apply_pin_event(&pin.server_msg_id, true, operation_seq(ev))
                            .await
                        {
                            Ok(crate::domain::OperationApplyResult::IgnoredStale) => {
                                should_publish = false;
                            }
                            Ok(crate::domain::OperationApplyResult::Applied) => {}
                            Ok(crate::domain::OperationApplyResult::NotFound) => {
                                warn!(
                                    server_msg_id = %pin.server_msg_id,
                                    "Event Pin: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Err(error) => {
                                warn!(error = %error, server_msg_id = %pin.server_msg_id, "Event Pin apply failed");
                                return false;
                            }
                        }
                    }
                    if should_publish {
                        self.bus.publish(SdkEvent::Message(MessageEvent::Pinned {
                            conversation_id: conversation_id.to_string(),
                            event: pin.clone(),
                        }));
                    }
                }
                EventPayload::Unpin(unpin) => {
                    let mut should_publish = true;
                    if let Some(ref stores) = self.stores {
                        match stores
                            .messages
                            .apply_pin_event(&unpin.server_msg_id, false, operation_seq(ev))
                            .await
                        {
                            Ok(crate::domain::OperationApplyResult::IgnoredStale) => {
                                should_publish = false;
                            }
                            Ok(crate::domain::OperationApplyResult::Applied) => {}
                            Ok(crate::domain::OperationApplyResult::NotFound) => {
                                warn!(
                                    server_msg_id = %unpin.server_msg_id,
                                    "Event Unpin: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Err(error) => {
                                warn!(error = %error, server_msg_id = %unpin.server_msg_id, "Event Unpin apply failed");
                                return false;
                            }
                        }
                    }
                    if should_publish {
                        self.bus.publish(SdkEvent::Message(MessageEvent::Unpinned {
                            conversation_id: conversation_id.to_string(),
                            event: unpin.clone(),
                        }));
                    }
                }
                EventPayload::Mark(mark) => {
                    let mut should_publish = true;
                    if let Some(ref stores) = self.stores {
                        match stores
                            .messages
                            .apply_mark_event(
                                &mark.server_msg_id,
                                mark.mark_type,
                                if mark.color.trim().is_empty() {
                                    None
                                } else {
                                    Some(mark.color.as_str())
                                },
                                true,
                                operation_seq(ev),
                            )
                            .await
                        {
                            Ok(crate::domain::OperationApplyResult::IgnoredStale) => {
                                should_publish = false;
                            }
                            Ok(crate::domain::OperationApplyResult::Applied) => {}
                            Ok(crate::domain::OperationApplyResult::NotFound) => {
                                warn!(
                                    server_msg_id = %mark.server_msg_id,
                                    "Event Mark: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Err(error) => {
                                warn!(error = %error, server_msg_id = %mark.server_msg_id, "Event Mark apply failed");
                                return false;
                            }
                        }
                    }
                    if should_publish {
                        self.bus.publish(SdkEvent::Message(MessageEvent::Marked {
                            conversation_id: conversation_id.to_string(),
                            event: mark.clone(),
                        }));
                    }
                }
                EventPayload::Unmark(unmark) => {
                    let mut should_publish = true;
                    if let Some(ref stores) = self.stores {
                        match stores
                            .messages
                            .apply_mark_event(
                                &unmark.server_msg_id,
                                unmark.mark_type,
                                None,
                                false,
                                operation_seq(ev),
                            )
                            .await
                        {
                            Ok(crate::domain::OperationApplyResult::IgnoredStale) => {
                                should_publish = false;
                            }
                            Ok(crate::domain::OperationApplyResult::Applied) => {}
                            Ok(crate::domain::OperationApplyResult::NotFound) => {
                                warn!(
                                    server_msg_id = %unmark.server_msg_id,
                                    "Event Unmark: no local row matched; event will be retried after message sync"
                                );
                                return false;
                            }
                            Err(error) => {
                                warn!(error = %error, server_msg_id = %unmark.server_msg_id, "Event Unmark apply failed");
                                return false;
                            }
                        }
                    }
                    if should_publish {
                        self.bus.publish(SdkEvent::Message(MessageEvent::Unmarked {
                            conversation_id: conversation_id.to_string(),
                            event: unmark.clone(),
                        }));
                    }
                }
                EventPayload::PluginState(plugin_state) => {
                    use crate::application::services::{
                        PluginSharedStateOutcome, apply_plugin_shared_state,
                    };
                    let Some(ref stores) = self.stores else {
                        return false;
                    };
                    match apply_plugin_shared_state(stores.messages.as_ref(), plugin_state).await {
                        Ok(PluginSharedStateOutcome::Applied {
                            conversation_id: stored_conversation_id,
                            message_id,
                            version,
                        }) => {
                            let conversation_id = if conversation_id.is_empty() {
                                stored_conversation_id
                            } else {
                                conversation_id.to_string()
                            };
                            self.bus
                                .publish(SdkEvent::Message(MessageEvent::PluginStateChanged {
                                    conversation_id,
                                    message_id,
                                    version,
                                }));
                        }
                        Ok(PluginSharedStateOutcome::Ignored)
                        | Ok(PluginSharedStateOutcome::Rejected) => {}
                        Ok(PluginSharedStateOutcome::NotFound) => {
                            warn!(
                                server_msg_id = %plugin_state.server_msg_id,
                                "Event PluginState: no local row matched; event will be retried after message sync"
                            );
                            return false;
                        }
                        Err(error) => {
                            warn!(error = %error, server_msg_id = %plugin_state.server_msg_id, "Event PluginState apply failed");
                            return false;
                        }
                    }
                }
                EventPayload::Custom(custom) => {
                    self.bus.publish(SdkEvent::Message(MessageEvent::Custom {
                        conversation_id: conversation_id.to_string(),
                        event: custom.clone(),
                    }));
                }
                EventPayload::Conversation(conversation) => {
                    if conversation.history_cleared_through_seq.is_some()
                        && let Some(sync) = self.session_sync.clone()
                    {
                        let user_id = self.current_user_id.read().await.clone();
                        if !user_id.is_empty() {
                            spawn_background(async move {
                                if let Err(error) =
                                    sync.request_history_visibility_sync(&user_id).await
                                {
                                    warn!(%error, "history visibility sync failed after clear hint");
                                }
                            });
                        }
                    }
                    if conversation.unread_count > 0 {
                        self.maybe_trigger_waterline_pull(
                            "conversation_update",
                            "conversation.waterline",
                            Some(conversation_id),
                            &HashMap::new(),
                        )
                        .await;
                    }
                    self.bus
                        .publish(SdkEvent::Conversation(ConversationEvent::Updated {
                            conversation_id: conversation_id.to_string(),
                        }));
                }
                EventPayload::ConversationDelete(_) => {
                    if let Some(stores) = self.stores.as_ref()
                        && let Err(error) = stores.conversations.delete(conversation_id).await
                    {
                        warn!(
                            conversation_id = %conversation_id,
                            error = %error,
                            "ConversationDelete push: local conversation purge failed"
                        );
                        return false;
                    }
                    self.bus
                        .publish(SdkEvent::Conversation(ConversationEvent::Deleted {
                            conversation_id: conversation_id.to_string(),
                        }));
                }
                _ => {}
            }
        }
        true
    }
}
