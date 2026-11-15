//! NotifyCenter 对已经发出的通知的修正（会话读了撤掉、展示着的消息撤回就替换、回到前台对账）、
//! 会话的开始与结束，以及角标。

use super::*;

impl NotifyCenter {
    // ---------------------------------------------------------------------------------------
    // 修正：已读、撤回、回到前台
    // ---------------------------------------------------------------------------------------

    pub(super) fn start_watcher(&self) {
        let mut conversations = self
            .bus
            .subscribe_filter(EventFilter::Kind(SdkEventKind::Conversation));
        let mut recalls = self
            .bus
            .subscribe_filter(EventFilter::Type(SdkEventType::Message(
                MessageEventType::Recalled,
            )));
        let center = self.me.clone();
        let task = spawn_background_task(async move {
            loop {
                let event = tokio::select! {
                    event = conversations.recv() => event,
                    event = recalls.recv() => event,
                };
                let Ok(event) = event else {
                    break;
                };
                let Some(center) = center.upgrade() else {
                    break;
                };
                center.on_event(event).await;
            }
        });
        *self
            .watcher
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(task);
    }

    async fn on_event(&self, event: SdkEvent) {
        if self.ended() {
            return;
        }
        match event {
            SdkEvent::Message(MessageEvent::Recalled {
                conversation_id,
                event,
            }) => {
                self.on_recalled(&conversation_id, &event.server_msg_id)
                    .await
            }
            SdkEvent::Conversation(ConversationEvent::UnreadCountChanged {
                conversation_id,
                ..
            }) => {
                self.check_read(vec![conversation_id]).await;
                self.schedule_badge(false);
            }
            // 会话的标题、免打扰、仅 @我、先后可能变了：通知快照随之重写（去抖）。
            SdkEvent::Conversation(
                ConversationEvent::Updated { conversation_id }
                | ConversationEvent::Created { conversation_id },
            ) => {
                self.check_read(vec![conversation_id]).await;
                self.schedule_badge(false);
                self.snapshot_changed();
            }
            SdkEvent::Conversation(ConversationEvent::Synced { conversation_ids }) => {
                self.check_read(conversation_ids).await;
                self.schedule_badge(false);
                self.snapshot_changed();
            }
            SdkEvent::Conversation(ConversationEvent::Deleted { conversation_id }) => {
                self.forget(&conversation_id);
                self.schedule_badge(false);
                self.snapshot_changed();
            }
            // 事件队列溢出：可能漏了已读，全部对一遍。
            SdkEvent::Sync(SyncNotify::ResyncNeeded { .. }) => {
                let watched = self.lock_state().slots.keys().cloned().collect();
                self.check_read(watched).await;
                self.schedule_badge(false);
            }
            _ => {}
        }
    }

    /// 这些会话里本中心手上有通知状态的，已读游标越过见过的最大 seq 就算读完：关槽位、撤通知。
    pub(super) async fn check_read(&self, conversation_ids: Vec<String>) {
        let watched: Vec<(String, u64)> = {
            let state = self.lock_state();
            conversation_ids
                .into_iter()
                .filter_map(|id| {
                    let slot = state.slots.get(&id)?;
                    (!slot.idle()).then_some((id, slot.max_seq))
                })
                .collect()
        };
        if watched.is_empty() {
            return;
        }
        let ids: Vec<String> = watched.iter().map(|(id, _)| id.clone()).collect();
        let rows = match self.stores.conversations.get_many(&ids).await {
            Ok(rows) => rows,
            Err(error) => {
                tracing::debug!(%error, "reading read state for notifications failed");
                return;
            }
        };
        for (conversation_id, max_seq) in watched {
            let Some(row) = rows
                .iter()
                .find(|row| row.conversation_id == conversation_id)
            else {
                continue;
            };
            if max_seq > 0 && row.last_read_seq >= max_seq {
                self.close_read(&conversation_id, row.last_read_seq);
            }
        }
    }

    /// 会话读到了 `read_seq`：关槽位、取消尾部刷新，展示着的通知撤掉（台账判定没有更新的通知时）。
    fn close_read(&self, conversation_id: &str, read_seq: u64) {
        let now = now_millis() as i64;
        let collapse_id = {
            let mut state = self.lock_state();
            let CenterState { device, slots, .. } = &mut *state;
            let Some(slot) = slots.get_mut(conversation_id) else {
                return;
            };
            let outcome = self.gate.on_read(now, &mut slot.gate, device);
            slot.tail_due_ms = None;
            slot.latest = None;
            slot.in_app_count = 0;
            let shown = slot.shown.take().is_some() || outcome.clear;
            let collapse_id = slot.collapse_id.clone();
            if slot.idle() {
                slots.remove(conversation_id);
            }
            shown.then_some(collapse_id)
        };
        if let Some(collapse_id) = collapse_id
            && self.alerts.claim_clear(&collapse_id, read_seq).is_ok()
        {
            self.publish_clear(vec![collapse_id], false);
        }
    }

    /// 会话删了：它的通知撤掉，状态不再留。
    fn forget(&self, conversation_id: &str) {
        let removed = self.lock_state().slots.remove(conversation_id);
        if let Some(slot) = removed
            && slot.shown.is_some()
        {
            self.publish_clear(vec![slot.collapse_id], false);
        }
    }

    /// 展示着的那条被撤回：用同一合并键静默替换成「撤回了一条消息」，计数减一。
    pub(super) async fn on_recalled(&self, conversation_id: &str, server_msg_id: &str) {
        let replaced = {
            let mut state = self.lock_state();
            let Some(slot) = state.slots.get_mut(conversation_id) else {
                return;
            };
            if slot
                .latest
                .as_ref()
                .is_some_and(|latest| latest.message.server_id == server_msg_id)
            {
                slot.latest = None;
            }
            match slot.shown.as_mut() {
                Some(shown) if !server_msg_id.is_empty() && shown.message_id == server_msg_id => {
                    shown.count = shown.count.saturating_sub(1).max(1);
                    Some((slot.collapse_id.clone(), shown.clone()))
                }
                _ => None,
            }
        };
        let Some((collapse_id, shown)) = replaced else {
            return;
        };
        // 应用内横幅只在前台还在；系统通知只在本进程仍能自己提示时替换（否则归推送去修正）。
        if self.reach.alert_surface() != Some(shown.surface)
            || self
                .alerts
                .claim_alert(&collapse_id, shown.seq, true)
                .is_err()
        {
            return;
        }
        let preview = self
            .cached_settings()
            .map(|settings| settings.preview)
            .unwrap_or_default();
        let text =
            AlertCopy::recalled(shown.title.as_deref(), preview, shown.count).render(self.lang);
        self.publish_presentation(NotificationPresentation {
            thread_id: collapse_id.clone(),
            collapse_id,
            conversation_id: conversation_id.to_owned(),
            message_id: shown.message_id,
            seq: shown.seq,
            title: text.title,
            body: text.body,
            count: shown.count,
            sound: false,
            channel: "quiet".to_owned(),
            surface: shown.surface.as_str().to_owned(),
        });
    }

    /// 宿主打开了这个会话：它的应用内横幅从头计数。
    pub(crate) fn focus_changed(&self, conversation_id: Option<&str>) {
        let Some(conversation_id) = conversation_id else {
            return;
        };
        if let Some(slot) = self.lock_state().slots.get_mut(conversation_id) {
            slot.in_app_count = 0;
        }
    }

    /// 应用回到前台：闸门换代（槽位全部作废、退出摘要），撤掉摘要通知与已读会话的系统通知，校准一次
    /// 角标。
    pub(crate) async fn on_foreground(&self) {
        if self.ended() {
            return;
        }
        let digest_shown = {
            let mut state = self.lock_state();
            let CenterState {
                device,
                slots,
                digest_shown,
                ..
            } = &mut *state;
            self.gate.on_active(device);
            for slot in slots.values_mut() {
                slot.tail_due_ms = None;
                slot.latest = None;
            }
            std::mem::take(digest_shown)
        };
        if digest_shown {
            self.publish_clear(vec![DIGEST_COLLAPSE_ID.to_owned()], false);
        }
        let conversations = match timeout(LIST_CAP, self.stores.conversations.list()).await {
            Ok(Ok(conversations)) => conversations,
            Ok(Err(error)) => {
                tracing::debug!(%error, "reading conversations on foreground failed");
                self.schedule_badge(true);
                return;
            }
            Err(_) => {
                self.schedule_badge(true);
                return;
            }
        };
        self.clear_read_alerts(&conversations);
        self.publish_badge(badge_of(&conversations), true);
    }

    /// 提示台账里还没撤的通知，会话已读游标盖过了的，一次撤掉。推送通道展示过的也在台账里。
    fn clear_read_alerts(&self, conversations: &[Conversation]) {
        let uncleared: HashMap<String, u64> = self.alerts.uncleared().into_iter().collect();
        if uncleared.is_empty() {
            return;
        }
        let mut cleared = Vec::new();
        for conversation in conversations {
            let Some(collapse_id) = self.alerts.collapse_id(&conversation.conversation_id) else {
                return;
            };
            let Some(alerted) = uncleared.get(&collapse_id) else {
                continue;
            };
            let read_seq = conversation.last_read_seq;
            if read_seq >= *alerted && self.alerts.claim_clear(&collapse_id, read_seq).is_ok() {
                cleared.push((conversation.conversation_id.clone(), collapse_id));
            }
        }
        if cleared.is_empty() {
            return;
        }
        {
            let mut state = self.lock_state();
            for (conversation_id, _) in &cleared {
                if let Some(slot) = state.slots.get_mut(conversation_id) {
                    slot.shown = None;
                    slot.in_app_count = 0;
                }
            }
        }
        self.publish_clear(
            cleared
                .into_iter()
                .map(|(_, collapse_id)| collapse_id)
                .collect(),
            false,
        );
    }

    // ---------------------------------------------------------------------------------------
    // 会话的开始与结束
    // ---------------------------------------------------------------------------------------

    /// 链路上线：读一次用户级通知设置；登录后的第一次上线另外报一次角标。
    pub(crate) fn on_link_up(&self, first: bool) {
        let center = self.me.clone();
        spawn_background(async move {
            if let Some(center) = center.upgrade()
                && let Err(error) = center.refresh_settings().await
            {
                tracing::debug!(%error, "reading user notification settings failed; using the last known");
            }
        });
        if first {
            self.schedule_badge(true);
        }
    }

    /// 会话结束（登出、断开）：本账号的通知全部撤掉（推送通道展示的也算），角标归零，给通知扩展的
    /// 快照删掉。只做一次。
    pub(crate) fn end_session(&self) {
        if self.ended.swap(true, Ordering::AcqRel) {
            return;
        }
        #[cfg(not(target_arch = "wasm32"))]
        self.delete_snapshot();
        *self.lock_state() = CenterState::default();
        *self
            .badge
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = BadgeState {
            last: Some(0),
            ..BadgeState::default()
        };
        self.publish_clear(Vec::new(), true);
        self.bus
            .publish(SdkEvent::Notification(NotificationEvent::Badge {
                count: 0,
            }));
    }

    // ---------------------------------------------------------------------------------------
    // 角标
    // ---------------------------------------------------------------------------------------

    /// 未读总数可能变了：等一小会儿再读（一串变化只读一次）。`force` 时数值没变也报。
    fn schedule_badge(&self, force: bool) {
        {
            let mut badge = self
                .badge
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            badge.force |= force;
            if badge.scheduled {
                return;
            }
            badge.scheduled = true;
        }
        let center = self.me.clone();
        spawn_background(async move {
            delay(BADGE_DEBOUNCE).await;
            if let Some(center) = center.upgrade() {
                center.flush_badge().await;
            }
        });
    }

    async fn flush_badge(&self) {
        if self.ended() {
            return;
        }
        let force = {
            let mut badge = self
                .badge
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            badge.scheduled = false;
            std::mem::take(&mut badge.force)
        };
        match timeout(LIST_CAP, self.stores.conversations.list()).await {
            Ok(Ok(conversations)) => self.publish_badge(badge_of(&conversations), force),
            Ok(Err(error)) => {
                tracing::debug!(%error, "reading the unread total for the badge failed")
            }
            Err(_) => tracing::debug!("reading the unread total for the badge timed out"),
        }
    }

    pub(super) fn publish_badge(&self, count: u32, force: bool) {
        let changed = {
            let mut badge = self
                .badge
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let changed = badge.last != Some(count);
            badge.last = Some(count);
            changed
        };
        if changed || force {
            self.bus
                .publish(SdkEvent::Notification(NotificationEvent::Badge { count }));
        }
    }
}
