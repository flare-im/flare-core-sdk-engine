use std::sync::Arc;

use crate::application::adapters::SyncProtocolAdapter;
use crate::application::sync_task::ConversationUserSettingsPatch;
use crate::application::{ConversationLocalLifecycle, LocalConversationVisibility};
use crate::domain::{
    ConversationIdentityService, ConversationReadService, ConversationStore, SyncCursorStore,
};
use crate::kernel::{CurrentUserIdStore, SessionSyncRunner};
use crate::model::conversation::ConversationType;
use crate::model::{
    Conversation, ConversationParticipant, mark_settings_dirty, user_settings_version,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

pub struct ConversationCommandUseCase {
    store: Arc<dyn ConversationStore>,
    cursors: Option<Arc<dyn SyncCursorStore>>,
    current_user_id: CurrentUserIdStore,
    identity_service: ConversationIdentityService,
    read_service: ConversationReadService,
    settings_sync: Option<Arc<SyncProtocolAdapter>>,
    read_ack_sync: Option<Arc<dyn SessionSyncRunner>>,
}

impl ConversationCommandUseCase {
    pub fn new(
        store: Arc<dyn ConversationStore>,
        current_user_id: CurrentUserIdStore,
        settings_sync: Option<Arc<SyncProtocolAdapter>>,
        cursors: Option<Arc<dyn SyncCursorStore>>,
    ) -> Self {
        Self {
            store,
            cursors,
            current_user_id,
            identity_service: ConversationIdentityService,
            read_service: ConversationReadService,
            read_ack_sync: settings_sync
                .as_ref()
                .map(|sync| Arc::clone(sync) as Arc<dyn SessionSyncRunner>),
            settings_sync,
        }
    }

    async fn after_user_settings_write(
        &self,
        conversation_id: &str,
        patch: ConversationUserSettingsPatch,
    ) -> Result<()> {
        let Some(mut conversation) = self.store.get(conversation_id).await? else {
            return Ok(());
        };
        mark_settings_dirty(&mut conversation);
        self.store.save_batch(&[conversation.clone()]).await?;
        if let Some(sync) = &self.settings_sync {
            let base = user_settings_version(&conversation);
            sync.push_conversation_user_settings(conversation_id, base, patch)
                .await?;
        }
        Ok(())
    }

    pub async fn current_user_id(&self) -> Result<String> {
        let uid = self.current_user_id.read().await.trim().to_string();
        if uid.is_empty() {
            return Err(FlareError::localized(ErrorCode::NotConnected, "未连接"));
        }
        Ok(uid)
    }

    pub async fn get_one(
        &self,
        source_id: &str,
        conversation_type: &ConversationType,
        save: bool,
    ) -> Result<Conversation> {
        let user_id = self.current_user_id().await?;
        let conversation_id = self.identity_service.resolve_conversation_id(
            &user_id,
            source_id,
            conversation_type,
        )?;
        let existing = self.store.get(&conversation_id).await?;
        let (mut conversation, mut needs_persist) = self.identity_service.merge_or_create(
            existing,
            conversation_id,
            &user_id,
            source_id,
            conversation_type,
        );
        // 单聊:把会话连同**双方**参与者一次性交服务端建立(幂等),确保对端成为可投递参与者。
        // 否则服务端会话只含先动作的一方,首发方的消息读扩散时跳过对端 → 单聊单向不达(对端收不到)。
        // best-effort,成功后打 single_server_established 标记避免重复 RPC;失败回退"首条消息携带成员"兜底建会话。
        if *conversation_type == ConversationType::Single {
            let already = conversation
                .ext
                .get("single_server_established")
                .map(|v| v == "1")
                .unwrap_or(false);
            if !already && let Some(sync) = &self.settings_sync {
                let mut members = vec![user_id.clone(), source_id.trim().to_string()];
                members.retain(|m| !m.is_empty());
                members.sort();
                members.dedup();
                if members.len() == 2 {
                    match sync
                        .ensure_conversation(
                            &conversation.conversation_id,
                            ConversationType::Single.to_proto_int(),
                            ConversationType::Single.as_str(),
                            &conversation.channel_id,
                            members,
                        )
                        .await
                    {
                        Ok(true) => {
                            conversation
                                .ext
                                .insert("single_server_established".to_string(), "1".to_string());
                            needs_persist = true;
                        }
                        Ok(false) => tracing::warn!(
                            conversation_id = %conversation.conversation_id,
                            "single ensure_conversation returned not-ok; falling back to message-roster establishment"
                        ),
                        Err(error) => tracing::warn!(
                            conversation_id = %conversation.conversation_id,
                            %error,
                            "single ensure_conversation rpc failed; falling back to message-roster establishment"
                        ),
                    }
                }
            }
        }
        if save && needs_persist {
            self.store
                .save_batch(std::slice::from_ref(&conversation))
                .await?;
        }
        Ok(conversation)
    }

    pub async fn get_group_by_user_ids(
        &self,
        user_ids: &[String],
        display_name: Option<&str>,
    ) -> Result<Conversation> {
        let current_user_id = self.current_user_id().await?;
        let mut members = user_ids
            .iter()
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty())
            .collect::<Vec<_>>();
        members.push(current_user_id.clone());
        members.sort();
        members.dedup();
        if members.len() < 2 {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "群聊至少需要 2 个成员",
            ));
        }

        let group_key = format!("users:{}", members.join(","));
        let conversation_id = self.identity_service.resolve_conversation_id(
            "",
            &group_key,
            &ConversationType::Group,
        )?;
        let existing = self.store.get(&conversation_id).await?;
        let (mut conversation, _) = self.identity_service.merge_or_create(
            existing,
            conversation_id,
            &current_user_id,
            &group_key,
            &ConversationType::Group,
        );
        // channel_id 绝不存整张成员表:去重身份已由 conversation_id(=hash(group_key))承担,成员在
        // participants/ext。把 "users:<全体成员>" 塞 channel_id 会让每条消息 channel_id 膨胀到 O(成员)
        // (穿 WAL/NATS/投递),大群直接压垮管线。ad-hoc 用户集合群无外部频道名,channel_id 取恒定大小的
        // conversation_id(merge_or_create 默认会把 source_id=group_key 当 channel_id,这里覆盖回收)。
        conversation.channel_id = conversation.conversation_id.clone();
        // 没起名的成员集合群名字列留空：标题在读取时用成员名拼接（resolve_conversation_title）。
        let name = display_name.unwrap_or(conversation.display_name.as_str());
        conversation.display_name = crate::domain::business_name(
            name,
            &[
                &group_key,
                &conversation.channel_id,
                &conversation.conversation_id,
            ],
        );
        conversation.members_count = members.len() as u32;
        let participants = members
            .iter()
            .map(|user_id| ConversationParticipant {
                user_id: user_id.clone(),
                roles: if user_id == &current_user_id {
                    vec!["owner".to_string()]
                } else {
                    vec!["member".to_string()]
                },
                ..Default::default()
            })
            .collect::<Vec<_>>();
        conversation.participant_version = participants.len() as u64;
        conversation.member_preview = participants.iter().take(10).cloned().collect();
        conversation.participants = participants;
        conversation
            .ext
            .insert("group_member_ids".to_string(), members.join(","));
        conversation
            .ext
            .insert("group_source".to_string(), "user_ids".to_string());

        // 显式建群：把整张成员表一次性交服务端建群，成功后消息**永不携带成员表**（超大群建群不再受 NATS
        // 单消息上限约束，且不把成员表落进消息行）。best-effort：失败则保留"首条消息携带成员表"的兜底建群。
        if let Some(sync) = &self.settings_sync {
            let business_type = ConversationType::Group.as_str().to_string();
            match sync
                .ensure_conversation(
                    &conversation.conversation_id,
                    ConversationType::Group.to_proto_int(),
                    &business_type,
                    &conversation.channel_id,
                    members.clone(),
                )
                .await
            {
                Ok(true) => {
                    conversation
                        .ext
                        .insert("group_server_established".to_string(), "1".to_string());
                }
                Ok(false) => {
                    tracing::warn!(
                        conversation_id = %conversation.conversation_id,
                        "server ensure_conversation returned not-ok; falling back to message-roster establishment"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        conversation_id = %conversation.conversation_id,
                        %error,
                        "ensure_conversation rpc failed; falling back to message-roster establishment"
                    );
                }
            }
        }

        self.store.save_batch(&[conversation.clone()]).await?;
        Ok(conversation)
    }

    pub async fn mark_read(&self, conversation_id: &str, read_seq: u64) -> Result<u32> {
        if read_seq == 0 {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "read_seq must be greater than 0",
            ));
        }
        let Some(current) = self.store.get(conversation_id).await? else {
            return Ok(0);
        };
        let local_max_seq = self
            .store
            .get_local_max_seq(conversation_id)
            .await
            .unwrap_or(current.max_seq);
        let decision = self
            .read_service
            .plan_mark_read(&current, local_max_seq, read_seq);

        if decision.next_read_seq > 0
            && let Some(sync) = &self.read_ack_sync
        {
            sync.send_read_ack(conversation_id, decision.next_read_seq)
                .await?;
        }

        self.store
            .update_unread(
                conversation_id,
                decision.unread_count,
                decision.next_read_seq,
            )
            .await?;

        let current_user_id = self.current_user_id.read().await.trim().to_string();
        if decision.should_recompute_local_unread && !current_user_id.is_empty() {
            let _ = self
                .store
                .recompute_unread_for_user(conversation_id, &current_user_id)
                .await;
            if let Some(updated) = self.store.get(conversation_id).await? {
                return Ok(updated.unread_count);
            }
        }
        Ok(decision.unread_count)
    }

    pub async fn delete(&self, conversation_id: &str) -> Result<()> {
        self.current_user_id().await?;
        self.store.delete(conversation_id).await
    }

    pub async fn set_pinned(&self, conversation_id: &str, pinned: bool) -> Result<()> {
        self.current_user_id().await?;
        self.store.set_pinned(conversation_id, pinned).await?;
        self.after_user_settings_write(
            conversation_id,
            ConversationUserSettingsPatch {
                is_pinned: Some(pinned),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn set_muted(&self, conversation_id: &str, muted: bool) -> Result<()> {
        self.current_user_id().await?;
        self.store.set_muted(conversation_id, muted).await?;
        self.after_user_settings_write(
            conversation_id,
            ConversationUserSettingsPatch {
                is_muted: Some(muted),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn set_archived(&self, conversation_id: &str, archived: bool) -> Result<()> {
        self.current_user_id().await?;
        self.store.set_archived(conversation_id, archived).await?;
        self.after_user_settings_write(
            conversation_id,
            ConversationUserSettingsPatch {
                is_archived: Some(archived),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn mark_unread(&self, conversation_id: &str) -> Result<u32> {
        self.current_user_id().await?;
        self.store.mark_unread(conversation_id).await
    }

    pub async fn update_draft(&self, conversation_id: &str, draft: Option<&str>) -> Result<()> {
        self.current_user_id().await?;
        self.store.update_draft(conversation_id, draft).await?;
        self.after_user_settings_write(
            conversation_id,
            ConversationUserSettingsPatch {
                draft: Some(draft.unwrap_or("").to_string()),
                ..Default::default()
            },
        )
        .await
    }

    /// 清空本地聊天记录（保留会话；已清空 seq 及更早消息不再同步落库）。
    pub async fn clear_local_chat_history(&self, conversation_id: &str) -> Result<()> {
        let user_id = self.current_user_id().await?;
        let cleared = ConversationLocalLifecycle::clear_history_boundary_with_ports(
            self.store.as_ref(),
            self.cursors.as_deref(),
            &user_id,
            conversation_id,
            LocalConversationVisibility::Keep,
        )
        .await?;
        if cleared.is_none() {
            return Err(FlareError::general_error("conversation not found"));
        }
        Ok(())
    }

    pub async fn ensure_local_conversation(
        &self,
        conversation_id: &str,
        display_name: Option<&str>,
        conversation_type: ConversationType,
        business_type: &str,
        channel_id: String,
    ) -> Result<()> {
        self.current_user_id().await?;
        if self.store.get(conversation_id).await?.is_some() {
            return Ok(());
        }
        let summary = self.identity_service.build_local_conversation(
            conversation_id,
            display_name,
            conversation_type,
            business_type,
            channel_id,
        );
        self.store.save_batch(&[summary]).await
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::*;
    use crate::domain::{ConversationReader, ConversationWriter};
    use crate::infrastructure::persistence::memory_im::MemoryConversationStore;
    use crate::model::Conversation;

    struct FakeSessionSyncRunner {
        fail_read_ack: bool,
    }

    impl SessionSyncRunner for FakeSessionSyncRunner {
        fn request_message_sync(
            &self,
            _conversation_id: &str,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn request_message_sync_from_seq(
            &self,
            _conversation_id: &str,
            _last_seq: u64,
            _limit: i32,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn send_read_ack(
            &self,
            _conversation_id: &str,
            _read_seq: u64,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
            Box::pin(async move {
                if self.fail_read_ack {
                    Err(FlareError::localized(
                        ErrorCode::NetworkConnectionLost,
                        "read ack failed",
                    ))
                } else {
                    Ok(())
                }
            })
        }

        fn request_participants_sync(
            &self,
            _conversation_id: &str,
            _limit: i32,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<ConversationParticipant>>> + Send + '_>>
        {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    fn command_use_case(
        store: Arc<MemoryConversationStore>,
        read_ack_sync: Arc<dyn SessionSyncRunner>,
    ) -> ConversationCommandUseCase {
        ConversationCommandUseCase {
            store,
            cursors: None,
            current_user_id: Arc::new(RwLock::new("hugo".to_string())),
            identity_service: ConversationIdentityService,
            read_service: ConversationReadService,
            settings_sync: None,
            read_ack_sync: Some(read_ack_sync),
        }
    }

    async fn save_unread_conversation(store: &MemoryConversationStore) {
        let conversation = Conversation {
            conversation_id: "conv-read".to_string(),
            max_seq: 8,
            last_read_seq: 3,
            unread_count: 5,
            ..Default::default()
        };
        store.save_batch(&[conversation]).await.unwrap();
    }

    #[tokio::test]
    async fn mark_read_does_not_clear_local_unread_when_read_ack_fails() {
        let store = Arc::new(MemoryConversationStore::new());
        save_unread_conversation(&store).await;
        let use_case = command_use_case(
            Arc::clone(&store),
            Arc::new(FakeSessionSyncRunner {
                fail_read_ack: true,
            }),
        );

        let err = use_case.mark_read("conv-read", 8).await.unwrap_err();

        assert_eq!(err.code(), Some(ErrorCode::NetworkConnectionLost));
        let stored = store.get("conv-read").await.unwrap().unwrap();
        assert_eq!(stored.unread_count, 5);
        assert_eq!(stored.last_read_seq, 3);
    }

    #[tokio::test]
    async fn mark_read_clears_local_unread_after_read_ack_succeeds() {
        let store = Arc::new(MemoryConversationStore::new());
        save_unread_conversation(&store).await;
        let use_case = command_use_case(
            Arc::clone(&store),
            Arc::new(FakeSessionSyncRunner {
                fail_read_ack: false,
            }),
        );

        let unread = use_case.mark_read("conv-read", 8).await.unwrap();

        assert_eq!(unread, 0);
        let stored = store.get("conv-read").await.unwrap().unwrap();
        assert_eq!(stored.unread_count, 0);
        assert_eq!(stored.last_read_seq, 8);
    }

    /// 没起名的成员集合群不写名字：标题在读取时由成员名拼接，名字列里不放 id。
    #[tokio::test]
    async fn an_unnamed_member_set_group_stores_no_name() {
        let store = Arc::new(MemoryConversationStore::new());
        let use_case = command_use_case(
            Arc::clone(&store),
            Arc::new(FakeSessionSyncRunner {
                fail_read_ack: false,
            }),
        );
        let ids = ["alice".to_string(), "bob".to_string()];
        let unnamed = use_case.get_group_by_user_ids(&ids, None).await.unwrap();
        assert_eq!(unnamed.display_name, "");
        assert_eq!(unnamed.member_preview.len(), 3);

        let named = use_case
            .get_group_by_user_ids(&ids, Some(" Weekend ride "))
            .await
            .unwrap();
        assert_eq!(named.display_name, "Weekend ride");
        let again = use_case.get_group_by_user_ids(&ids, None).await.unwrap();
        assert_eq!(again.display_name, "Weekend ride", "a given name is kept");
    }
}
