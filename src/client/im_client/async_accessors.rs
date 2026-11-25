use std::collections::HashMap;
use std::sync::Arc;

use crate::client::api::{ConversationApi, MessageApi, UserPresenceDto, ViewApi};
use crate::shared::error::Result;

use super::IMClient;

impl IMClient {
    pub async fn message_async(&self) -> Result<MessageApi> {
        let g = self.read_inner_async().await?;
        g.message_api
            .clone()
            .map(|api| api.for_session(self.session_end_watch(g.session_generation)))
            .ok_or_else(Self::not_connected)
    }

    pub async fn conversation_async(&self) -> Result<ConversationApi> {
        let g = self.read_inner_async().await?;
        g.conversation_api
            .as_ref()
            .map(|a| {
                a.as_ref()
                    .clone()
                    .for_session(self.session_end_watch(g.session_generation))
            })
            .ok_or_else(Self::not_connected)
    }

    pub async fn view_async(&self) -> Result<Arc<ViewApi>> {
        let g = self.read_inner_async().await?;
        Ok(Arc::new(
            g.view_api
                .as_deref()
                .ok_or_else(Self::not_connected)?
                .clone()
                .for_session(self.session_end_watch(g.session_generation)),
        ))
    }

    pub async fn get_user_presence(&self, user_id: &str) -> Result<UserPresenceDto> {
        let g = self.read_inner_async().await?;
        let api = g
            .presence_api
            .as_deref()
            .ok_or_else(Self::not_connected)?
            .clone()
            .for_session(self.session_end_watch(g.session_generation));
        drop(g);
        api.get_user_presence(user_id).await
    }

    pub async fn batch_get_user_presence(
        &self,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserPresenceDto>> {
        let g = self.read_inner_async().await?;
        let api = g
            .presence_api
            .as_deref()
            .ok_or_else(Self::not_connected)?
            .clone()
            .for_session(self.session_end_watch(g.session_generation));
        drop(g);
        api.batch_get_user_presence(user_ids).await
    }

    pub async fn subscribe_user_presence(&self, user_ids: Vec<String>) -> Result<()> {
        let g = self.read_inner_async().await?;
        let api = g
            .presence_api
            .as_deref()
            .ok_or_else(Self::not_connected)?
            .clone()
            .for_session(self.session_end_watch(g.session_generation));
        drop(g);
        api.subscribe_user_presence(user_ids).await
    }
}
