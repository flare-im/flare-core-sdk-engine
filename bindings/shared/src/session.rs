//! Shared connected API session cache for platform bindings.
//!
//! C, Tauri, and wasm all need the same generation-aware `ConnectedApis`
//! cache. Keeping it here prevents each binding from inventing subtly
//! different hot-path behavior.

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use flare_im_core_sdk::Result;
use flare_im_core_sdk::client::api::{
    CapabilityApi, ConversationApi, MediaApi, MessageApi, MessageBuildApi, PresenceApi, ViewApi,
};
use flare_im_core_sdk::client::{ConnectedApis, IMClient};

#[derive(Clone)]
struct SessionCache {
    generation: u64,
    apis: ConnectedApis,
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_im_core_sdk::client::{LoginDbKind, SdkConfigOverlay};
    use flare_im_core_sdk::prelude::in_memory_im_provider;

    #[tokio::test]
    async fn all_search_dispatch_routes_preserve_query_filters() {
        use flare_im_core_sdk::model::{IMMessage, MessageType};
        use flare_im_core_sdk::prelude::in_memory_im_provider;
        use flare_im_core_sdk::serde_json::{self, json};
        let client = IMClient::new();
        client
            .init(
                None,
                Some(SdkConfigOverlay {
                    data_url: Some("file:///tmp/flare-search-contract-test".into()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        let stores = in_memory_im_provider();
        let mut rows = Vec::new();
        for (id, kind, time, sender, recalled) in [
            ("text", MessageType::Text, 900, "u1", false),
            ("image", MessageType::Image, 800, "u1", false),
            ("file", MessageType::File, 700, "u1", false),
            ("other-sender", MessageType::File, 600, "u2", false),
            ("recalled", MessageType::File, 650, "u1", true),
            ("too-old", MessageType::File, 100, "u1", false),
        ] {
            let mut m = IMMessage::new(Default::default());
            m.server_id = id.into();
            m.client_msg_id = format!("client-{id}");
            m.conversation_id = "c1".into();
            m.sender_id = sender.into();
            m.created_at = time;
            m.conversation_seq = time;
            m.message_type = kind as i32;
            m.is_recalled = recalled;
            m.content = Some(
                serde_json::from_value(if kind == MessageType::File {
                    json!({"contentType":"file", "fileId":id,"fileName":"report-11.pdf",
                    "mimeType":"application/pdf","fileSize":10,"url":"","description":""})
                } else {
                    json!({"contentType":"text","text":"111","mentions":[]})
                })
                .unwrap(),
            );
            rows.push(m);
        }
        stores.messages.save_batch(&rows).await.unwrap();
        client
            .prepare("u1", LoginDbKind::provided(stores))
            .await
            .unwrap();
        let api = client.message().unwrap();
        let query = json!({"conversationId":"c1","keyword":"11","kinds":["file"],
            "senderId":"u1","fromTime":500,"toTime":750,"includeRecalled":false,"limit":1});
        for operation in ["search", "search_in_conversation", "search_by_query"] {
            let response = crate::generated::dispatch::message::dispatch_message_json(
                &api,
                operation,
                &query.to_string(),
            )
            .await
            .unwrap();
            let response = crate::binding_response_to_value(response);
            assert_eq!(
                response["messages"].as_array().unwrap().len(),
                1,
                "{operation}"
            );
            assert_eq!(response["messages"][0]["serverId"], "file", "{operation}");
        }
        client.logout().await.unwrap();
    }

    #[tokio::test]
    async fn late_install_cannot_relabel_an_old_accounts_facades() {
        let client = IMClient::new();
        let root =
            std::env::temp_dir().join(format!("flare-binding-session-{}", std::process::id()));
        client
            .init(
                None,
                Some(SdkConfigOverlay {
                    data_url: Some(format!("file://{}", root.display())),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
        client
            .prepare("alice", LoginDbKind::provided(in_memory_im_provider()))
            .await
            .unwrap();
        let alice = client.connected_apis().await.unwrap();
        let slot = SessionSlot::default();
        slot.install(&client, alice.clone()).await;
        client
            .prepare("bob", LoginDbKind::provided(in_memory_im_provider()))
            .await
            .unwrap();
        // A delayed binding completion must not associate Alice's API with Bob's generation.
        slot.install(&client, alice).await;
        let bob = slot.message_api(&client).await.unwrap();
        assert!(bob.search("hello", 10).await.unwrap().is_empty());
        client.logout().await.unwrap();
        assert!(slot.message_api(&client).await.is_err());
        assert!(bob.search("hello", 10).await.is_err());
    }
}

/// Per-binding-handle cache for the API facades of the active IM session.
#[derive(Clone, Default)]
pub struct SessionSlot {
    inner: Arc<RwLock<Option<SessionCache>>>,
}

impl SessionSlot {
    fn read_cache(&self) -> RwLockReadGuard<'_, Option<SessionCache>> {
        match self.inner.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn write_cache(&self) -> RwLockWriteGuard<'_, Option<SessionCache>> {
        match self.inner.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Install a freshly returned API snapshot after login.
    pub async fn install(&self, client: &IMClient, apis: ConnectedApis) {
        let generation = apis.session_generation();
        if generation == client.session_generation_snapshot() {
            *self.write_cache() = Some(SessionCache { generation, apis });
        }
    }

    /// Clear cached API facades after logout/disconnect/uninit.
    pub async fn clear(&self) {
        *self.write_cache() = None;
    }

    async fn session_or_live(&self, client: &IMClient) -> Result<ConnectedApis> {
        let generation = client.session_generation_snapshot();
        if let Some(apis) = self
            .read_cache()
            .as_ref()
            .filter(|cache| cache.generation == generation)
            .map(|cache| cache.apis.clone())
        {
            return Ok(apis);
        }

        let apis = client.connected_apis().await?;
        let generation = apis.session_generation();
        if generation != client.session_generation_snapshot() {
            return Err(flare_im_core_sdk::FlareError::localized(
                flare_im_core_sdk::ErrorCode::NotConnected,
                "Session changed during API acquisition",
            ));
        }
        *self.write_cache() = Some(SessionCache {
            generation,
            apis: apis.clone(),
        });
        Ok(apis)
    }

    pub async fn message_api(&self, client: &IMClient) -> Result<MessageApi> {
        Ok(self.session_or_live(client).await?.message_api)
    }

    pub async fn message_build_api(&self, client: &IMClient) -> Result<Arc<MessageBuildApi>> {
        Ok(self.session_or_live(client).await?.message_build_api)
    }

    pub async fn conversation_api(&self, client: &IMClient) -> Result<ConversationApi> {
        Ok(self.session_or_live(client).await?.conversation_api)
    }

    pub async fn view_api(&self, client: &IMClient) -> Result<Arc<ViewApi>> {
        Ok(self.session_or_live(client).await?.view_api)
    }

    pub async fn media_api(&self, client: &IMClient) -> Result<Arc<MediaApi>> {
        Ok(self.session_or_live(client).await?.media_api)
    }

    pub async fn capability_api(&self, client: &IMClient) -> Result<Arc<CapabilityApi>> {
        Ok(self.session_or_live(client).await?.capability_api)
    }

    pub async fn presence_api(&self, client: &IMClient) -> Result<Arc<PresenceApi>> {
        Ok(self.session_or_live(client).await?.presence_api)
    }
}
