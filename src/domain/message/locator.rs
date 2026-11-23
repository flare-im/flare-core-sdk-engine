use crate::domain::MessageStore;
use crate::model::message::IMMessage;
use crate::shared::error::{ErrorCode, FlareError, Result};

pub struct MessageLocatorService;

impl MessageLocatorService {
    pub async fn find_by_any_id(
        &self,
        store: &dyn MessageStore,
        message_id: &str,
    ) -> Result<Option<IMMessage>> {
        match store.get(message_id).await? {
            Some(message) => Ok(Some(message)),
            None => store.get_by_client_msg_id(message_id).await,
        }
    }

    pub async fn require_by_any_id(
        &self,
        store: &dyn MessageStore,
        message_id: &str,
    ) -> Result<IMMessage> {
        self.find_by_any_id(store, message_id)
            .await?
            .ok_or_else(|| {
                FlareError::localized(
                    ErrorCode::MessageNotFound,
                    format!("message not found: {}", message_id),
                )
            })
    }
}
