//! SQLite implementation of the account-scoped IM stores.
pub mod provider;
pub mod repos;
mod runtime;
pub use runtime::SqliteRuntimeAssembler;

use flare_im_core_sdk::Result;
use flare_im_core_sdk::client::LoginDbKind;
use flare_im_core_sdk::storage::{AccountStoreOpener, SecureKeyStore, StoreProvider};
use std::path::Path;
use std::sync::Arc;

pub struct SqliteAccountStore {
    encryption: Option<(Arc<dyn SecureKeyStore>, SecureDatabaseScope)>,
}
#[derive(Default)]
pub struct SecureDatabaseScope {
    pub namespace: Option<String>,
    pub tenant: Option<String>,
    pub key_name: Option<String>,
}

pub fn sqlite() -> LoginDbKind {
    LoginDbKind::Open(Arc::new(SqliteAccountStore { encryption: None }))
}

pub fn encrypted_sqlite(
    key_store: Arc<dyn SecureKeyStore>,
    scope: SecureDatabaseScope,
) -> LoginDbKind {
    LoginDbKind::Open(Arc::new(SqliteAccountStore {
        encryption: Some((key_store, scope)),
    }))
}

impl SqliteAccountStore {
    /// Whether the account opener uses the standard unencrypted SQLite database.
    pub fn is_plain_sqlite(storage: &LoginDbKind) -> bool {
        let LoginDbKind::Open(opener) = storage else {
            return false;
        };
        let opener: &dyn std::any::Any = opener.as_ref();
        opener
            .downcast_ref::<Self>()
            .is_some_and(|store| store.encryption.is_none())
    }
}

#[async_trait::async_trait]
impl AccountStoreOpener for SqliteAccountStore {
    async fn open(&self, root: &Path, tenant: &str, user: &str) -> Result<StoreProvider> {
        match &self.encryption {
            None => {
                provider::open_sqlite_store_for_user(root, "flare-im-core-sdk", tenant, user).await
            }
            Some((keys, scope)) => {
                provider::open_sqlite_store_for_user_with_secure_key_store(
                    root,
                    user,
                    scope.namespace.as_deref().unwrap_or("flare-im-core-sdk"),
                    scope.tenant.as_deref().unwrap_or(tenant),
                    scope.key_name.as_deref(),
                    keys.as_ref(),
                )
                .await
            }
        }
    }

    fn credential_store(&self, tenant: &str) -> Option<(Arc<dyn SecureKeyStore>, String, String)> {
        self.encryption.as_ref().map(|(keys, scope)| {
            (
                keys.clone(),
                scope
                    .namespace
                    .as_deref()
                    .unwrap_or("flare-im-core-sdk")
                    .into(),
                scope.tenant.as_deref().unwrap_or(tenant).into(),
            )
        })
    }
}
