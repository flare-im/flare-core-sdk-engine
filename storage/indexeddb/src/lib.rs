#![cfg(target_arch = "wasm32")]
//! Browser IndexedDB-backed storage adapter for the WASM SDK binding.

pub mod extension_records;
mod host;
mod plugin_bundle_storage;
mod plugin_command;
mod plugin_consent;
mod provider;

pub use host::{clear_storage_host, set_storage_host, storage_host_configured};
pub use provider::build_web_store_provider;

/// Open the production browser host using the account selected inside prepare.
pub struct IndexedDbAccountStore;

#[async_trait::async_trait]
impl flare_im_core_sdk::storage::AccountStoreOpener for IndexedDbAccountStore {
    async fn open(
        &self,
        _data_root: &std::path::Path,
        tenant: &str,
        user: &str,
    ) -> flare_im_core_sdk::Result<flare_im_core_sdk::storage::StoreProvider> {
        let (tenant, user) = (tenant.to_owned(), user.to_owned());
        let (tx, rx) = tokio::sync::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = tx.send(build_web_store_provider("flare-im-core-sdk", &tenant, &user).await);
        });
        rx.await
            .map_err(|_| flare_im_core_sdk::FlareError::system("IndexedDB open cancelled"))?
    }
}
