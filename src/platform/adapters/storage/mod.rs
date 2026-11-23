//! Storage adapters.
//!
//! `StoreProvider` is the stable storage port consumed by core IM logic.
//! Built-in native storage can open Memory and SQLite. Web/RN/uni-app storage
//! is injected by host adapters as a `StoreProvider`, commonly backed by
//! IndexedDB or platform SQLite.

mod profile;
mod store_factory;

pub use crate::infrastructure::persistence::StoreProvider as CoreStoreProvider;
pub use profile::StorageAdapterProfile;
pub use store_factory::{
    open_store_from_runtime_config, open_store_from_runtime_config_with_secure_key_store,
};
