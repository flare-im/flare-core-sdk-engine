#![cfg(target_arch = "wasm32")]
//! Browser IndexedDB-backed storage adapter for the WASM SDK binding.

mod access_store;
mod host;
mod plugin_command;
mod provider;

pub use access_store::JsGlobalStorageHost;
pub use host::{clear_storage_host, set_storage_host, storage_host_configured};
pub use provider::build_web_store_provider;
