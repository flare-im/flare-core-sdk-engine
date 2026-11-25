//! Native runtime assembly.
//!
//! Native platforms keep using the existing `StoreProvider` and socket
//! transport. This module gives Android/iOS/Flutter/Tauri/Electron/RN native
//! bridges a stable assembly point without changing the public IM client APIs.

use crate::Result;
use crate::platform::runtime::{RuntimeAssembler, RuntimeComponents, RuntimeConfig};
use crate::storage::StoreProvider;
use async_trait::async_trait;

pub struct NativeRuntimeAssembler {
    stores: StoreProvider,
}
impl NativeRuntimeAssembler {
    pub fn new(stores: StoreProvider) -> Self {
        Self { stores }
    }
    pub fn stores(&self) -> &StoreProvider {
        &self.stores
    }
}
#[async_trait]
impl RuntimeAssembler for NativeRuntimeAssembler {
    async fn assemble(&self, config: RuntimeConfig) -> Result<RuntimeComponents> {
        self.stores.require_reliable_outbox()?;
        Ok(RuntimeComponents::native_default(
            config.sdk,
            self.stores.clone(),
        ))
    }
}
