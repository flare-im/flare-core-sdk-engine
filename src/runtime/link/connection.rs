//! Device-local state shared by Core HTTP and the session transport.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use flare_access_contract::network::NetPrint;

use crate::client::endpoints::{CoreEndpoints, EndpointStore, EndpointUpdate};
use crate::shared::error::{ErrorCode, FlareError, Result};

use super::heartbeat_learning::HeartbeatLearning;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ProtocolMode {
    #[default]
    Race,
    WebSocket,
    Quic,
}

#[derive(Default)]
struct Network {
    fingerprint: NetPrint,
    unavailable: bool,
}

type UpgradeObserver = Arc<dyn Fn(Option<u64>) + Send + Sync>;

#[derive(Default)]
pub(crate) struct ConnectionRuntime {
    pub endpoints: Option<EndpointStore>,
    pub heartbeat_learning: HeartbeatLearning,
    network: Mutex<Network>,
    protocol_mode: Mutex<ProtocolMode>,
    upgrade_observer: Mutex<Option<UpgradeObserver>>,
}

impl ConnectionRuntime {
    pub fn new(endpoints: Option<CoreEndpoints>, allow_cleartext: bool) -> Self {
        Self {
            endpoints: endpoints.map(|endpoints| EndpointStore::new(endpoints, allow_cleartext)),
            ..Self::default()
        }
    }

    pub fn endpoint_revision(&self) -> Option<u32> {
        self.endpoints
            .as_ref()
            .map(|store| store.snapshot().revision)
    }

    pub fn development_mode(&self) -> bool {
        self.endpoints
            .as_ref()
            .is_some_and(|store| store.allow_cleartext)
    }

    pub fn endpoint_store(&self) -> Result<&EndpointStore> {
        self.endpoints.as_ref().ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "initialise Core with sdkConfig.endpoints",
            )
        })
    }

    pub fn validate_replacement(&self, endpoints: CoreEndpoints) -> Result<CoreEndpoints> {
        endpoints.validate(self.endpoint_store()?.allow_cleartext)
    }

    pub fn replace_endpoints(&self, endpoints: CoreEndpoints) -> Result<EndpointUpdate> {
        self.endpoint_store()?.replace(endpoints)
    }

    pub fn report_network(
        &self,
        available: bool,
        interface: Option<&str>,
        hint: Option<&str>,
    ) -> bool {
        let fingerprint = NetPrint::from_parts(interface, hint);
        let mut network = lock(&self.network);
        network.unavailable = !available;
        let changed = network.fingerprint != fingerprint;
        network.fingerprint = fingerprint;
        changed
    }

    pub fn network_unavailable(&self) -> bool {
        lock(&self.network).unavailable
    }

    pub fn net(&self) -> NetPrint {
        lock(&self.network).fingerprint
    }

    pub fn set_protocol_mode(&self, mode: ProtocolMode) -> bool {
        std::mem::replace(&mut *lock(&self.protocol_mode), mode) != mode
    }

    pub fn protocol_mode(&self) -> ProtocolMode {
        *lock(&self.protocol_mode)
    }

    pub fn observe_upgrade_required(&self, observer: UpgradeObserver) {
        *lock(&self.upgrade_observer) = Some(observer);
    }

    pub fn note_upgrade_required(&self, min_build: Option<u64>) {
        let observer = lock(&self.upgrade_observer).clone();
        if let Some(observer) = observer {
            observer(min_build);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub(super) fn now_ms() -> i64 {
    crate::shared::util::now_millis() as i64
}
