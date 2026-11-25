use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::mpsc;

use super::super::selector::EventFilter;
use super::super::types::{SdkEvent, SyncNotify};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventReceiveError {
    Closed,
}

/// Shared raw event envelope for raw subscribers.
///
/// The core bus does not know platform JSON contracts, but it can provide a
/// per-published-event cache slot so binding layers serialize an event at most
/// once across raw subscribers.
pub struct RawSdkEvent {
    event: SdkEvent,
    json: OnceLock<Arc<str>>,
}

impl RawSdkEvent {
    fn new(event: SdkEvent) -> Self {
        Self {
            event,
            json: OnceLock::new(),
        }
    }

    pub(super) fn from_event(event: SdkEvent) -> Self {
        Self::new(event)
    }

    pub fn event(&self) -> &SdkEvent {
        &self.event
    }

    pub fn cloned_event(&self) -> SdkEvent {
        self.event.clone()
    }

    /// Returns the canonical raw SDK event JSON for this event, computing it once.
    ///
    /// The serializer must produce the same raw event contract every time; this
    /// is a single-slot cache, not a cache keyed by platform-specific formats.
    pub fn cached_json<F>(&self, serialize: F) -> Arc<str>
    where
        F: FnOnce(&SdkEvent) -> String,
    {
        Arc::clone(
            self.json
                .get_or_init(|| Arc::<str>::from(serialize(self.event()))),
        )
    }
}

/// Bounded event receiver. Each subscription owns an independent queue.
pub struct EventReceiver {
    lifetime: super::SessionLifetime,
    rx: mpsc::Receiver<SdkEvent>,
    filter: EventFilter,
    lagged: Arc<AtomicBool>,
    dropped_events: Arc<AtomicU64>,
}

pub type FilteredEventReceiver = EventReceiver;

impl EventReceiver {
    pub fn new(
        rx: mpsc::Receiver<SdkEvent>,
        filter: EventFilter,
        lagged: Arc<AtomicBool>,
        dropped_events: Arc<AtomicU64>,
    ) -> Self {
        Self {
            lifetime: super::SessionLifetime::default(),
            rx,
            filter,
            lagged,
            dropped_events,
        }
    }

    pub(super) fn with_lifetime(mut self, lifetime: super::SessionLifetime) -> Self {
        self.lifetime = lifetime;
        self
    }

    pub fn filter(&self) -> &EventFilter {
        &self.filter
    }

    pub async fn recv(&mut self) -> Result<SdkEvent, EventReceiveError> {
        if self.lifetime.retired() {
            return Err(EventReceiveError::Closed);
        }
        if let Some(event) = self.take_resync_event() {
            return Ok(event);
        }
        self.lifetime
            .until_retired(self.rx.recv())
            .await
            .flatten()
            .ok_or(EventReceiveError::Closed)
    }

    pub fn try_recv(&mut self) -> Result<SdkEvent, mpsc::error::TryRecvError> {
        if self.lifetime.retired() {
            return Err(mpsc::error::TryRecvError::Disconnected);
        }
        if let Some(event) = self.take_resync_event() {
            return Ok(event);
        }
        self.rx.try_recv()
    }

    fn take_resync_event(&self) -> Option<SdkEvent> {
        if !self.lagged.swap(false, Ordering::AcqRel) {
            return None;
        }
        let dropped_events = self.dropped_events.swap(0, Ordering::AcqRel).max(1);
        Some(SdkEvent::Sync(SyncNotify::ResyncNeeded {
            scope: "global".to_string(),
            reason: "event_queue_lagged".to_string(),
            dropped_events,
        }))
    }
}

/// Shared raw event receiver used by platform bridges that can reuse cached
/// serialized payloads across multiple subscribers.
pub struct SharedEventReceiver {
    lifetime: super::SessionLifetime,
    rx: mpsc::Receiver<Arc<RawSdkEvent>>,
    filter: EventFilter,
    lagged: Arc<AtomicBool>,
    dropped_events: Arc<AtomicU64>,
}

impl SharedEventReceiver {
    pub fn new(
        rx: mpsc::Receiver<Arc<RawSdkEvent>>,
        filter: EventFilter,
        lagged: Arc<AtomicBool>,
        dropped_events: Arc<AtomicU64>,
    ) -> Self {
        Self {
            lifetime: super::SessionLifetime::default(),
            rx,
            filter,
            lagged,
            dropped_events,
        }
    }

    pub(super) fn with_lifetime(mut self, lifetime: super::SessionLifetime) -> Self {
        self.lifetime = lifetime;
        self
    }

    pub fn filter(&self) -> &EventFilter {
        &self.filter
    }

    pub async fn recv(&mut self) -> Result<Arc<RawSdkEvent>, EventReceiveError> {
        if self.lifetime.retired() {
            return Err(EventReceiveError::Closed);
        }
        if let Some(event) = self.take_resync_event() {
            return Ok(event);
        }
        self.lifetime
            .until_retired(self.rx.recv())
            .await
            .flatten()
            .ok_or(EventReceiveError::Closed)
    }

    pub fn try_recv(&mut self) -> Result<Arc<RawSdkEvent>, mpsc::error::TryRecvError> {
        if self.lifetime.retired() {
            return Err(mpsc::error::TryRecvError::Disconnected);
        }
        if let Some(event) = self.take_resync_event() {
            return Ok(event);
        }
        self.rx.try_recv()
    }

    fn take_resync_event(&self) -> Option<Arc<RawSdkEvent>> {
        if !self.lagged.swap(false, Ordering::AcqRel) {
            return None;
        }
        let dropped_events = self.dropped_events.swap(0, Ordering::AcqRel).max(1);
        Some(Arc::new(RawSdkEvent::from_event(SdkEvent::Sync(
            SyncNotify::ResyncNeeded {
                scope: "global".to_string(),
                reason: "event_queue_lagged".to_string(),
                dropped_events,
            },
        ))))
    }
}
