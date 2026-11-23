use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

const DEFAULT_DEDUPE_CAPACITY: usize = 4096;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum EventDedupeKey {
    EventId(String),
    Seq {
        conversation_id: String,
        event_type: i32,
        seq: u64,
    },
    RequestId {
        conversation_id: String,
        event_type: i32,
        request_id: String,
    },
}

#[derive(Default)]
struct EventDedupeState {
    order: VecDeque<EventDedupeKey>,
    seen: HashSet<EventDedupeKey>,
    pending: HashSet<EventDedupeKey>,
}

impl EventDedupeState {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            order: VecDeque::with_capacity(capacity),
            seen: HashSet::with_capacity(capacity),
            pending: HashSet::new(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct EventDeduper {
    capacity: usize,
    state: Arc<Mutex<EventDedupeState>>,
}

impl EventDeduper {
    pub(crate) fn new(capacity: Option<usize>) -> Self {
        let capacity = capacity.unwrap_or(DEFAULT_DEDUPE_CAPACITY).max(1);
        Self {
            capacity,
            state: Arc::new(Mutex::new(EventDedupeState::with_capacity(capacity))),
        }
    }

    pub(crate) fn claim(&self, event: &flare_proto::common::Event) -> EventClaim {
        let key = dedupe_key_for_event(event);
        if let Some(key) = &key {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.seen.contains(key) {
                return EventClaim::Applied;
            }
            if state.pending.len() >= self.capacity || !state.pending.insert(key.clone()) {
                return EventClaim::InFlight;
            }
        }
        EventClaim::New(EventReservation {
            key,
            deduper: self.clone(),
        })
    }
}

pub(crate) enum EventClaim {
    Applied,
    InFlight,
    New(EventReservation),
}

pub(crate) struct EventReservation {
    key: Option<EventDedupeKey>,
    deduper: EventDeduper,
}

impl EventReservation {
    pub(crate) fn complete(mut self) {
        let Some(key) = self.key.take() else { return };
        let mut state = self.deduper.state.lock().unwrap_or_else(|e| e.into_inner());
        state.pending.remove(&key);
        state.seen.insert(key.clone());
        state.order.push_back(key);
        while state.order.len() > self.deduper.capacity {
            if let Some(removed) = state.order.pop_front() {
                state.seen.remove(&removed);
            }
        }
    }
}

impl Drop for EventReservation {
    fn drop(&mut self) {
        if let Some(key) = &self.key {
            // Cancellation and every failed application release ownership without marking success.
            self.deduper
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pending
                .remove(key);
        }
    }
}

fn dedupe_key_for_event(event: &flare_proto::common::Event) -> Option<EventDedupeKey> {
    if !event.event_id.trim().is_empty() {
        return Some(EventDedupeKey::EventId(event.event_id.clone()));
    }
    if event.conversation_seq > 0 {
        return Some(EventDedupeKey::Seq {
            conversation_id: event.conversation_id.clone(),
            event_type: event.r#type,
            seq: event.conversation_seq,
        });
    }
    if let Some(request_id) = &event.request_id
        && !request_id.trim().is_empty()
    {
        return Some(EventDedupeKey::RequestId {
            conversation_id: event.conversation_id.clone(),
            event_type: event.r#type,
            request_id: request_id.clone(),
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{EventClaim, EventDeduper};

    fn record(deduper: &EventDeduper, event: &flare_proto::common::Event) -> bool {
        match deduper.claim(event) {
            EventClaim::New(reservation) => {
                reservation.complete();
                true
            }
            _ => false,
        }
    }

    #[tokio::test]
    async fn duplicate_event_id_is_deduped() {
        let deduper = EventDeduper::new(Some(16));
        let event = flare_proto::common::Event {
            event_id: "evt-1".to_string(),
            ..Default::default()
        };

        assert!(record(&deduper, &event));
        assert!(!record(&deduper, &event));
    }

    fn evt(id: u64) -> flare_proto::common::Event {
        flare_proto::common::Event {
            event_id: format!("e{id}"),
            ..Default::default()
        }
    }

    #[test]
    fn an_in_flight_event_is_not_applied_and_failure_leaves_it_replayable() {
        let deduper = EventDeduper::new(Some(1));
        let EventClaim::New(reservation) = deduper.claim(&evt(1)) else {
            panic!("first owner")
        };
        assert!(matches!(deduper.claim(&evt(1)), EventClaim::InFlight));
        // Capacity pressure must not evict the active owner.
        assert!(matches!(deduper.claim(&evt(2)), EventClaim::InFlight));
        drop(reservation);
        assert!(record(&deduper, &evt(1)));
        assert!(matches!(deduper.claim(&evt(1)), EventClaim::Applied));
    }

    #[tokio::test]
    async fn cancelling_an_application_releases_its_reservation() {
        let deduper = EventDeduper::new(Some(2));
        let owner = deduper.clone();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let EventClaim::New(_reservation) = owner.claim(&evt(1)) else {
                panic!("first owner")
            };
            entered.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(record(&deduper, &evt(1)));
    }

    /// 窗口满后淘汰最旧 key:被淘汰的 key 再次出现应视为"新"。
    #[tokio::test]
    async fn evicts_oldest_when_window_full() {
        let cap = 4;
        let deduper = EventDeduper::new(Some(cap));
        for i in 0..cap as u64 {
            assert!(record(&deduper, &evt(i)), "first sight of e{i}");
        }
        // e0 仍在窗口 [e0..e3] → 重复被拒(且不重排)。
        assert!(!record(&deduper, &evt(0)), "e0 still in window");
        // e4 为新 → 窗口满 → 淘汰最旧 e0 → 窗口 [e1..e4]。
        assert!(record(&deduper, &evt(4)), "e4 is new");
        // e0 已被淘汰 → 再次视为新。
        assert!(record(&deduper, &evt(0)), "e0 was evicted, new again");
        // e4 仍在窗口 → 重复被拒。
        assert!(!record(&deduper, &evt(4)), "e4 still in window");
    }

    /// TEST-1:对拍独立参考实现(保留最近 cap 个不同 key 的 FIFO 窗口),
    /// 小 key 空间强制重复+淘汰,逐步断言去重判定一致。
    #[tokio::test]
    async fn prop_dedupe_window_matches_independent_reference() {
        struct RefDedup {
            cap: usize,
            order: Vec<u64>, // 当前窗口内不同 key,插入序;超容从头淘汰
        }
        impl RefDedup {
            fn record(&mut self, k: u64) -> bool {
                if self.order.contains(&k) {
                    return false;
                }
                self.order.push(k);
                if self.order.len() > self.cap {
                    self.order.remove(0);
                }
                true
            }
        }

        // 确定性 LCG(避免引入 rand/proptest 依赖)。
        let mut s: u64 = 0xC0FF_EE12_3456_789A;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            s
        };

        let cap = 8usize;
        let deduper = EventDeduper::new(Some(cap));
        let mut reference = RefDedup {
            cap,
            order: Vec::new(),
        };

        for step in 0..6000 {
            let k = next() % 20; // key 空间 20 > cap 8 → 必经淘汰
            let got = record(&deduper, &evt(k));
            let want = reference.record(k);
            assert_eq!(
                got, want,
                "dedupe decision mismatch at step {step}, key {k}"
            );
        }
    }
}
