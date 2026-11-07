//! Tenant config notices (`DataPacket.tenant_notice`): "topic `T` of this
//! tenant moved to version `V`". The notices of a topic that arrive before it is
//! handled merge into the newest one, and a topic is handled after a random part
//! of the notice's spread (never sooner than a spread after the last time), so a
//! tenant's clients do not all re-read in the same instant.
//!
//! The plugin catalog (`flare.plugin.catalog`) is the SDK's own topic: its
//! notice invalidates the catalog at once and announces
//! `PluginEvent::CatalogChanged` to hosts. So is the access manifest
//! (`flare.access`, version = serial): its notice starts a refresh of the
//! manifest. So is the release manifest (`flare.release`): its notice starts a
//! conditional fetch. Any other topic belongs to an extension and is announced as
//! `SdkEvent::Extension` from [`TENANT_NOTICE_SOURCE`], named by the topic, with
//! `{"version": "<decimal>", "tenantWide": bool}`. Core never interprets a topic.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use flare_proto::common::TenantNotice;
use rand::Rng;

use super::CapabilityApi;
use crate::kernel::TenantNoticePort;
use crate::kernel::event::{
    ACCESS_MANIFEST_TOPIC, EventBus, ExtensionEvent, PLUGIN_CATALOG_TOPIC, PluginEvent,
    RELEASE_MANIFEST_TOPIC, SdkEvent, TENANT_NOTICE_SOURCE,
};
use crate::runtime::link::AccessRuntime;
use crate::shared::util::{delay, now_millis, spawn_background};

/// The shortest and the longest spread honored, whatever a notice says.
pub(super) const MIN_SPREAD_MS: u64 = 3_000;
pub(super) const MAX_SPREAD_MS: u64 = 60_000;
/// Topics tracked at once; a tenant has a handful.
const MAX_TOPICS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Pending {
    pub version: u64,
    pub tenant_wide: bool,
}

/// When to handle one topic: once per burst of notices, at least a spread
/// after the previous time.
#[derive(Debug, Default)]
pub(super) struct NoticeWindow {
    pending: Option<Pending>,
    last_handled_at_ms: Option<u64>,
}

impl NoticeWindow {
    /// Records a notice at `now_ms`. Returns the delay of the handling it
    /// schedules (`jitter_ms` capped at `spread_ms`, and never sooner than a
    /// spread after the last handling), or `None` when one is already
    /// scheduled: that one handles the newest version.
    pub(super) fn notice(
        &mut self,
        notice: Pending,
        now_ms: u64,
        jitter_ms: u64,
        spread_ms: u64,
    ) -> Option<u64> {
        let scheduled = self.pending.is_some();
        let newest = match self.pending {
            Some(pending) if pending.version >= notice.version => Pending {
                tenant_wide: pending.tenant_wide || notice.tenant_wide,
                ..pending
            },
            Some(pending) => Pending {
                tenant_wide: pending.tenant_wide || notice.tenant_wide,
                ..notice
            },
            None => notice,
        };
        self.pending = Some(newest);
        if scheduled {
            return None;
        }
        let gap_ms = self
            .last_handled_at_ms
            .map_or(0, |last| (last + spread_ms).saturating_sub(now_ms));
        Some(jitter_ms.min(spread_ms).max(gap_ms))
    }

    /// The notice to handle at `now_ms`; `None` when nothing is pending.
    pub(super) fn take(&mut self, now_ms: u64) -> Option<Pending> {
        let pending = self.pending.take()?;
        self.last_handled_at_ms = Some(now_ms);
        Some(pending)
    }
}

fn spread_of(notice: &TenantNotice) -> u64 {
    u64::from(notice.spread_ms).clamp(MIN_SPREAD_MS, MAX_SPREAD_MS)
}

fn random_jitter_ms(spread_ms: u64) -> u64 {
    rand::thread_rng().gen_range(0..=spread_ms)
}

/// [`TenantNoticePort`] of a client: refreshes the plugin catalog and announces
/// the other topics to extensions on its event bus.
#[derive(Clone)]
pub(crate) struct TenantNotices {
    capability: CapabilityApi,
    access: Arc<AccessRuntime>,
    bus: EventBus,
    windows: Arc<Mutex<HashMap<String, NoticeWindow>>>,
    jitter_ms: fn(u64) -> u64,
}

impl TenantNotices {
    pub(crate) fn new(
        capability: CapabilityApi,
        access: Arc<AccessRuntime>,
        bus: EventBus,
    ) -> Self {
        Self::with_jitter(capability, access, bus, random_jitter_ms)
    }

    fn with_jitter(
        capability: CapabilityApi,
        access: Arc<AccessRuntime>,
        bus: EventBus,
        jitter_ms: fn(u64) -> u64,
    ) -> Self {
        Self {
            capability,
            access,
            bus,
            windows: Arc::new(Mutex::new(HashMap::new())),
            jitter_ms,
        }
    }

    fn windows(&self) -> std::sync::MutexGuard<'_, HashMap<String, NoticeWindow>> {
        self.windows.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn handle(&self, topic: &str) {
        let Some(pending) = self
            .windows()
            .get_mut(topic)
            .and_then(|window| window.take(now_millis()))
        else {
            return;
        };
        if topic == ACCESS_MANIFEST_TOPIC {
            self.access.notify_access_serial(pending.version);
            return;
        }
        if topic == RELEASE_MANIFEST_TOPIC {
            self.access.notify_release();
            return;
        }
        if topic == PLUGIN_CATALOG_TOPIC {
            // Re-announce the recent plugin cards against the fresh catalog, then tell hosts
            // to read their plugin entries again.
            if let Err(error) = self.capability.invalidate_plugin_catalog().await {
                tracing::debug!(target: "flare_sdk.plugin", %error, "plugin catalog invalidation failed");
            }
            self.bus
                .publish(SdkEvent::Plugin(PluginEvent::CatalogChanged {
                    version: pending.version.to_string(),
                }));
            return;
        }
        let payload = serde_json::json!({
            "version": pending.version.to_string(),
            "tenantWide": pending.tenant_wide,
        });
        self.bus.publish(SdkEvent::Extension(ExtensionEvent {
            source: TENANT_NOTICE_SOURCE.to_string(),
            event_type: topic.to_string(),
            payload: payload.to_string().into_bytes(),
        }));
    }
}

impl TenantNoticePort for TenantNotices {
    fn notice(&self, notice: TenantNotice) {
        if notice.topic.is_empty() || notice.version == 0 {
            return;
        }
        if notice.topic == PLUGIN_CATALOG_TOPIC {
            // Reads in flight now answer for the old catalog: never share them.
            self.capability.invalidate_catalog_reads();
        }
        let spread_ms = spread_of(&notice);
        let jitter_ms = (self.jitter_ms)(spread_ms);
        let pending = Pending {
            version: notice.version,
            tenant_wide: notice.tenant_wide,
        };
        let scheduled = {
            let mut windows = self.windows();
            if windows.len() >= MAX_TOPICS && !windows.contains_key(&notice.topic) {
                tracing::debug!(topic = %notice.topic, "too many tenant notice topics; dropped");
                return;
            }
            windows.entry(notice.topic.clone()).or_default().notice(
                pending,
                now_millis(),
                jitter_ms,
                spread_ms,
            )
        };
        let Some(delay_ms) = scheduled else {
            return;
        };
        let this = self.clone();
        spawn_background(async move {
            delay(Duration::from_millis(delay_ms)).await;
            this.handle(&notice.topic).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use super::*;
    use crate::infrastructure::transport::http::http_client::HttpRequestContext;
    use crate::kernel::event::{ExtensionEventType, PluginEventType, SdkEventType};

    const SPREAD: u64 = 3_000;

    fn pending(version: u64) -> Pending {
        Pending {
            version,
            tenant_wide: false,
        }
    }

    #[test]
    fn a_lone_notice_waits_its_jitter_capped_at_the_spread() {
        for (jitter, expected) in [(0, 0), (1_234, 1_234), (3_000, 3_000), (9_999, 3_000)] {
            let mut window = NoticeWindow::default();
            assert_eq!(
                window.notice(pending(1), 10_000, jitter, SPREAD),
                Some(expected)
            );
        }
        let mut window = NoticeWindow::default();
        assert_eq!(window.notice(pending(1), 0, 40_000, 30_000), Some(30_000));
    }

    #[test]
    fn spreads_are_clamped_and_jitter_stays_inside() {
        let notice = |spread_ms| TenantNotice {
            topic: "social.features".into(),
            version: 1,
            spread_ms,
            tenant_wide: false,
        };
        assert_eq!(spread_of(&notice(0)), MIN_SPREAD_MS);
        assert_eq!(spread_of(&notice(20_000)), 20_000);
        assert_eq!(spread_of(&notice(u32::MAX)), MAX_SPREAD_MS);
        for _ in 0..1_000 {
            assert!(random_jitter_ms(SPREAD) <= SPREAD);
        }
    }

    #[test]
    fn notices_before_the_handling_merge_into_the_newest_version() {
        let mut window = NoticeWindow::default();
        assert_eq!(window.notice(pending(1), 0, 2_000, SPREAD), Some(2_000));
        assert_eq!(window.notice(pending(3), 500, 0, SPREAD), None);
        assert_eq!(
            window.notice(
                Pending {
                    version: 2,
                    tenant_wide: true
                },
                1_900,
                0,
                SPREAD
            ),
            None
        );
        assert_eq!(
            window.take(2_000),
            Some(Pending {
                version: 3,
                tenant_wide: true
            })
        );
        assert_eq!(window.take(2_001), None);
    }

    #[test]
    fn handlings_are_a_spread_apart() {
        let mut window = NoticeWindow::default();
        assert_eq!(window.notice(pending(1), 0, 100, SPREAD), Some(100));
        assert_eq!(window.take(100), Some(pending(1)));
        // 0.4 s after the handling: wait out the rest of the spread.
        assert_eq!(window.notice(pending(2), 500, 0, SPREAD), Some(2_600));
        assert_eq!(window.take(3_100), Some(pending(2)));
        // Long after: only the jitter.
        assert_eq!(window.notice(pending(3), 60_000, 700, SPREAD), Some(700));
    }

    fn capability_api() -> CapabilityApi {
        CapabilityApi::new(
            "http://127.0.0.1:1",
            Arc::new(RwLock::new("u1".to_string())),
            "201",
            Arc::new(HttpRequestContext::new()),
        )
    }

    fn access() -> Arc<AccessRuntime> {
        Arc::new(AccessRuntime::new(
            None,
            crate::runtime::link::AccessStoreSource::memory(),
        ))
    }

    fn notice(topic: &str, version: u64) -> TenantNotice {
        TenantNotice {
            topic: topic.into(),
            version,
            spread_ms: 3_000,
            tenant_wide: false,
        }
    }

    /// The bus hands events to subscribers on a blocking thread and the catalog
    /// invalidation tries real HTTP; neither moves with the paused clock. Give
    /// them real time, bounded.
    async fn settle(done: impl Fn() -> bool) {
        for _ in 0..500 {
            if done() {
                break;
            }
            tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(2)))
                .await
                .unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_catalog_notice_invalidates_the_catalog_then_announces_once_after_the_jitter() {
        let api = capability_api();
        let bus = EventBus::new();
        let announced = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = announced.clone();
        let _subscription = bus.on_event_type(
            SdkEventType::Plugin(PluginEventType::CatalogChanged),
            move |event| {
                if let SdkEvent::Plugin(PluginEvent::CatalogChanged { version }) = event.as_ref() {
                    seen.lock().unwrap().push(version.clone());
                }
            },
        );
        let notices = TenantNotices::with_jitter(api.clone(), access(), bus.clone(), |_| 1_500);
        let generation = api.catalog_flight.generation();

        notices.notice(notice(PLUGIN_CATALOG_TOPIC, 7));
        assert!(
            api.catalog_flight.generation() > generation,
            "the catalog is invalidated before the announcement"
        );
        notices.notice(notice(PLUGIN_CATALOG_TOPIC, 8));

        tokio::time::sleep(Duration::from_millis(1_400)).await;
        assert!(
            announced.lock().unwrap().is_empty(),
            "nothing before the jitter"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        settle(|| !announced.lock().unwrap().is_empty()).await;
        assert_eq!(*announced.lock().unwrap(), vec!["8".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn an_extension_topic_is_announced_as_an_extension_event_named_by_it() {
        let api = capability_api();
        let bus = EventBus::new();
        let announced = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let seen = announced.clone();
        let _subscription = bus.on_event_type(
            SdkEventType::Extension(ExtensionEventType::named(
                TENANT_NOTICE_SOURCE,
                "social.features",
            )),
            move |event| {
                if let SdkEvent::Extension(extension) = event.as_ref() {
                    seen.lock()
                        .unwrap()
                        .push(serde_json::from_slice(&extension.payload).unwrap());
                }
            },
        );
        let notices = TenantNotices::with_jitter(api.clone(), access(), bus.clone(), |_| 0);
        let generation = api.catalog_flight.generation();
        notices.notice(TenantNotice {
            tenant_wide: true,
            ..notice("social.features", 12)
        });
        notices.notice(notice("social.features", 0));
        assert_eq!(
            api.catalog_flight.generation(),
            generation,
            "another topic leaves the catalog alone"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
        settle(|| !announced.lock().unwrap().is_empty()).await;
        assert_eq!(
            *announced.lock().unwrap(),
            vec![serde_json::json!({"version": "12", "tenantWide": true})]
        );
    }
}
