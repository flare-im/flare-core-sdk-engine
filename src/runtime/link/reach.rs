//! 设备可达状态：这台设备此刻由谁负责把新消息提示给用户。
//!
//! 服务端不再把「帧写进了套接字」当成「送到了」：每条需要提示的下行帧都要等设备确认，等不到就在秒级
//! 内转给推送通道。所以引擎确认一条消息，就是在承诺「这条由我提示」；自己提示不了的时候（手机进了
//! 后台、链路已交给推送通道）必须不确认，让推送去提示。提示不了这件事设备自己最清楚，所以它直接
//! 报给服务端（`DEFERRED`），服务端就不必每条消息都等到确认超时才去推送。
//!
//! 这里只放各处共用的事实与判定：平台进后台后的做法、宿主最近报的前后台、后台展示方接没接上、托管
//! 交接走到哪一步，以及由它们得出的设备状态。何时上报、何时托管由链路状态机决定。

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use flare_proto::common::{DeviceAppState, DeviceAttention, DeviceStateReport};
use tokio::sync::Notify;

use super::driver::DeviceLinkState;
use crate::application::notification::{AlertReach, AlertSurface};

/// 应用进后台后链路怎么办。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackgroundMode {
    /// 保留链路（Android、鸿蒙、桌面、Web）：进程还活着时照常收消息。报 `STANDBY` 还是 `DEFERRED`
    /// 看本进程在后台能不能自己提示，见 [`DeviceReach::background_state`]。
    Standby,
    /// 托管（iOS）：系统几秒内就挂起进程，而内核还会替它应答 TCP，连接看起来一直活着。
    /// 所以进后台就把提示交给推送通道，再主动关掉链路。
    Park,
}

/// 一个平台的可达特征。平台取 CONNECT 里报的那个值（`ios` / `android` / `harmonyos` / `pc` / `web`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReachProfile {
    pub background: BackgroundMode,
    /// 手机系统：进了后台的进程随时被冻结或回收，那之后的提示靠推送通道。所以后台里没接上后台
    /// 展示方时报 `DEFERRED`、不确认消息，断线后也只自己重连有限的几轮（接没接上展示方都一样）。
    /// 桌面与 Web 的进程在后台照常运行，也没有推送可以接手，这两条都不适用。
    pub mobile: bool,
}

impl ReachProfile {
    pub fn for_platform(platform: &str) -> Self {
        match platform {
            "ios" => Self {
                background: BackgroundMode::Park,
                mobile: true,
            },
            // 推送令牌登记用 `harmony`，CONNECT 用 `harmonyos`，是同一个平台。
            "android" | "harmony" | "harmonyos" => Self {
                background: BackgroundMode::Standby,
                mobile: true,
            },
            _ => Self {
                background: BackgroundMode::Standby,
                mobile: false,
            },
        }
    }
}

/// `DEVICE_STATE` 的载荷。前台才算「在看」；空闲检测还没有，后台一律不报注意力。
pub(crate) fn device_state_report(
    state: DeviceLinkState,
    badge: u32,
    reported_at_ms: i64,
) -> DeviceStateReport {
    let (app_state, attention) = match state {
        DeviceLinkState::Active => (DeviceAppState::Active, DeviceAttention::Attentive),
        DeviceLinkState::Standby => (DeviceAppState::Standby, DeviceAttention::Unspecified),
        DeviceLinkState::Deferred => (DeviceAppState::Deferred, DeviceAttention::Unspecified),
        DeviceLinkState::Parking => (DeviceAppState::Parking, DeviceAttention::Unspecified),
    };
    DeviceStateReport {
        state: app_state as i32,
        attention: attention as i32,
        badge,
        reported_at: reported_at_ms,
    }
}

/// 没在托管。
const PARKING_NONE: u8 = 0;
/// 已决定托管、`PARKING` 正在送出或等应答。
const PARKING_PENDING: u8 = 1;
/// 服务端已应答 `PARKING`：之后它不再往这条连接写需要提示的帧，还到的帧不确认。
const PARKING_ACKED: u8 = 2;

/// 引擎内共享的可达状态：链路驱动、连接监听器、下行分发各持一份引用，读写都不加锁。
pub(crate) struct DeviceReach {
    profile: ReachProfile,
    /// 宿主最近报的前后台。在 `set_app_state` 的入口就写，不等链路状态机处理到那条命令：
    /// 手机一进后台，之后到的消息就不能再确认。
    foreground: AtomicBool,
    /// 后台展示方（平台推送包的本地通知）是否已接上。
    background_presenter: AtomicBool,
    parking: AtomicU8,
    parking_acked: Notify,
}

impl DeviceReach {
    pub fn new(profile: ReachProfile) -> Self {
        Self {
            profile,
            foreground: AtomicBool::new(true),
            background_presenter: AtomicBool::new(false),
            parking: AtomicU8::new(PARKING_NONE),
            parking_acked: Notify::new(),
        }
    }

    pub fn profile(&self) -> ReachProfile {
        self.profile
    }

    pub fn set_foreground(&self, foreground: bool) {
        self.foreground.store(foreground, Ordering::Release);
    }

    pub fn set_background_presenter(&self, attached: bool) {
        self.background_presenter.store(attached, Ordering::Release);
    }

    /// 宿主最近报的是不是前台。
    pub fn is_foreground(&self) -> bool {
        self.foreground.load(Ordering::Acquire)
    }

    /// 应用在后台、链路保留时的设备状态：本进程能自己提示就是 `STANDBY`（桌面、Web，或接上了后台
    /// 展示方的手机），提示不了就是 `DEFERRED`（手机没有后台展示方），提示交给推送。
    pub fn background_state(&self) -> DeviceLinkState {
        if self.profile.mobile && !self.background_presenter.load(Ordering::Acquire) {
            DeviceLinkState::Deferred
        } else {
            DeviceLinkState::Standby
        }
    }

    /// 链路保留时本进程此刻的设备状态，按宿主最近报的前后台。
    pub fn link_state(&self) -> DeviceLinkState {
        if self.foreground.load(Ordering::Acquire) {
            DeviceLinkState::Active
        } else {
            self.background_state()
        }
    }

    /// 此刻从链路到达的消息由不由本进程确认：谁确认谁负责提示。
    ///
    /// 只在 `ACTIVE` 与 `STANDBY` 确认，这两个状态下提示归本进程。`DEFERRED` 时不确认：服务端已经
    /// 知道提示归推送，也不等确认。托管应答之后到的帧一律不确认。
    pub fn confirms_deliveries(&self) -> bool {
        if self.parking.load(Ordering::Acquire) == PARKING_ACKED {
            return false;
        }
        matches!(
            self.link_state(),
            DeviceLinkState::Active | DeviceLinkState::Standby
        )
    }

    /// 开始托管交接：接下来送出 `PARKING` 并等它的应答。
    pub fn begin_parking(&self) {
        self.parking.store(PARKING_PENDING, Ordering::Release);
    }

    /// 托管作废：用户回到前台、上报了别的状态，或换了一条新连接。之后才到的 `PARKING` 应答不算数。
    pub fn end_parking(&self) {
        self.parking.store(PARKING_NONE, Ordering::Release);
    }

    /// `PARKING` 还该不该送出（没被作废、也还没被应答）。
    pub fn parking_pending(&self) -> bool {
        self.parking.load(Ordering::Acquire) == PARKING_PENDING
    }

    /// 服务端应答了一次状态上报。只有等着的 `PARKING` 被应答才有意义。
    pub fn on_state_ack(&self, state: DeviceAppState) {
        if state != DeviceAppState::Parking {
            return;
        }
        if self
            .parking
            .compare_exchange(
                PARKING_PENDING,
                PARKING_ACKED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            // 存一个许可：等的一方还没开始等也不会错过。
            self.parking_acked.notify_one();
        }
    }

    /// 等到 `PARKING` 被应答。调用方自己加时限；丢弃即取消。
    pub async fn parking_acked(&self) {
        loop {
            if self.parking.load(Ordering::Acquire) == PARKING_ACKED {
                return;
            }
            // 上一次托管留下的许可只会让这里多转一圈。
            self.parking_acked.notified().await;
        }
    }
}

/// 提示决策点问「此刻在哪里提示」：与送达确认同一个判定。本进程确认的消息由本进程提示，前台在应用内，
/// 后台在系统通知；不确认的（手机后台没有展示方、托管已应答）提示归推送，这里不提示。
impl AlertReach for DeviceReach {
    fn alert_surface(&self) -> Option<AlertSurface> {
        if !self.confirms_deliveries() {
            return None;
        }
        Some(if self.is_foreground() {
            AlertSurface::InApp
        } else {
            AlertSurface::System
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    fn reach(platform: &str) -> DeviceReach {
        DeviceReach::new(ReachProfile::for_platform(platform))
    }

    /// 只有 iOS 进后台托管；Android 与鸿蒙保留链路但算手机；桌面与 Web 不算。
    #[test]
    fn platforms_map_onto_their_background_behaviour() {
        assert_eq!(
            ReachProfile::for_platform("ios"),
            ReachProfile {
                background: BackgroundMode::Park,
                mobile: true,
            }
        );
        for phone in ["android", "harmony", "harmonyos"] {
            assert_eq!(
                ReachProfile::for_platform(phone),
                ReachProfile {
                    background: BackgroundMode::Standby,
                    mobile: true,
                },
                "{phone}"
            );
        }
        for other in ["pc", "web", "h5", "something-else", ""] {
            assert_eq!(
                ReachProfile::for_platform(other),
                ReachProfile {
                    background: BackgroundMode::Standby,
                    mobile: false,
                },
                "{other}"
            );
        }
    }

    /// 后台状态：手机没有后台展示方是 DEFERRED，接上了是 STANDBY；桌面与 Web 总是 STANDBY。
    /// 前台都是 ACTIVE。
    #[test]
    fn the_background_state_follows_the_platform_and_the_presenter() {
        for phone in ["ios", "android", "harmonyos"] {
            let reach = reach(phone);
            assert_eq!(reach.link_state(), DeviceLinkState::Active, "{phone}");
            reach.set_foreground(false);
            assert_eq!(reach.link_state(), DeviceLinkState::Deferred, "{phone}");
            reach.set_background_presenter(true);
            assert_eq!(reach.link_state(), DeviceLinkState::Standby, "{phone}");
            assert_eq!(
                reach.background_state(),
                DeviceLinkState::Standby,
                "{phone}"
            );
            reach.set_foreground(true);
            assert_eq!(reach.link_state(), DeviceLinkState::Active, "{phone}");
        }
        for desktop in ["pc", "web"] {
            let reach = reach(desktop);
            reach.set_foreground(false);
            assert_eq!(reach.link_state(), DeviceLinkState::Standby, "{desktop}");
            reach.set_background_presenter(true);
            assert_eq!(reach.link_state(), DeviceLinkState::Standby, "{desktop}");
        }
    }

    /// 在哪里提示：前台一律应用内；后台的桌面与 Web、接上展示方的手机出系统通知；后台没有展示方的
    /// 手机与托管应答之后不提示（归推送）。
    #[test]
    fn the_alert_surface_follows_who_confirms() {
        for platform in ["ios", "android", "harmonyos", "pc", "web"] {
            let reach = reach(platform);
            assert_eq!(
                reach.alert_surface(),
                Some(AlertSurface::InApp),
                "{platform}"
            );
            reach.set_foreground(false);
            let expected = if reach.profile().mobile {
                None
            } else {
                Some(AlertSurface::System)
            };
            assert_eq!(reach.alert_surface(), expected, "{platform}");
            reach.set_background_presenter(true);
            assert_eq!(
                reach.alert_surface(),
                Some(AlertSurface::System),
                "{platform}"
            );
            reach.begin_parking();
            reach.on_state_ack(DeviceAppState::Parking);
            assert_eq!(reach.alert_surface(), None, "{platform} parked");
        }
    }

    /// 谁确认谁负责提示：只在 ACTIVE 与 STANDBY 确认。前台都确认；后台手机（DEFERRED）不确认，接上
    /// 后台展示方（STANDBY）才确认；桌面与 Web 后台照常确认。
    #[test]
    fn a_backgrounded_phone_confirms_only_with_a_presenter() {
        for phone in ["ios", "android", "harmonyos"] {
            let reach = reach(phone);
            assert!(reach.confirms_deliveries(), "{phone} in the foreground");
            reach.set_foreground(false);
            assert!(!reach.confirms_deliveries(), "{phone} in the background");
            reach.set_background_presenter(true);
            assert!(reach.confirms_deliveries(), "{phone} with a presenter");
            reach.set_background_presenter(false);
            reach.set_foreground(true);
            assert!(
                reach.confirms_deliveries(),
                "{phone} back in the foreground"
            );
        }
        for desktop in ["pc", "web"] {
            let reach = reach(desktop);
            reach.set_foreground(false);
            assert!(reach.confirms_deliveries(), "{desktop} in the background");
        }
    }

    /// 托管应答之后到的帧不确认，哪怕后台展示方已接上；作废托管后恢复。
    #[test]
    fn nothing_is_confirmed_after_the_parking_ack() {
        let reach = reach("ios");
        reach.set_foreground(false);
        reach.set_background_presenter(true);
        reach.begin_parking();
        assert!(reach.parking_pending());
        assert!(reach.confirms_deliveries(), "应答之前到的帧照常确认");

        reach.on_state_ack(DeviceAppState::Parking);
        assert!(!reach.parking_pending());
        assert!(!reach.confirms_deliveries());

        reach.end_parking();
        assert!(reach.confirms_deliveries());
    }

    /// 没在等的应答不算数：别的状态的应答、托管作废之后才到的 `PARKING` 应答。
    #[test]
    fn a_stray_ack_does_not_hand_the_link_over() {
        let reach = reach("ios");
        reach.on_state_ack(DeviceAppState::Parking);
        assert!(reach.confirms_deliveries());

        reach.begin_parking();
        reach.on_state_ack(DeviceAppState::Active);
        reach.on_state_ack(DeviceAppState::Standby);
        assert!(reach.parking_pending());

        reach.end_parking();
        reach.on_state_ack(DeviceAppState::Parking);
        assert!(
            reach.confirms_deliveries(),
            "用户已经回来，迟到的应答不能让前台停止确认"
        );
    }

    /// 等应答：先到后等、先等后到都能等到；上一次托管留下的许可不会让下一次提前返回。
    #[tokio::test]
    async fn the_parking_ack_wakes_the_waiter() {
        let reach = Arc::new(reach("ios"));
        reach.begin_parking();
        reach.on_state_ack(DeviceAppState::Parking);
        tokio::time::timeout(Duration::from_secs(1), reach.parking_acked())
            .await
            .expect("an ack that came first is not missed");

        // 上一次的许可还留着：新一次托管不能凭它返回。
        reach.end_parking();
        reach.begin_parking();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), reach.parking_acked())
                .await
                .is_err(),
            "no ack yet for the second hand-over"
        );
        let waiter = {
            let reach = reach.clone();
            tokio::spawn(async move { reach.parking_acked().await })
        };
        tokio::task::yield_now().await;
        reach.on_state_ack(DeviceAppState::Parking);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the waiter is woken")
            .unwrap();
    }

    /// 上报载荷：前台 ACTIVE + ATTENTIVE，后台 STANDBY、DEFERRED 或 PARKING 且不报注意力。
    #[test]
    fn reports_carry_the_state_attention_and_badge() {
        let active = device_state_report(DeviceLinkState::Active, 7, 1_700_000_000_000);
        assert_eq!(active.state(), DeviceAppState::Active);
        assert_eq!(active.attention(), DeviceAttention::Attentive);
        assert_eq!(active.badge, 7);
        assert_eq!(active.reported_at, 1_700_000_000_000);

        let standby = device_state_report(DeviceLinkState::Standby, 0, 1);
        assert_eq!(standby.state(), DeviceAppState::Standby);
        assert_eq!(standby.attention(), DeviceAttention::Unspecified);

        let deferred = device_state_report(DeviceLinkState::Deferred, 5, 1);
        assert_eq!(deferred.state(), DeviceAppState::Deferred);
        assert_eq!(deferred.attention(), DeviceAttention::Unspecified);
        assert_eq!(deferred.badge, 5);

        let parking = device_state_report(DeviceLinkState::Parking, 3, 1);
        assert_eq!(parking.state(), DeviceAppState::Parking);
        assert_eq!(parking.attention(), DeviceAttention::Unspecified);
        assert_eq!(parking.badge, 3);
    }
}
