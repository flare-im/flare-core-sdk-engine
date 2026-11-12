//! 设备状态上报、进后台托管（iOS）与后台重连预算。

use super::*;

/// 三轮后台重连要等两次退避（各自最长几秒）。
const LONG: Duration = Duration::from_secs(12);

async fn online(platform: &str) -> Harness {
    let mut h = harness_on(platform);
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    h
}

/// 等到上报过 `count` 次设备状态。
async fn wait_reports(h: &Harness, count: usize) {
    h.wait_driver("the device state report", SHORT, |driver| {
        driver.reports().len() >= count
    })
    .await;
}

/// 托管完成：连接按托管关掉了，发件箱已暂停。
async fn wait_parked(h: &Harness) {
    h.wait_driver("parking", SHORT, |driver| {
        driver.closes().contains(&"parked") && !driver.outbox_running()
    })
    .await;
}

fn drain(h: &mut Harness) {
    while h.rx.try_recv().is_ok() {}
}

/// 每次建连之后、每次前后台变化都把设备状态报给服务端；重复的前后台通知不重复上报。
/// 手机在后台没有后台展示方，报的是 DEFERRED。
#[tokio::test]
async fn the_device_state_is_reported_on_connect_and_on_every_change() {
    use DeviceLinkState::{Active, Deferred};

    let h = online("android").await;
    assert_eq!(h.driver.opened_states(), vec![Active]);
    assert_eq!(h.driver.reports(), vec![Active]);

    h.link.app_state(false).await;
    h.link.app_state(false).await;
    h.link.app_state(true).await;
    wait_reports(&h, 3).await;
    assert_eq!(h.driver.reports(), vec![Active, Deferred, Active]);
    assert_eq!(h.driver.hand_overs(), 0, "保留链路的平台进后台不托管");
    assert!(h.driver.closes().is_empty());
    assert_eq!(h.driver.opened().len(), 1);
}

/// 桌面与 Web 在后台自己提示：报的是 STANDBY，与后台展示方无关。
#[tokio::test]
async fn a_desktop_reports_standby_in_the_background() {
    use DeviceLinkState::{Active, Standby};

    let h = online("pc").await;
    h.link.app_state(false).await;
    wait_reports(&h, 2).await;
    assert_eq!(h.driver.reports(), vec![Active, Standby]);
}

/// 后台展示方接上或卸下：应用在后台时重新上报（STANDBY / DEFERRED）；在前台时状态没变，不报。
#[tokio::test]
async fn a_presenter_change_is_reported_while_in_the_background() {
    use DeviceLinkState::{Active, Deferred, Standby};

    let h = online("android").await;
    h.set_background_presenter(true).await;
    h.set_background_presenter(false).await;
    h.link.app_state(false).await;
    wait_reports(&h, 2).await;
    assert_eq!(
        h.driver.reports(),
        vec![Active, Deferred],
        "前台的变化不上报"
    );

    // 一次一变：上报读的是处理到那一刻的展示方状态，连着变两次可能只看到后一次。
    h.set_background_presenter(true).await;
    wait_reports(&h, 3).await;
    h.set_background_presenter(false).await;
    wait_reports(&h, 4).await;
    assert_eq!(
        h.driver.reports(),
        vec![Active, Deferred, Standby, Deferred]
    );

    // 接着展示方在后台重连：CONNECT 里带的与连上后报的都是 STANDBY。
    h.set_background_presenter(true).await;
    wait_reports(&h, 5).await;
    h.lose(h.current_epoch(), DisconnectReason::HeartbeatTimeout);
    h.wait_link_ups(2, SHORT).await;
    assert_eq!(h.driver.opened_states(), vec![Active, Standby]);
    assert_eq!(h.driver.reports().last(), Some(&Standby));
}

/// iOS 进后台：交接、按托管关闭、暂停发件箱，对外状态不变；之后不重连，网络变化也不动它。
/// 回到前台立即重连并补同步。
#[tokio::test]
async fn a_parking_platform_hands_the_link_over_in_the_background() {
    let mut h = online("ios").await;
    drain(&mut h);

    h.link.app_state(false).await;
    wait_parked(&h).await;
    assert_eq!(h.driver.hand_overs(), 1);
    assert_eq!(
        h.driver.reports(),
        vec![DeviceLinkState::Active],
        "PARKING 由交接自己上报"
    );

    h.link.network_changed(false, false).await;
    h.link.network_changed(true, true).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.driver.opened().len(), 1, "停靠时不重连，网络变化也不动它");
    assert_eq!(h.link.status().phase, ConnectionPhase::Online);
    assert!(h.rx.try_recv().is_err(), "在线时托管不产生新的状态事件");

    h.link.app_state(true).await;
    h.wait_link_ups(2, SHORT).await;
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    assert_eq!(h.driver.opened_foreground(), vec![true, true]);
    assert_eq!(
        h.driver.opened_states().last(),
        Some(&DeviceLinkState::Active)
    );
    assert!(matches!(
        h.driver.link_ups().last(),
        Some(LinkUp::Reconnected {
            lost_at_ms: Some(_)
        })
    ));
    assert_eq!(h.driver.reports().last(), Some(&DeviceLinkState::Active));
    assert!(h.driver.outbox_running());
}

/// 交接到一半用户回来了：放弃托管，连接留着，报回 ACTIVE。再进后台照常托管。
#[tokio::test]
async fn returning_during_the_hand_over_keeps_the_link() {
    let h = online("ios").await;
    let gate = h.driver.hold_hand_over();

    h.link.app_state(false).await;
    h.wait_driver("the hand-over", SHORT, |driver| driver.hand_overs() == 1)
        .await;
    // 交接中展示方变了也不上报：再报别的状态会把托管作废。
    h.set_background_presenter(true).await;
    h.set_background_presenter(false).await;
    h.link.app_state(true).await;
    wait_reports(&h, 2).await;
    assert_eq!(
        h.driver.reports(),
        vec![DeviceLinkState::Active, DeviceLinkState::Active]
    );
    assert!(h.driver.closes().is_empty(), "连接没有被关");
    assert_eq!(h.driver.opened().len(), 1);
    assert_eq!(h.link.status().phase, ConnectionPhase::Online);
    assert!(h.driver.outbox_running());

    gate.add_permits(1);
    h.link.app_state(false).await;
    wait_parked(&h).await;
    assert_eq!(h.driver.hand_overs(), 2);
}

/// 已托管时显式重连照样连上（应用还在后台、没有后台展示方，按 DEFERRED 建连）；那条连接再断仍旧
/// 停靠，不自己重连。
#[tokio::test]
async fn an_explicit_retry_reconnects_a_parked_link() {
    use DeviceLinkState::{Active, Deferred};

    let h = online("ios").await;
    h.link.app_state(false).await;
    wait_parked(&h).await;

    h.link.reconnect_now().await;
    h.wait_link_ups(2, SHORT).await;
    assert_eq!(h.driver.opened_foreground(), vec![true, false]);
    assert_eq!(h.driver.opened_states(), vec![Active, Deferred]);
    assert_eq!(h.driver.reports().last(), Some(&Deferred));

    h.lose(h.current_epoch(), DisconnectReason::HeartbeatTimeout);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.driver.opened().len(), 2, "后台丢了连接不自己重连");

    h.link.app_state(true).await;
    h.wait_link_ups(3, SHORT).await;
    assert_eq!(h.driver.opened_states().last(), Some(&Active));
}

/// 没有连接可交接时进后台（正在重连）也停靠：不留重试定时，回到前台再连。
#[tokio::test]
async fn a_parking_platform_stops_retrying_in_the_background() {
    let mut h = harness_on("ios");
    h.driver.set_unreachable(true);
    h.link
        .start("alice", Tokens::new("t1", []))
        .await
        .expect("连不上时链路保留会话，登录照常成功");
    h.wait_phase(ConnectionPhase::Reconnecting, SHORT).await;

    h.link.app_state(false).await;
    h.wait_driver("parking", SHORT, |driver| driver.link_downs() >= 1)
        .await;
    let opened = h.driver.opened().len();
    // 首次退避最长 1.5 秒：过了它还没有新的建连，说明定时已经撤掉。
    tokio::time::sleep(Duration::from_millis(1_800)).await;
    assert_eq!(h.driver.opened().len(), opened, "停靠时没有重试定时");
    assert_eq!(h.driver.hand_overs(), 0, "没有连接就没有可交接的");
    assert_eq!(h.driver.closes(), vec!["close"]);

    h.driver.set_unreachable(false);
    drain(&mut h);
    h.link.app_state(true).await;
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    assert_eq!(h.driver.link_ups(), vec![LinkUp::First]);
}

/// 保留链路的手机（Android）在后台断线：最多自己重连三轮，然后停下（没有下一次重试的时刻），不再建连。
/// 预算与它在后台报哪个状态无关：`presenter` 为真时报的是 STANDBY，否则是 DEFERRED。
async fn stopped_in_the_background(presenter: bool) -> Harness {
    let mut h = online("android").await;
    h.reach.set_background_presenter(presenter);
    h.link.app_state(false).await;
    // 等状态机处理完进后台再断线：断线若先被处理，第一轮就不算后台的。
    wait_reports(&h, 2).await;
    let background = if presenter {
        DeviceLinkState::Standby
    } else {
        DeviceLinkState::Deferred
    };
    assert_eq!(h.driver.reports().last(), Some(&background));
    h.driver.set_unreachable(true);
    h.lose(h.current_epoch(), DisconnectReason::HeartbeatTimeout);

    let stopped = h
        .wait_status("the link to stop retrying", LONG, |status| {
            status.phase == ConnectionPhase::ServiceUnavailable && status.next_retry_at.is_none()
        })
        .await;
    assert_eq!(stopped.attempt, 3, "三轮之后停下");
    let opened = h.driver.opened().len();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(h.driver.opened().len(), opened, "停下之后不再建连");
    assert!(
        h.driver.opened_states()[1..]
            .iter()
            .all(|state| *state == background),
        "后台里的每次建连都带着后台状态"
    );
    drain(&mut h);
    h
}

/// 后台用完重连预算停下之后，回到前台立即再来一轮。
#[tokio::test]
async fn the_foreground_restarts_a_link_stopped_in_the_background() {
    let mut h = stopped_in_the_background(false).await;
    h.link.app_state(true).await;
    h.wait_phase(ConnectionPhase::Switching, SHORT).await;
}

/// 后台用完重连预算停下之后，网络恢复的通知也让它再来一轮（预算重新计）。接上了后台展示方的手机
/// （后台报 STANDBY）同样受这个预算约束。
#[tokio::test]
async fn a_network_event_restarts_a_link_stopped_in_the_background() {
    let mut h = stopped_in_the_background(true).await;
    h.link.network_changed(true, false).await;
    h.wait_phase(ConnectionPhase::Switching, SHORT).await;
}
