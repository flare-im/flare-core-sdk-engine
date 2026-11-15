//! 推送唤醒，以及进后台时等托管交接收尾。

use super::*;

/// 三轮后台重连要等两次退避（各自最长几秒）。
const LONG: Duration = Duration::from_secs(12);

async fn online(platform: &str) -> Harness {
    let mut h = harness_on(platform);
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    h
}

/// iOS 在线后进后台，等托管完成。
async fn parked_ios() -> Harness {
    let h = online("ios").await;
    h.link.app_state(false).await;
    h.wait_driver("parking", SHORT, |driver| {
        driver.closes().contains(&"parked") && !driver.outbox_running()
    })
    .await;
    h
}

/// 已托管时推送唤醒：按后台状态（没有后台展示方，DEFERRED）连上、补同步；预算用完照进后台时那样
/// 交接、按托管关闭，之后不再连。
#[tokio::test]
async fn a_wake_connects_a_parked_link_and_parks_it_again() {
    let h = parked_ios().await;

    h.link.wake(Duration::from_millis(1_000)).await;
    h.wait_link_ups(2, SHORT).await;
    assert_eq!(h.driver.opened_foreground(), vec![true, false]);
    assert_eq!(
        h.driver.opened_states().last(),
        Some(&DeviceLinkState::Deferred)
    );
    assert!(matches!(
        h.driver.link_ups().last(),
        Some(LinkUp::Reconnected { .. })
    ));
    assert_eq!(h.driver.hand_overs(), 1, "预算没到之前链路留着");

    h.wait_driver("parking again", SHORT, |driver| {
        driver.hand_overs() == 2 && driver.closes() == vec!["parked", "parked"]
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h.driver.opened().len(), 2, "重新托管之后不再连");
    assert!(!h.driver.outbox_running());
}

/// 同一时间只有一次唤醒：在跑的那次不因新的唤醒延长，也不多连一次。
#[tokio::test]
async fn a_second_wake_while_one_runs_is_ignored() {
    let h = parked_ios().await;

    h.link.wake(Duration::from_millis(800)).await;
    h.wait_link_ups(2, SHORT).await;
    h.link.wake(Duration::from_secs(20)).await;
    h.wait_driver("parking again", SHORT, |driver| driver.hand_overs() == 2)
        .await;
    assert_eq!(h.driver.opened().len(), 2);
}

/// 唤醒途中回到前台：唤醒到此为止，连接留着并报 ACTIVE，预算到了也不再托管。
#[tokio::test]
async fn the_foreground_ends_a_wake() {
    let h = parked_ios().await;

    h.link.wake(Duration::from_millis(500)).await;
    h.wait_link_ups(2, SHORT).await;
    h.link.app_state(true).await;
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(h.driver.hand_overs(), 1);
    assert_eq!(h.driver.closes(), vec!["parked"]);
    assert_eq!(h.driver.reports().last(), Some(&DeviceLinkState::Active));
    assert_eq!(h.link.status().phase, ConnectionPhase::Online);
    assert!(h.driver.outbox_running());
}

/// 后台刷新：唤醒托管的链路，补完同步时答 `true`；提前结束就像预算到了一样重新交接、托管。
#[tokio::test]
async fn a_background_refresh_waits_for_the_sync_and_parks_when_it_ends() {
    let h = parked_ios().await;

    let ready = h
        .link
        .wake_until_ready(WakeSource::BackgroundRefresh, Duration::from_secs(5))
        .await;
    assert!(ready, "连上并补完同步");
    assert_eq!(h.driver.opened().len(), 2);
    assert_eq!(h.driver.hand_overs(), 1, "还没结束之前链路留着");

    h.link.end_wake().await;
    h.wait_driver("parking again", SHORT, |driver| {
        driver.hand_overs() == 2 && driver.closes() == vec!["parked", "parked"]
    })
    .await;
    tokio::time::timeout(SHORT, h.link.parking_settled())
        .await
        .expect("settles after the hand-over");
}

/// 后台刷新等不到同步（连不上）：预算用完答 `false`；没有会话时立即答 `false`；链路本来就在线时立即
/// 答 `true`。
#[tokio::test]
async fn a_background_refresh_reports_whether_it_completed() {
    let h = parked_ios().await;
    h.driver.set_unreachable(true);
    let ready = h
        .link
        .wake_until_ready(WakeSource::BackgroundRefresh, Duration::from_millis(600))
        .await;
    assert!(!ready, "连不上就没有完成");

    let idle = harness_on("ios");
    let ready = tokio::time::timeout(
        Duration::from_millis(500),
        idle.link
            .wake_until_ready(WakeSource::BackgroundRefresh, Duration::from_secs(5)),
    )
    .await
    .expect("answers at once without a session");
    assert!(!ready);

    let online = online("android").await;
    online.link.app_state(false).await;
    let ready = tokio::time::timeout(
        Duration::from_millis(500),
        online
            .link
            .wake_until_ready(WakeSource::BackgroundRefresh, Duration::from_secs(5)),
    )
    .await
    .expect("answers at once while online");
    assert!(ready);
    online.link.end_wake().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(online.driver.closes().is_empty(), "保留链路的平台不关");
}

/// 前台不唤醒：链路本来就按前台的做法收消息。
#[tokio::test]
async fn a_wake_in_the_foreground_does_nothing() {
    let h = online("ios").await;

    h.link.wake(Duration::from_millis(200)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(h.driver.opened().len(), 1);
    assert_eq!(h.driver.hand_overs(), 0);
    assert!(h.driver.closes().is_empty());
}

/// 保留链路的手机在后台用完重连预算停下：唤醒立即再连一轮（带后台状态）；预算到了链路留着。
#[tokio::test]
async fn a_wake_restarts_a_phone_stopped_in_the_background_and_keeps_the_link() {
    let mut h = online("android").await;
    h.link.app_state(false).await;
    h.wait_driver("the background report", SHORT, |driver| {
        driver.reports().len() >= 2
    })
    .await;
    h.driver.set_unreachable(true);
    h.lose(h.current_epoch(), DisconnectReason::HeartbeatTimeout);
    h.wait_status("the link to stop retrying", LONG, |status| {
        status.phase == ConnectionPhase::ServiceUnavailable && status.next_retry_at.is_none()
    })
    .await;
    let opened = h.driver.opened().len();

    h.driver.set_unreachable(false);
    h.link.wake(Duration::from_millis(500)).await;
    h.wait_phase(ConnectionPhase::Online, SHORT).await;
    assert_eq!(h.driver.opened().len(), opened + 1);
    assert_eq!(
        h.driver.opened_states().last(),
        Some(&DeviceLinkState::Deferred)
    );

    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(h.driver.closes().is_empty(), "保留链路的平台预算到了也不关");
    assert_eq!(h.driver.hand_overs(), 0);
    assert_eq!(h.driver.opened().len(), opened + 1);
    assert_eq!(h.link.status().phase, ConnectionPhase::Online);
}

/// 托管的平台：进后台的调用方等交接收尾才返回，返回时连接已经按托管关掉。
#[tokio::test]
async fn going_to_the_background_settles_after_the_hand_over() {
    let h = online("ios").await;
    let gate = h.driver.hold_hand_over();

    h.link.app_state(false).await;
    let link = h.link.clone();
    let settled = tokio::spawn(async move { link.parking_settled().await });
    h.wait_driver("the hand-over", SHORT, |driver| driver.hand_overs() == 1)
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!settled.is_finished(), "交接没完不返回");

    gate.add_permits(1);
    tokio::time::timeout(SHORT, settled)
        .await
        .expect("settles once the hand-over is done")
        .unwrap();
    assert_eq!(h.driver.closes(), vec!["parked"]);
}

/// 交接中用户回来了：等着的调用方随即返回，连接留着。
#[tokio::test]
async fn returning_during_the_hand_over_settles_the_waiter() {
    let h = online("ios").await;
    let _gate = h.driver.hold_hand_over();

    h.link.app_state(false).await;
    let link = h.link.clone();
    let settled = tokio::spawn(async move { link.parking_settled().await });
    h.wait_driver("the hand-over", SHORT, |driver| driver.hand_overs() == 1)
        .await;
    h.link.app_state(true).await;
    tokio::time::timeout(SHORT, settled)
        .await
        .expect("settles when the hand-over is dropped")
        .unwrap();
    assert!(h.driver.closes().is_empty());
}

/// 没有连接可交接（正在重连）时进后台直接停靠：调用方不必等。保留链路的平台从来不等。
#[tokio::test]
async fn nothing_to_hand_over_settles_at_once() {
    let mut h = harness_on("ios");
    h.driver.set_unreachable(true);
    h.link.start("alice", Tokens::new("t1", [])).await.unwrap();
    h.wait_phase(ConnectionPhase::Reconnecting, SHORT).await;
    h.link.app_state(false).await;
    tokio::time::timeout(Duration::from_millis(500), h.link.parking_settled())
        .await
        .expect("parks without a hand-over");
    assert_eq!(h.driver.hand_overs(), 0);

    let h = online("android").await;
    h.link.app_state(false).await;
    tokio::time::timeout(Duration::from_millis(100), h.link.parking_settled())
        .await
        .expect("a standby platform returns at once");
}
