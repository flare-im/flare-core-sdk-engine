use super::*;
use crate::application::notification::AlertSkip;
use crate::content::message_elem::TextElem;
use crate::infrastructure::persistence::memory_im::in_memory_im_provider;
use crate::infrastructure::protocol::{PacketSender, ProtobufCodec};
use crate::kernel::event::EventReceiver;
use crate::model::notification::QuietHours;

/// 由测试决定此刻在哪里提示。
struct Reach(Mutex<Option<AlertSurface>>);

impl Reach {
    fn set(&self, surface: Option<AlertSurface>) {
        *self.0.lock().unwrap() = surface;
    }
}

impl AlertReach for Reach {
    fn alert_surface(&self) -> Option<AlertSurface> {
        *self.0.lock().unwrap()
    }
}

struct Harness {
    center: Arc<NotifyCenter>,
    reach: Arc<Reach>,
    alerts: Arc<AlertLedger>,
    stores: StoreProvider,
    events: EventReceiver,
}

fn harness(surface: Option<AlertSurface>) -> Harness {
    let bus = EventBus::new();
    let events = bus.subscribe_filter(EventFilter::Kind(SdkEventKind::Notification));
    let stores = in_memory_im_provider();
    let reach = Arc::new(Reach(Mutex::new(surface)));
    let alerts = Arc::new(AlertLedger::new());
    alerts.scope("0");
    // 没有连接：读写用户级设置的同步请求一律失败，决策用默认设置。
    let sender = Arc::new(PacketSender::new(
        Arc::new(tokio::sync::Mutex::new(None)),
        Arc::new(ProtobufCodec),
    ));
    let center = NotifyCenter::start(NotifyCenterParts {
        bus,
        stores: stores.clone(),
        reach: reach.clone(),
        alerts: alerts.clone(),
        sync: SyncRequestUseCase::new(sender, "device-1"),
        locale: Some("zh-CN".to_owned()),
    });
    Harness {
        center,
        reach,
        alerts,
        stores,
        events,
    }
}

fn text(conversation_id: &str, seq: u64, sender: &str, body: &str) -> IMMessage {
    let mut message = IMMessage::new(flare_proto::common::Message::default());
    message.server_id = format!("{conversation_id}-{seq}");
    message.conversation_id = conversation_id.to_owned();
    message.conversation_type = flare_proto::common::ConversationType::Group as i32;
    message.sender_id = sender.to_owned();
    message.sender_name = sender.to_uppercase();
    message.conversation_seq = seq;
    message.content = Some(Elem::Text(TextElem {
        text: body.to_owned(),
        mentions: vec![],
    }));
    message
}

fn group(conversation_id: &str, name: &str) -> Conversation {
    Conversation {
        conversation_id: conversation_id.to_owned(),
        conversation_type: ConversationType::Group,
        channel_id: conversation_id.to_owned(),
        display_name: name.to_owned(),
        members_count: 12,
        ..Default::default()
    }
}

/// 收下事件队列里的展示与撤掉（角标另有防抖，不在这里看）。
fn drain(events: &mut EventReceiver) -> (Vec<NotificationPresentation>, Vec<Vec<String>>) {
    let mut presented = Vec::new();
    let mut cleared = Vec::new();
    while let Ok(event) = events.try_recv() {
        match event {
            SdkEvent::Notification(NotificationEvent::Present(presentation)) => {
                presented.push(*presentation)
            }
            SdkEvent::Notification(NotificationEvent::Clear { collapse_ids, .. }) => {
                cleared.push(collapse_ids)
            }
            _ => {}
        }
    }
    (presented, cleared)
}

async fn deliver(harness: &Harness, messages: Vec<IMMessage>) {
    let candidates = harness.center.live_candidates(&messages, "me");
    harness.center.present_live(candidates, "me").await;
}

/// 前台：同一会话一批只出一条横幅、带条数、正文是最新一条；自己发的、正在看的会话不提示；
/// 一批里只响一次。
#[tokio::test]
async fn the_foreground_coalesces_per_conversation_into_in_app_banners() {
    let mut h = harness(Some(AlertSurface::InApp));
    h.stores
        .conversations
        .save_one(&group("g1", "项目群"))
        .await
        .unwrap();
    h.stores
        .conversations
        .save_one(&group("g2", "读书会"))
        .await
        .unwrap();
    h.stores
        .conversations
        .save_one(&group("g3", "正在看"))
        .await
        .unwrap();
    let focus = Arc::new(NotificationHost::default());
    focus.set_focus(Some("g3".to_owned()));
    h.center.adopt_host(focus);

    deliver(
        &h,
        vec![
            text("g1", 1, "ann", "第一条"),
            text("g2", 1, "bob", "hello"),
            text("g1", 2, "ann", "第二条"),
            text("g1", 3, "me", "我自己发的"),
            text("g3", 1, "bob", "正在看的会话"),
        ],
    )
    .await;
    let (presented, _) = drain(&mut h.events);
    assert_eq!(presented.len(), 2, "{presented:?}");
    let g2 = &presented[0];
    assert_eq!(g2.conversation_id, "g2");
    assert_eq!(
        (g2.title.as_str(), g2.body.as_str()),
        ("读书会", "BOB: hello")
    );
    assert!(g2.sound, "一批里第一个出的横幅响");
    let g1 = &presented[1];
    assert_eq!(g1.conversation_id, "g1");
    assert_eq!(g1.count, 2);
    assert_eq!(g1.seq, 2);
    assert_eq!(
        (g1.title.as_str(), g1.body.as_str()),
        ("[2条] 项目群", "ANN: 第二条")
    );
    assert!(!g1.sound, "一批里只响一次");
    assert_eq!(g1.surface, "inApp");
    assert_eq!(g1.collapse_id, h.alerts.collapse_id("g1").unwrap());
    assert_eq!(g1.thread_id, g1.collapse_id);

    // 同一会话的下一条接着计数；提示过的记进台账，同一条的推送再来不响。
    deliver(&h, vec![text("g1", 4, "ann", "第三条")]).await;
    let (presented, _) = drain(&mut h.events);
    assert_eq!(presented[0].count, 3);
    assert_eq!(
        h.alerts.claim_alert(&presented[0].collapse_id, 4, false),
        Err(AlertSkip::AlreadyAlerted)
    );
}

/// 会话偏好：免打扰不提示；仅 @我 只提示点名的，点名带标记、走 @我 渠道；没有展示方的后台不提示。
#[tokio::test]
async fn conversation_preferences_and_surface_decide_whether_to_alert() {
    let mut h = harness(Some(AlertSurface::InApp));
    let mut muted = group("muted", "免打扰群");
    muted.is_muted = true;
    let mut mention_only = group("mention", "只看 @我");
    mention_only.is_mention_only = Some(true);
    h.stores.conversations.save_one(&muted).await.unwrap();
    h.stores
        .conversations
        .save_one(&mention_only)
        .await
        .unwrap();

    let mut mentioned = text("mention", 2, "ann", "看下这个");
    mentioned.mention_users.push("me".to_owned());
    deliver(
        &h,
        vec![
            text("muted", 1, "ann", "x"),
            text("mention", 1, "ann", "没点名"),
            mentioned,
        ],
    )
    .await;
    let (presented, _) = drain(&mut h.events);
    assert_eq!(presented.len(), 1, "{presented:?}");
    assert_eq!(presented[0].conversation_id, "mention");
    assert_eq!(presented[0].body, "[有人@你] ANN: 看下这个");
    assert_eq!(presented[0].channel, "mentions");

    h.reach.set(None);
    assert!(
        h.center
            .live_candidates(&[text("mention", 3, "ann", "x")], "me")
            .is_empty(),
        "提示归推送时不挑候选"
    );
}

/// 大群默认只提示 @我；显式开启了「全部消息通知」的照常提示。
#[tokio::test]
async fn large_groups_alert_only_mentions_unless_notify_all() {
    let mut h = harness(Some(AlertSurface::InApp));
    let mut large = group("large", "千人群");
    large.members_count = 5_000;
    h.stores.conversations.save_one(&large).await.unwrap();
    deliver(&h, vec![text("large", 1, "ann", "大家好")]).await;
    assert!(drain(&mut h.events).0.is_empty());

    h.center.apply_notify_all("large", true).await;
    assert!(notify_all(
        &h.stores.conversations.get("large").await.unwrap().unwrap()
    ));
    deliver(&h, vec![text("large", 2, "ann", "大家好")]).await;
    assert_eq!(drain(&mut h.events).0.len(), 1);
}

/// 后台（本进程能自己提示）：第一条响铃出系统通知；间隔内的吞并，到期补一次静默的尾部刷新，显示
/// 最新一条与正确计数。
#[tokio::test]
async fn the_background_gates_system_notifications_and_refreshes_the_tail() {
    let mut h = harness(Some(AlertSurface::System));
    h.stores
        .conversations
        .save_one(&group("g1", "项目群"))
        .await
        .unwrap();
    deliver(
        &h,
        vec![
            text("g1", 1, "ann", "第一条"),
            text("g1", 2, "ann", "第二条"),
        ],
    )
    .await;
    let (presented, _) = drain(&mut h.events);
    assert_eq!(presented.len(), 1);
    assert!(presented[0].sound);
    assert_eq!(presented[0].surface, "system");
    assert_eq!(presented[0].channel, "messages");
    assert_eq!((presented[0].seq, presented[0].count), (1, 1));

    let due = h.center.lock_state().slots["g1"]
        .tail_due_ms
        .expect("a tail refresh is scheduled");
    h.center.on_tail_due("g1".to_owned(), due).await;
    let (presented, _) = drain(&mut h.events);
    assert_eq!(presented.len(), 1);
    let tail = &presented[0];
    assert!(!tail.sound);
    assert_eq!(tail.channel, "quiet");
    assert_eq!((tail.seq, tail.count), (2, 2));
    assert_eq!(
        (tail.title.as_str(), tail.body.as_str()),
        ("[2条] 项目群", "ANN: 第二条")
    );

    // 已经补过的到期不再算数。
    h.center.on_tail_due("g1".to_owned(), due).await;
    assert!(drain(&mut h.events).0.is_empty());
}

/// 会话读到了提示过的 seq：撤掉通知、关槽位；之后同一段的推送不再展示。
#[tokio::test]
async fn reading_the_conversation_clears_its_notification() {
    let mut h = harness(Some(AlertSurface::System));
    h.stores
        .conversations
        .save_one(&group("g1", "项目群"))
        .await
        .unwrap();
    deliver(&h, vec![text("g1", 5, "ann", "hi")]).await;
    let (presented, _) = drain(&mut h.events);
    let collapse_id = presented[0].collapse_id.clone();

    h.center.check_read(vec!["g1".to_owned()]).await;
    assert!(drain(&mut h.events).1.is_empty(), "还没读，不撤");

    h.stores
        .conversations
        .update_unread("g1", 0, 5)
        .await
        .unwrap();
    h.center.check_read(vec!["g1".to_owned()]).await;
    let (_, cleared) = drain(&mut h.events);
    assert_eq!(cleared, vec![vec![collapse_id.clone()]]);
    assert!(
        !h.center.lock_state().slots.contains_key("g1"),
        "读完的槽位不再留"
    );
    assert_eq!(
        h.alerts.claim_alert(&collapse_id, 5, false),
        Err(AlertSkip::Stale)
    );
}

/// 展示着的那条被撤回：同一合并键静默替换成「撤回了一条消息」，计数减一；别的消息撤回不动它。
#[tokio::test]
async fn a_recall_replaces_the_shown_message_silently() {
    let mut h = harness(Some(AlertSurface::InApp));
    h.stores
        .conversations
        .save_one(&group("g1", "项目群"))
        .await
        .unwrap();
    deliver(
        &h,
        vec![text("g1", 1, "ann", "一"), text("g1", 2, "ann", "二")],
    )
    .await;
    let (presented, _) = drain(&mut h.events);
    assert_eq!(presented[0].count, 2);

    h.center.on_recalled("g1", "g1-1").await;
    assert!(drain(&mut h.events).0.is_empty(), "撤回的不是展示着的那条");

    h.center.on_recalled("g1", "g1-2").await;
    let (presented, _) = drain(&mut h.events);
    assert_eq!(presented.len(), 1);
    let replaced = &presented[0];
    assert_eq!(
        (replaced.title.as_str(), replaced.body.as_str()),
        ("项目群", "撤回了一条消息")
    );
    assert_eq!((replaced.seq, replaced.count), (2, 1));
    assert!(!replaced.sound);
    assert_eq!(replaced.surface, "inApp");
}

/// 推送已经展示过的那条，链路再到达时不再提示。
#[tokio::test]
async fn an_alert_the_push_already_showed_is_not_repeated() {
    let mut h = harness(Some(AlertSurface::InApp));
    let collapse_id = h.alerts.collapse_id("g1").unwrap();
    h.alerts.claim_alert(&collapse_id, 3, false).unwrap();
    deliver(&h, vec![text("g1", 3, "ann", "hi")]).await;
    assert!(drain(&mut h.events).0.is_empty());
    deliver(&h, vec![text("g1", 4, "ann", "hi")]).await;
    assert_eq!(drain(&mut h.events).0.len(), 1);
}

/// 勿扰时段：普通消息静默送达（出通知不响），点名在放行时照常响。
#[tokio::test]
async fn quiet_hours_silence_ordinary_messages() {
    let mut h = harness(Some(AlertSurface::InApp));
    h.stores
        .conversations
        .save_one(&group("g1", "项目群"))
        .await
        .unwrap();
    h.center.adopt_settings(
        UserNotificationSettings {
            // 全天、每天。
            quiet_hours: Some(QuietHours {
                start_minute: 0,
                end_minute: 0,
                weekdays: 0,
                allow_mentions: true,
            }),
            ..UserNotificationSettings::default()
        },
        true,
    );
    deliver(&h, vec![text("g1", 1, "ann", "hi")]).await;
    let (presented, _) = drain(&mut h.events);
    assert_eq!(presented.len(), 1);
    assert!(!presented[0].sound, "勿扰时段里不响");
    assert_eq!(presented[0].channel, "quiet");
}

/// 预览设置为隐藏：通用标题与正文。
#[tokio::test]
async fn the_hidden_preview_shows_nothing_of_the_message() {
    let mut h = harness(Some(AlertSurface::InApp));
    h.stores
        .conversations
        .save_one(&group("g1", "项目群"))
        .await
        .unwrap();
    h.center.adopt_settings(
        UserNotificationSettings {
            preview: NotificationPreview::Hidden,
            settings_version: 2,
            ..UserNotificationSettings::default()
        },
        true,
    );
    deliver(&h, vec![text("g1", 1, "ann", "机密")]).await;
    let (presented, _) = drain(&mut h.events);
    assert_eq!(
        (presented[0].title.as_str(), presented[0].body.as_str()),
        ("新消息", "你收到一条新消息")
    );
}

/// 设置：读到的版本比手上的旧就不覆盖；本机写入总是覆盖。
#[test]
fn settings_never_go_back_to_an_older_version() {
    let h = harness(None);
    let newer = UserNotificationSettings {
        settings_version: 5,
        ..UserNotificationSettings::default()
    };
    let older = UserNotificationSettings {
        preview: NotificationPreview::Hidden,
        settings_version: 4,
        ..UserNotificationSettings::default()
    };
    h.center.adopt_settings(newer.clone(), false);
    assert_eq!(h.center.adopt_settings(older.clone(), false), newer);
    assert_eq!(h.center.adopt_settings(older.clone(), true), older);
}

/// 角标只在数值变了时报，强制时照报；会话结束时撤掉全部通知、角标归零，只做一次。
#[tokio::test]
async fn badges_are_reported_on_change_and_the_session_end_clears_everything() {
    let mut h = harness(Some(AlertSurface::InApp));
    let badges = |events: &mut EventReceiver| {
        let mut counts = Vec::new();
        let mut all = 0;
        while let Ok(event) = events.try_recv() {
            match event {
                SdkEvent::Notification(NotificationEvent::Badge { count }) => counts.push(count),
                SdkEvent::Notification(NotificationEvent::Clear { all: true, .. }) => all += 1,
                _ => {}
            }
        }
        (counts, all)
    };
    h.center.publish_badge(3, false);
    h.center.publish_badge(3, false);
    h.center.publish_badge(3, true);
    h.center.publish_badge(4, false);
    assert_eq!(badges(&mut h.events), (vec![3, 3, 4], 0));

    h.center.end_session();
    h.center.end_session();
    assert_eq!(badges(&mut h.events), (vec![0], 1));
}

/// 规则输入：用户设置转成规则的偏好，越界的勿扰时段当作没有。
#[test]
fn user_settings_map_onto_rule_preferences() {
    let settings = UserNotificationSettings {
        quiet_hours: Some(QuietHours {
            start_minute: 1_320,
            end_minute: 420,
            weekdays: 0x1f,
            allow_mentions: true,
        }),
        preview: NotificationPreview::SenderOnly,
        multi_device: MultiDeviceAlertPolicy::Immediate,
        settings_version: 1,
    };
    let prefs = user_prefs(Some(&settings));
    assert_eq!(
        prefs.quiet_hours,
        Some(QuietWindow {
            start_minute: 1_320,
            end_minute: 420,
            weekdays: 0x1f,
            allow_mentions: true,
        })
    );
    assert_eq!(prefs.preview, PreviewMode::SenderOnly);
    assert_eq!(prefs.multi_device, MultiDevicePolicy::Immediate);
    let broken = UserNotificationSettings {
        quiet_hours: Some(QuietHours {
            start_minute: 2_000,
            ..QuietHours::default()
        }),
        ..settings
    };
    assert_eq!(user_prefs(Some(&broken)).quiet_hours, None);
    assert_eq!(user_prefs(None), UserPrefs::default());
}

/// 提示等级与点名：通知类内容按标志分级；E2EE 密文说不清是否点名。
#[test]
fn alert_class_and_mentions_follow_the_server() {
    let mut message = text("g1", 1, "ann", "hi");
    assert_eq!(alert_class(&message), AlertClass::Normal);
    assert_eq!(mentioned(&message, "me"), Mentioned::No);
    message.mention_all = true;
    assert_eq!(mentioned(&message, "me"), Mentioned::Yes);
    message.mention_all = false;
    message.content = Some(Elem::Placeholder(
        crate::content::message_elem::PlaceholderElem {
            reason: E2EE_PLACEHOLDER_REASON.to_owned(),
            payload: vec![],
            fallback_text: String::new(),
            metadata: Default::default(),
        },
    ));
    assert_eq!(mentioned(&message, "me"), Mentioned::Unknown);
    let notice = |play_sound, show_badge| {
        let mut message = text("g1", 1, "ann", "");
        message.content = Some(Elem::Notification(
            crate::content::message_elem::NotificationElem {
                title: String::new(),
                body: String::new(),
                notification_type: "x".into(),
                data: Default::default(),
                target_user_ids: vec![],
                target_role_id: String::new(),
                notify_all: false,
                persistent: true,
                show_in_list: false,
                show_badge,
                play_sound,
            },
        ));
        message
    };
    assert_eq!(alert_class(&notice(true, false)), AlertClass::Normal);
    assert_eq!(alert_class(&notice(false, true)), AlertClass::Silent);
    assert_eq!(alert_class(&notice(false, false)), AlertClass::None);
}
