//! 名字投影的输入变了（身份缓存的资料名与别名、会话内成员昵称）：打开中的视图只重读受影响的
//! 行并推 upsert。
//!
//! 测试台（`view_test_harness.rs`）每一步都把推出的更新应用到宿主侧窗口并与视图比对，所以每个
//! 用例同时验证「增量应用到上一版本后等于新快照」。

use std::collections::HashMap;
use std::time::Duration;

use flare_proto::common::Message as ProtoMessage;
use serde_json::Value;

use super::harness::{CONVERSATION, Harness, ReadCounts, Setup, row};
use crate::domain::{DisplayName, NameSource, ParticipantDisplaySnapshot, UserProfile};
use crate::extension::ExtensionContext;
use crate::kernel::event::NameChange;
use crate::model::conversation::ConversationType;
use crate::model::{
    Conversation, ConversationParticipant, IMMessage, MessagePreviewElem,
    OpenConversationListViewRequest, OpenTimelineViewRequest, TimelineViewRequest, ViewDelta,
    ViewDeltaKind, ViewSnapshot, ViewUpdate, ViewUpdateKind,
};

fn setup(max_seq: u64) -> Setup<'static> {
    Setup {
        page_size: 40,
        max_seq,
        visible_after_seq: 0,
        server: Box::leak(Box::new([[1, max_seq]])),
        local: Box::leak(Box::new([[1, max_seq]])),
        coverage: Box::leak(Box::new([[1, max_seq]])),
    }
}

/// 会话里的一条消息，发送者与会话类型可选（测试台的 `row` 都是 u2 发的、不带会话类型）。
fn message(conversation_id: &str, seq: u64, sender: &str, kind: ConversationType) -> IMMessage {
    let mut message = IMMessage::new(ProtoMessage {
        conversation_id: conversation_id.into(),
        server_id: format!("{conversation_id}-s{seq}"),
        client_msg_id: format!("{conversation_id}-m{seq}"),
        sender_id: sender.into(),
        conversation_seq: seq,
        ..Default::default()
    });
    message.conversation_type = kind.to_proto_int();
    message
}

fn keys(rows: &[IMMessage]) -> Vec<String> {
    rows.iter().map(IMMessage::row_key).collect()
}

fn timeline_delta(update: &ViewUpdate) -> &ViewDelta {
    assert_eq!(update.kind, ViewUpdateKind::Delta, "a delta update");
    update.delta.as_ref().expect("delta payload")
}

async fn save_profile(h: &Harness, profile: UserProfile) {
    h.stores.save_user_profiles(&[profile]).await.unwrap();
}

/// 窗口当前的快照（`view.timeline.snapshot`）与宿主按增量拼出的窗口逐行相同。
async fn assert_host_is_the_snapshot(h: &Harness) {
    let response = h
        .view
        .timeline_snapshot(TimelineViewRequest {
            view_id: h.view_id(),
        })
        .await
        .unwrap();
    let ViewSnapshot::Timeline(snapshot) = &response.snapshot else {
        panic!("timeline snapshot");
    };
    let host = h.host.lock().unwrap().clone();
    assert_eq!(host.revision, response.revision);
    assert_eq!(
        serde_json::to_value(&host.rows).unwrap(),
        serde_json::to_value(&snapshot.messages).unwrap(),
        "the deltas applied to the earlier window give the new snapshot"
    );
}

/// 资料变了：只按 id 重读窗口里这个人发出的行，推且只推这些行的 upsert，版本号加 1。
#[tokio::test]
async fn a_profile_change_upserts_only_the_rows_its_person_sent() {
    let h = Harness::new(setup(6)).await;
    // 2、5 是 u3 发的，其余是 u2。
    let others = [2, 5]
        .into_iter()
        .map(|seq| {
            let mut other = row(seq);
            other.sender_id = "u3".into();
            other
        })
        .collect::<Vec<_>>();
    h.stores.messages.save_batch(&others).await.unwrap();
    h.open().await;
    let revision = h.state().await.revision;
    h.reads.counts.reset();

    save_profile(&h, UserProfile::new("u2").with_nickname("Bob")).await;
    h.names_changed(NameChange::users(["u2"])).await;

    let counts = &h.reads.counts;
    assert_eq!(ReadCounts::get(&counts.pages), 0, "no window reread");
    assert_eq!(ReadCounts::get(&counts.newer_pages), 0);
    assert_eq!(ReadCounts::get(&counts.by_id_calls), 1, "one batch read");
    assert_eq!(ReadCounts::get(&counts.by_id_ids), 4, "for u2's four rows");
    assert_eq!(ReadCounts::get(&counts.by_id_rows), 4);
    let updates = h.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].revision, revision + 1);
    let ViewDelta::Timeline {
        prepend,
        append,
        upsert,
        remove,
        reorder,
        conversation,
        ..
    } = timeline_delta(&updates[0])
    else {
        panic!("timeline delta");
    };
    assert_eq!(keys(upsert), vec!["m1", "m3", "m4", "m6"]);
    for fresh in upsert {
        assert_eq!(fresh.sender, DisplayName::named("Bob", NameSource::Profile));
    }
    assert!(prepend.is_empty() && append.is_empty() && remove.is_empty());
    assert!(reorder.is_none() && conversation.is_none());
    assert_host_is_the_snapshot(&h).await;

    // 别名排在资料名之前：同样只重读、只推 u2 的行。
    h.reads.counts.reset();
    save_profile(
        &h,
        UserProfile::new("u2")
            .with_nickname("Bob")
            .with_alias("Bobby"),
    )
    .await;
    h.names_changed(NameChange::users(["u2"])).await;
    assert_eq!(ReadCounts::get(&h.reads.counts.by_id_ids), 4);
    let updates = h.take_updates();
    assert_eq!(updates.len(), 1);
    let ViewDelta::Timeline { upsert, .. } = timeline_delta(&updates[0]) else {
        panic!("timeline delta");
    };
    assert_eq!(keys(upsert), vec!["m1", "m3", "m4", "m6"]);
    assert_eq!(
        upsert[0].sender,
        DisplayName::named("Bobby", NameSource::Alias)
    );
    assert_host_is_the_snapshot(&h).await;
}

/// 窗口里没有这个人发出的行：一行也不读，不推更新；有他的行但重新投影后名字没变：不推空增量。
#[tokio::test]
async fn nothing_is_published_when_no_row_reads_differently() {
    let h = Harness::new(setup(6)).await;
    h.open().await;
    let revision = h.state().await.revision;
    h.reads.counts.reset();

    save_profile(&h, UserProfile::new("u9").with_nickname("Nobody here")).await;
    h.names_changed(NameChange::users(["u9"])).await;
    assert_eq!(ReadCounts::get(&h.reads.counts.by_id_calls), 0);
    assert_eq!(ReadCounts::get(&h.reads.counts.pages), 0);
    assert!(h.take_updates().is_empty());
    assert_eq!(h.state().await.revision, revision);

    // u2 的行在窗口里，但他的名字没有变（还是无名）：读了，没有可推的。
    h.names_changed(NameChange::users(["u2"])).await;
    assert_eq!(ReadCounts::get(&h.reads.counts.by_id_calls), 1);
    assert!(h.take_updates().is_empty(), "no empty delta");
    assert_eq!(h.state().await.revision, revision, "the revision stays");
}

/// 没有打开的视图：名字变化什么也不读、什么也不推。
#[tokio::test]
async fn without_open_views_a_name_change_does_nothing() {
    let h = Harness::new(setup(6)).await;
    let mut events = h.bus.subscribe_raw();
    save_profile(&h, UserProfile::new("u2").with_nickname("Bob")).await;
    h.names_changed(NameChange::users(["u2"])).await;
    let counts = &h.reads.counts;
    assert_eq!(ReadCounts::get(&counts.by_id_calls), 0);
    assert_eq!(ReadCounts::get(&counts.pages), 0);
    assert_eq!(ReadCounts::get(&counts.newer_pages), 0);
    assert!(events.try_recv().is_err(), "nothing is published");
}

/// 成员昵称只在它所在的会话里生效：只有该会话的视图重读、推 upsert，同一个人在别的会话里的行不读。
#[tokio::test]
async fn a_member_nickname_renames_rows_only_in_its_conversation() {
    // 本地没有测试台的行，换成三条群消息（成员昵称只在群与频道里生效）。
    let h = Harness::new(Setup {
        page_size: 40,
        max_seq: 3,
        visible_after_seq: 0,
        server: &[[1, 3]],
        local: &[],
        coverage: &[[1, 3]],
    })
    .await;
    let group = ConversationType::Group;
    let c1 = (1..=3)
        .map(|seq| message(CONVERSATION, seq, "u2", group))
        .collect::<Vec<_>>();
    h.stores.messages.save_batch(&c1).await.unwrap();
    h.stores
        .conversations
        .save_one(&Conversation {
            conversation_id: "g2".into(),
            channel_id: "g2".into(),
            conversation_type: group,
            max_seq: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    let g2 = (1..=2)
        .map(|seq| message("g2", seq, "u2", group))
        .collect::<Vec<_>>();
    h.stores.messages.save_batch(&g2).await.unwrap();
    h.stores
        .timeline_coverage
        .add("g2", crate::domain::SeqRange::new(1, 2).unwrap())
        .await
        .unwrap();
    h.open().await;
    assert_eq!(
        keys(&h.window_rows().await),
        vec!["c1-m1", "c1-m2", "c1-m3"]
    );
    let other = h
        .view
        .open_timeline(OpenTimelineViewRequest {
            conversation_id: "g2".into(),
            message_limit: 40,
            anchor: None,
        })
        .await
        .unwrap();
    let other_revision = || async {
        h.view
            .inner
            .views
            .read()
            .await
            .timelines
            .get(&other.view_id)
            .map(|view| view.revision)
    };
    assert_eq!(other_revision().await, Some(1));
    h.reads.counts.reset();

    let snapshots = [ParticipantDisplaySnapshot::new(
        CONVERSATION,
        "u2",
        "Group Bob",
    )];
    let changed = h
        .stores
        .apply_participant_display(&snapshots)
        .await
        .unwrap();
    assert_eq!(changed, vec![CONVERSATION.to_string()]);
    h.names_changed(NameChange::written_member_nicknames(&snapshots, &changed))
        .await;

    assert_eq!(ReadCounts::get(&h.reads.counts.by_id_calls), 1);
    assert_eq!(
        ReadCounts::get(&h.reads.counts.by_id_ids),
        3,
        "only the rows of the nickname's conversation"
    );
    let updates = h.take_updates();
    assert_eq!(updates.len(), 1);
    let ViewDelta::Timeline { upsert, .. } = timeline_delta(&updates[0]) else {
        panic!("timeline delta");
    };
    assert_eq!(keys(upsert), vec!["c1-m1", "c1-m2", "c1-m3"]);
    assert_eq!(
        upsert[0].sender,
        DisplayName::named("Group Bob", NameSource::MemberNickname)
    );
    assert_eq!(
        other_revision().await,
        Some(1),
        "the other view is untouched"
    );
    assert_host_is_the_snapshot(&h).await;
}

/// 经扩展存储写入的资料（社交 SDK 读到新发送者资料后补进身份缓存走的就是这条路）：视图的刷新
/// 任务收到名字变化，推出发送者的 upsert，宿主不用自己重读。
#[tokio::test]
async fn names_written_through_the_extension_store_reach_the_open_views() {
    let h = Harness::new(setup(3)).await;
    h.open().await;
    let context = ExtensionContext::from_core(h.stores.clone(), h.bus.clone());

    context
        .store()
        .save_user_profiles(&[UserProfile::new("u2").with_nickname("Bob")])
        .await
        .unwrap();
    let updates = wait_for_updates(&h).await;
    let ViewDelta::Timeline { upsert, .. } = timeline_delta(&updates[0]) else {
        panic!("timeline delta");
    };
    assert_eq!(keys(upsert), vec!["m1", "m2", "m3"]);
    assert_eq!(
        upsert[0].sender,
        DisplayName::named("Bob", NameSource::Profile)
    );

    context
        .store()
        .apply_user_profile(
            &UserProfile::new("u2")
                .with_nickname("Bob")
                .with_alias("Bobby"),
        )
        .await
        .unwrap();
    let updates = wait_for_updates(&h).await;
    let ViewDelta::Timeline { upsert, .. } = timeline_delta(&updates[0]) else {
        panic!("timeline delta");
    };
    assert_eq!(
        upsert[0].sender,
        DisplayName::named("Bobby", NameSource::Alias)
    );
    assert_host_is_the_snapshot(&h).await;
}

/// 等刷新任务推出更新（去抖 40ms）。
async fn wait_for_updates(h: &Harness) -> Vec<ViewUpdate> {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        h.settle().await;
        let updates = h.take_updates();
        if !updates.is_empty() {
            return updates;
        }
    }
    panic!("the open view published no update");
}

/// 会话列表视图：只重读以这个人命名的会话（标题、末条发送者、成员预览里的名字），推这些会话的
/// update；增量应用到上一版等于新快照。
#[tokio::test]
async fn conversation_lists_update_the_conversations_named_after_the_person() {
    let h = Harness::new(setup(1)).await;
    let member = |user_id: &str| ConversationParticipant {
        user_id: user_id.into(),
        ..Default::default()
    };
    let last_from = |sender: &str, at: u64| MessagePreviewElem {
        message_id: format!("last-{at}"),
        sender_id: sender.into(),
        r#type: 1,
        text: "hi".into(),
        time: at,
    };
    let conversations = [
        // 与 u2 的单聊：标题是他。
        Conversation {
            conversation_id: "1A_u2".into(),
            channel_id: "u2".into(),
            conversation_type: ConversationType::Single,
            updated_at: 50,
            ..Default::default()
        },
        // 有名字的群，末条是 u3 发的，成员预览里没有 u2：与他无关。
        Conversation {
            conversation_id: "2A_team".into(),
            channel_id: "team".into(),
            conversation_type: ConversationType::Group,
            display_name: "Team".into(),
            last_sender_id: Some("u3".into()),
            last_message: Some(last_from("u3", 40)),
            member_preview: vec![member("u3"), member("u4")],
            updated_at: 40,
            ..Default::default()
        },
        // 有名字的群，标题与末条都不取自 u2，但成员预览里他的名字是投影出来的。
        Conversation {
            conversation_id: "2A_design".into(),
            channel_id: "design".into(),
            conversation_type: ConversationType::Group,
            display_name: "Design".into(),
            last_sender_id: Some("u3".into()),
            last_message: Some(last_from("u3", 35)),
            member_preview: vec![member("u2"), member("u3")],
            updated_at: 35,
            ..Default::default()
        },
        // 无名群，u2 在成员预览里：标题由成员拼成。
        Conversation {
            conversation_id: "2A_unnamed".into(),
            channel_id: "unnamed".into(),
            conversation_type: ConversationType::Group,
            member_preview: vec![member("u2"), member("u4")],
            members_count: 2,
            updated_at: 30,
            ..Default::default()
        },
        // 有名字的群，末条是 u2 发的：末条发送者是他。
        Conversation {
            conversation_id: "2A_ops".into(),
            channel_id: "ops".into(),
            conversation_type: ConversationType::Group,
            display_name: "Ops".into(),
            last_sender_id: Some("u2".into()),
            last_message: Some(last_from("u2", 20)),
            updated_at: 20,
            ..Default::default()
        },
    ];
    h.stores
        .conversations
        .save_batch(&conversations)
        .await
        .unwrap();
    let request = OpenConversationListViewRequest {
        conversation_limit: 20,
    };
    let opened = h
        .view
        .open_conversation_list(request.clone())
        .await
        .unwrap();
    let ViewSnapshot::ConversationList(before) = &opened.snapshot else {
        panic!("conversation list snapshot");
    };
    let mut events = h.bus.subscribe_raw();

    save_profile(&h, UserProfile::new("u2").with_nickname("Bob")).await;
    h.names_changed(NameChange::users(["u2"])).await;

    let mut updates = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let crate::kernel::event::SdkEvent::View(update) = event
            && update.view_id == opened.view_id
        {
            updates.push(update);
        }
    }
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].revision, opened.revision + 1);
    let Some(ViewDelta::ConversationList { ops, .. }) = &updates[0].delta else {
        panic!("conversation list delta");
    };
    let mut updated = ops
        .iter()
        .map(|op| {
            assert_eq!(op.op, ViewDeltaKind::Update, "only updates in place");
            op.key.clone()
        })
        .collect::<Vec<_>>();
    updated.sort();
    assert_eq!(updated, vec!["1A_u2", "2A_design", "2A_ops", "2A_unnamed"]);

    // 按键原位换掉，得到的就是新快照。
    let mut host = before
        .conversations
        .iter()
        .map(|conversation| {
            (
                conversation.conversation_id.clone(),
                serde_json::to_value(conversation).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let items = ops
        .iter()
        .map(|op| (op.key.clone(), op.item.clone().expect("update item")))
        .collect::<HashMap<_, _>>();
    for (key, value) in host.iter_mut() {
        if let Some(item) = items.get(key) {
            *value = item.clone();
        }
    }
    let fresh = h.view.open_conversation_list(request).await.unwrap();
    let ViewSnapshot::ConversationList(after) = &fresh.snapshot else {
        panic!("conversation list snapshot");
    };
    assert_eq!(
        host.into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<Value>>(),
        after
            .conversations
            .iter()
            .map(|conversation| serde_json::to_value(conversation).unwrap())
            .collect::<Vec<_>>()
    );
    let single = after
        .conversations
        .iter()
        .find(|conversation| conversation.conversation_id == "1A_u2")
        .unwrap();
    assert_eq!(single.title, DisplayName::named("Bob", NameSource::Profile));

    // 没有以 u9 命名的会话：什么也不推。
    h.names_changed(NameChange::users(["u9"])).await;
    assert!(events.try_recv().is_err());
}
