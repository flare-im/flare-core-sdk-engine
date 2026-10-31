use std::collections::HashMap;

use serde::Deserialize;

use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vectors {
    id_tail: Vec<IdTailCase>,
    people: Vec<PersonCase>,
    conversations: Vec<ConversationCase>,
}

#[derive(Deserialize)]
struct IdTailCase {
    name: String,
    id: String,
    expected: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersonCase {
    name: String,
    user_id: String,
    conversation_type: Option<String>,
    alias: String,
    member_nickname: String,
    profile_name: String,
    snapshot: String,
    expected: DisplayName,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct KnownPerson {
    alias: String,
    member_nickname: String,
    profile_name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConversationCase {
    name: String,
    conversation_id: String,
    conversation_type: String,
    channel_id: String,
    remark: String,
    display_name: String,
    current_user_id: String,
    members_count: u32,
    members: Vec<String>,
    people: HashMap<String, KnownPerson>,
    expected: DisplayName,
}

/// The shared vectors live in the client SDK spec next to this repository.
fn vectors() -> Vectors {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../flare-im-core-client-sdk/sdk-spec/golden/display-name-projection.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|error| panic!("decode {}: {error}", path.display()))
}

#[test]
fn id_tail_follows_the_shared_vectors() {
    let cases = vectors().id_tail;
    assert!(!cases.is_empty());
    for case in cases {
        assert_eq!(id_tail(&case.id), case.expected, "{}", case.name);
    }
}

#[test]
fn person_names_follow_the_shared_vectors() {
    let cases = vectors().people;
    assert!(cases.len() >= 13);
    for case in cases {
        let names = PersonNameCandidates {
            alias: &case.alias,
            member_nickname: &case.member_nickname,
            profile_name: &case.profile_name,
            snapshot: &case.snapshot,
        };
        let context = case
            .conversation_type
            .as_deref()
            .map(ConversationType::from);
        assert_eq!(
            resolve_person_name(&case.user_id, &names, context),
            case.expected,
            "{}",
            case.name
        );
    }
}

#[test]
fn conversation_titles_follow_the_shared_vectors() {
    let cases = vectors().conversations;
    assert!(cases.len() >= 20);
    for case in cases {
        let members: Vec<&str> = case.members.iter().map(String::as_str).collect();
        let input = ConversationTitleInput {
            conversation_id: &case.conversation_id,
            conversation_type: ConversationType::from(case.conversation_type.as_str()),
            channel_id: &case.channel_id,
            remark: &case.remark,
            display_name: &case.display_name,
            current_user_id: &case.current_user_id,
            member_ids: &members,
            members_count: case.members_count,
        };
        let title = resolve_conversation_title(&input, |id| {
            case.people
                .get(id)
                .map(|person| PersonNameCandidates {
                    alias: &person.alias,
                    member_nickname: &person.member_nickname,
                    profile_name: &person.profile_name,
                    snapshot: "",
                })
                .unwrap_or_default()
        });
        assert_eq!(title, case.expected, "{}", case.name);
    }
}

#[test]
fn display_name_wire_is_tagged_camel_case() {
    let named = serde_json::to_value(DisplayName::named("Alice", NameSource::MemberNickname))
        .expect("encode named");
    assert_eq!(
        named,
        serde_json::json!({"kind": "named", "text": "Alice", "source": "memberNickname"})
    );
    let members = serde_json::to_value(DisplayName::Members {
        names: vec!["A".into(), "B".into()],
        total: 12,
    })
    .expect("encode members");
    assert_eq!(
        members,
        serde_json::json!({"kind": "members", "names": ["A", "B"], "total": 12})
    );
    let unnamed = serde_json::to_value(DisplayName::unnamed(NameEntity::Person, "u_9876"))
        .expect("encode unnamed");
    assert_eq!(
        unnamed,
        serde_json::json!({"kind": "unnamed", "entity": "person", "idTail": "9876"})
    );
    let back: DisplayName = serde_json::from_value(unnamed).expect("decode unnamed");
    assert_eq!(back, DisplayName::unnamed(NameEntity::Person, "9876"));
}

#[test]
fn business_names_drop_values_equal_to_an_own_id() {
    assert_eq!(
        business_name(" Design team ", &["g_1", "2A_1"]),
        "Design team"
    );
    assert_eq!(business_name("g_1", &["g_1", "2A_1"]), "");
    assert_eq!(business_name(" 2A_1 ", &["g_1", "2A_1"]), "");
    assert_eq!(business_name("", &["g_1"]), "");
    assert_eq!(business_name("0401", &["g_1", ""]), "0401");
}

#[test]
fn participant_snapshots_never_store_the_user_id_as_a_nickname() {
    let snapshot = ParticipantDisplaySnapshot::new(" c1 ", " u1 ", " u1 ")
        .normalized()
        .expect("ids present");
    assert_eq!(snapshot, ParticipantDisplaySnapshot::new("c1", "u1", ""));
    assert!(
        ParticipantDisplaySnapshot::new("", "u1", "Bob")
            .normalized()
            .is_none()
    );
}
