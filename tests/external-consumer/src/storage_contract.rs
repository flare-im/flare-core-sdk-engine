//! Storage contract samples any account backend runs through the public store traits.
//! A third-party backend passes when its observations equal the built-in ones.
use flare_im_core_sdk::{
    model::{Conversation, IMMessage, MessageKey},
    storage::StoreProvider,
};
use flare_proto::common::{
    Message as ProtoMessage, MessageContent, TextContent, message_content::Content,
};

pub const CID: &str = "group-contract";

fn inbound(seq: u64) -> IMMessage {
    IMMessage::new(ProtoMessage {
        conversation_id: CID.into(),
        server_id: format!("contract-s{seq}"),
        client_msg_id: format!("contract-c{seq}"),
        sender_id: "peer".into(),
        conversation_seq: seq,
        content: Some(MessageContent {
            content: Some(Content::Text(TextContent {
                text: format!("message {seq}"),
                ..Default::default()
            })),
        }),
        ..Default::default()
    })
}

fn ids(rows: &[IMMessage]) -> Vec<String> {
    rows.iter().map(|m| m.client_msg_id.clone()).collect()
}

/// What a backend answered, in a form two backends can be compared on.
#[derive(Debug, PartialEq, Eq)]
pub struct Observed {
    pub latest_page: Vec<String>,
    pub older_page: Vec<String>,
    pub newer_page: Vec<String>,
    pub by_key: Option<(String, u64)>,
    pub unread_and_read: (u32, u64),
    pub unread_after_read: (u32, u64),
}

pub async fn run(stores: &StoreProvider) -> Observed {
    // Summary and unread come first: once message rows exist, a backend may already own
    // the summary through its projection receipt (SQLite stages it with the rows), and a
    // later summary cannot override that. Core flows always go through the projection.
    let conversation = Conversation {
        conversation_id: CID.into(),
        max_seq: 5,
        unread_count: 5,
        ..Default::default()
    };
    stores.conversations.save_one(&conversation).await.unwrap();
    let read = |c: Option<Conversation>| {
        let c = c.expect("conversation");
        (c.unread_count, c.last_read_seq)
    };
    let unread_and_read = read(stores.conversations.get(CID).await.unwrap());
    stores.conversations.update_unread(CID, 2, 3).await.unwrap();
    let unread_after_read = read(stores.conversations.get(CID).await.unwrap());

    let messages: Vec<_> = (1..=5).map(inbound).collect();
    stores.messages.save_batch(&messages).await.unwrap();
    // A replayed row converges on the same message.
    stores.messages.save_batch(&messages[2..3]).await.unwrap();

    let latest = stores
        .messages
        .get_by_conversation(CID, 0, 2)
        .await
        .unwrap();
    let oldest_seen = latest.iter().map(|m| m.conversation_seq).min().unwrap();
    let older = stores
        .messages
        .get_by_conversation(CID, oldest_seen, 2)
        .await
        .unwrap();
    let newer = stores
        .messages
        .get_by_conversation_after(CID, 2, 10)
        .await
        .unwrap();
    let key = MessageKey {
        conversation_id: CID.into(),
        sender_id: "peer".into(),
        client_msg_id: "contract-c3".into(),
    };
    let by_key = stores
        .messages
        .get_by_message_key(&key)
        .await
        .unwrap()
        .map(|m| (m.server_id, m.conversation_seq));

    Observed {
        latest_page: ids(&latest),
        older_page: ids(&older),
        newer_page: ids(&newer),
        by_key,
        unread_and_read,
        unread_after_read,
    }
}
