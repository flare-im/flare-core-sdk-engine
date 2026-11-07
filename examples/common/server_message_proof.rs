//! Closed protocol and exact raw-SyncRes matcher for the partition test helper.
//!
//! This is test tooling, not an application SDK API. In particular there is no
//! local-store, Push, HTTP management, SQL, or direct Reader proof path.

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use flare_proto::common::{SingleConversationSync, Sync, SyncRes, sync, sync_res, sync_slice_item};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use url::{Host, Url};

pub const MAX_LINE_BYTES: usize = 16 * 1024;
pub const MAX_LIFETIME_MS: u64 = 900_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema: String,
    pub ws_url: String,
    pub http_url: String,
    #[serde(deserialize_with = "deserialize_optional_path")]
    pub tls_ca_cert_path: Option<String>,
    pub app_id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub channel_id: String,
    pub device_id: String,
    pub token: String,
    pub connect_timeout_ms: u64,
    pub lifetime_ms: u64,
}

fn deserialize_optional_path<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)
}

impl Config {
    pub fn validate(&self) -> Result<(), ()> {
        if self.schema != "flare-sdk-proof-config/1"
            || !(50..=30_000).contains(&self.connect_timeout_ms)
            || !(50..=MAX_LIFETIME_MS).contains(&self.lifetime_ms)
            || self.connect_timeout_ms > self.lifetime_ms
            || matches!(self.app_id.as_str(), "0" | "default" | "*")
            || [
                &self.app_id,
                &self.user_id,
                &self.conversation_id,
                &self.channel_id,
                &self.device_id,
            ]
            .into_iter()
            .any(|value| !valid_id(value))
            || self.token.is_empty()
            || self.token.len() > 8_192
            || self.token.bytes().any(|byte| !(33..=126).contains(&byte))
        {
            return Err(());
        }
        let ws = local_endpoint(&self.ws_url, true)?;
        let http = local_endpoint(&self.http_url, false)?;
        if ws.scheme() == "wss" || http.scheme() == "https" {
            let ca = self.tls_ca_cert_path.as_deref().ok_or(())?;
            let path = Path::new(ca);
            let metadata = std::fs::symlink_metadata(path).map_err(|_| ())?;
            if !path.is_absolute()
                || !metadata.file_type().is_file()
                || metadata.len() == 0
                || metadata.len() > 64 * 1024
            {
                return Err(());
            }
        } else if self.tls_ca_cert_path.is_some() {
            // Do not silently ignore a supplied trust file on a plaintext run.
            return Err(());
        }
        Ok(())
    }

    pub fn binding_sha256(&self) -> String {
        canonical_sha256(&serde_json::json!({
            "app_id": self.app_id,
            "user_id": self.user_id,
            "conversation_id": self.conversation_id,
            "channel_id": self.channel_id,
        }))
    }

    /// Preflight is not a DNS-pinning transport. The SDK later resolves the same
    /// .localhost name for normal TLS SNI; hostile DNS rebinding is out of scope.
    pub async fn check_local_resolution(&self, deadline: Instant) -> Result<(), ()> {
        for raw in [&self.ws_url, &self.http_url] {
            let parsed = Url::parse(raw).map_err(|_| ())?;
            if matches!(parsed.host(), Some(Host::Ipv4(_) | Host::Ipv6(_))) {
                // Config::validate already checked literal loopback addresses;
                // no DNS preflight (or IPv6 bracket reinterpretation) is needed.
                continue;
            }
            let host = parsed.host_str().ok_or(())?;
            let port = parsed.port_or_known_default().ok_or(())?;
            let addresses = bounded(deadline, tokio::net::lookup_host((host, port)))
                .await?
                .map_err(|_| ())?;
            let mut found = false;
            for address in addresses {
                found = true;
                if !address.ip().is_loopback() {
                    return Err(());
                }
            }
            if !found {
                return Err(());
            }
        }
        Ok(())
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_.:-".contains(&byte))
}

fn localhost_domain(host: &str) -> bool {
    host == "localhost"
        || host.strip_suffix(".localhost").is_some_and(|prefix| {
            !prefix.is_empty()
                && prefix.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && !label.starts_with('-')
                        && !label.ends_with('-')
                        && label
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                })
        })
}

pub fn local_endpoint(raw: &str, websocket: bool) -> Result<Url, ()> {
    if raw.len() > 2_048 || raw.bytes().any(|byte| !(33..=126).contains(&byte)) {
        return Err(());
    }
    let url = Url::parse(raw).map_err(|_| ())?;
    let scheme_ok = if websocket {
        matches!(url.scheme(), "ws" | "wss")
    } else {
        matches!(url.scheme(), "http" | "https")
    };
    let host_ok = match url.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        Some(Host::Domain(host)) => localhost_domain(host),
        None => false,
    };
    if !scheme_ok
        || !host_ok
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default().is_none()
    {
        return Err(());
    }
    Ok(url)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Expected {
    pub server_id: String,
    pub conversation_id: String,
    pub client_msg_id: String,
    pub sender_id: String,
    pub channel_id: String,
    pub conversation_seq: u64,
    pub message_type: i32,
    pub conversation_type: i32,
}

impl Expected {
    pub fn validate(&self, config: &Config) -> Result<(), ()> {
        if [
            &self.server_id,
            &self.conversation_id,
            &self.client_msg_id,
            &self.sender_id,
            &self.channel_id,
        ]
        .into_iter()
        .any(|value| !valid_id(value))
            || self.conversation_seq == 0
            || self.conversation_seq > (i64::MAX as u64) - 1
            || self.message_type != 19
            || self.conversation_type != 1
            || self.conversation_id != config.conversation_id
            || self.channel_id != config.channel_id
            || self.sender_id != config.user_id
        {
            return Err(());
        }
        Ok(())
    }

    pub fn sync_request(&self, device_id: &str) -> Sync {
        Sync {
            device_id: device_id.to_string(),
            payload: Some(sync::Payload::SingleConversation(SingleConversationSync {
                conversation_id: self.conversation_id.clone(),
                after_conversation_seq: 0,
                before_conversation_seq: self.conversation_seq + 1,
                cursor: String::new(),
                limit: 1,
            })),
        }
    }

    pub fn sha256(&self) -> String {
        canonical_sha256(&serde_json::json!({
            "server_id": self.server_id,
            "conversation_id": self.conversation_id,
            "client_msg_id": self.client_msg_id,
            "sender_id": self.sender_id,
            "channel_id": self.channel_id,
            "conversation_seq": self.conversation_seq,
            "message_type": self.message_type,
            "conversation_type": self.conversation_type,
        }))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub schema: String,
    pub request_id: String,
    pub timeout_ms: u64,
    pub expected: Expected,
}

impl Request {
    pub fn validate(&self, config: &Config) -> Result<(), ()> {
        if self.schema != "flare-sdk-proof-request/1"
            || !valid_request_id(&self.request_id)
            || !(50..=MAX_LIFETIME_MS).contains(&self.timeout_ms)
        {
            return Err(());
        }
        self.expected.validate(config)
    }
}

pub fn valid_request_id(raw: &str) -> bool {
    uuid::Uuid::parse_str(raw).is_ok_and(|uuid| uuid.hyphenated().to_string() == raw)
}

pub fn recover_request_id(line: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(line).ok()?;
    let id = value.get("request_id")?.as_str()?;
    valid_request_id(id).then(|| id.to_string())
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum State {
    Verified,
    NotFound,
    Mismatch,
    PermissionDenied,
    Timeout,
    SdkError,
    InvalidInput,
}

pub fn classify_sdk_error(error: &flare_im_core_sdk::FlareError) -> State {
    use flare_im_core_sdk::ErrorCode;
    match error.code() {
        Some(ErrorCode::PermissionDenied) => State::PermissionDenied,
        Some(
            ErrorCode::OperationTimeout | ErrorCode::NetworkTimeout | ErrorCode::ConnectionTimeout,
        ) => State::Timeout,
        _ => State::SdkError,
    }
}

pub fn must_retire(state: State) -> bool {
    matches!(state, State::Timeout | State::SdkError)
}

#[derive(Serialize)]
pub struct ProofResult {
    pub schema: &'static str,
    pub request_id: String,
    pub state: State,
    pub observation_sha256: Option<String>,
    /// Raw SDK method invocations, not a claim of server-side dispatch: the
    /// method may have been cancelled while waiting for its serial RPC mutex.
    pub rpc_attempts: u8,
    pub elapsed_ms: u64,
}

impl ProofResult {
    pub fn new(request_id: String, state: State, rpc_attempts: u8, elapsed: Duration) -> Self {
        Self {
            schema: "flare-sdk-proof-result/1",
            request_id,
            state,
            observation_sha256: None,
            rpc_attempts,
            elapsed_ms: elapsed.as_millis().min(u64::MAX as u128) as u64,
        }
    }
}

pub fn match_server_response(expected: &Expected, response: &SyncRes) -> State {
    let Some(sync_res::Payload::SingleConversation(page)) = &response.payload else {
        return State::Mismatch;
    };
    if page.conversation_id != expected.conversation_id {
        return State::Mismatch;
    }
    if page.items.is_empty() {
        return State::NotFound;
    }
    if page.items.len() != 1 {
        return State::Mismatch;
    }
    let item = &page.items[0];
    let Some(sync_slice_item::Payload::Message(message)) = &item.payload else {
        return State::Mismatch;
    };
    if item.conversation_seq != expected.conversation_seq
        || message.server_id != expected.server_id
        || message.conversation_id != expected.conversation_id
        || message.client_msg_id != expected.client_msg_id
        || message.sender_id != expected.sender_id
        || message.channel_id != expected.channel_id
        || message.conversation_seq != expected.conversation_seq
        || message.message_type != expected.message_type
        || message.conversation_type != expected.conversation_type
        || message.source != flare_proto::common::MessageSource::User as i32
        || message.thread_id.is_some()
        || matches!(
            flare_proto::common::MessageStatus::try_from(message.status),
            Ok(flare_proto::common::MessageStatus::Failed
                | flare_proto::common::MessageStatus::Recalled
                | flare_proto::common::MessageStatus::Deleted)
                | Err(_)
        )
    {
        return State::Mismatch;
    }
    let Some(flare_proto::common::message_content::Content::Custom(content)) = message
        .content
        .as_ref()
        .and_then(|content| content.content.as_ref())
    else {
        return State::Mismatch;
    };
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ProbeContent {
        text: String,
    }
    if content.r#type != "http.json" || content.payload.len() > MAX_LINE_BYTES {
        return State::Mismatch;
    }
    let Ok(body) = serde_json::from_slice::<ProbeContent>(&content.payload) else {
        return State::Mismatch;
    };
    if body.text != expected.client_msg_id {
        return State::Mismatch;
    }
    State::Verified
}

fn canonical_sha256(value: &serde_json::Value) -> String {
    // Sort explicitly even if a consumer enables serde_json/preserve_order.
    // Both closed objects are flat ASCII identities and integer numbers.
    let fields = value
        .as_object()
        .into_iter()
        .flat_map(|map| map.iter())
        .collect::<std::collections::BTreeMap<_, _>>();
    let json = format!(
        "{{{}}}",
        fields
            .into_iter()
            .map(|(key, value)| format!("{}:{value}", serde_json::Value::String(key.clone())))
            .collect::<Vec<_>>()
            .join(",")
    );
    hex::encode(Sha256::digest(json.as_bytes()))
}

/// The absolute outer deadline also includes PacketSender's RPC mutex wait.
/// Dropping this future does NOT establish underlying waiter/socket cleanup;
/// the process owner must retire and reap the helper after timeout/cancel.
pub async fn bounded<F: Future>(deadline: Instant, future: F) -> Result<F::Output, ()> {
    if Instant::now() >= deadline {
        return Err(());
    }
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{
        CustomContent, Message, MessageContent, MessageSource, MessageStatus,
        SingleConversationSyncRes, SyncSkipItem, SyncSliceItem, SyncTombstoneItem,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    fn config() -> Config {
        Config {
            schema: "flare-sdk-proof-config/1".into(),
            ws_url: "ws://127.0.0.1:60051/ws".into(),
            http_url: "http://127.0.0.1:8080".into(),
            tls_ca_cert_path: None,
            app_id: "30".into(),
            user_id: "user-1".into(),
            conversation_id: "conversation-1".into(),
            channel_id: "user-2".into(),
            device_id: "proof-device-1".into(),
            token: "private-token".into(),
            connect_timeout_ms: 500,
            lifetime_ms: 30_000,
        }
    }

    fn expected() -> Expected {
        Expected {
            server_id: "server-1".into(),
            conversation_id: "conversation-1".into(),
            client_msg_id: "client-1".into(),
            sender_id: "user-1".into(),
            channel_id: "user-2".into(),
            conversation_seq: 1,
            message_type: 19,
            conversation_type: 1,
        }
    }

    fn response() -> SyncRes {
        let expected = expected();
        SyncRes {
            payload: Some(sync_res::Payload::SingleConversation(
                SingleConversationSyncRes {
                    conversation_id: expected.conversation_id.clone(),
                    items: vec![SyncSliceItem {
                        conversation_seq: expected.conversation_seq,
                        payload: Some(sync_slice_item::Payload::Message(Message {
                            server_id: expected.server_id,
                            conversation_id: expected.conversation_id,
                            client_msg_id: expected.client_msg_id,
                            sender_id: expected.sender_id,
                            channel_id: expected.channel_id,
                            conversation_seq: expected.conversation_seq,
                            message_type: expected.message_type,
                            conversation_type: expected.conversation_type,
                            source: MessageSource::User as i32,
                            status: MessageStatus::Persisted as i32,
                            content: Some(MessageContent {
                                content: Some(
                                    flare_proto::common::message_content::Content::Custom(
                                        CustomContent {
                                            r#type: "http.json".into(),
                                            payload: serde_json::json!({"text": "client-1"})
                                                .to_string()
                                                .into_bytes(),
                                            ..Default::default()
                                        },
                                    ),
                                ),
                            }),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
        }
    }

    fn page(response: &mut SyncRes) -> &mut SingleConversationSyncRes {
        match response.payload.as_mut().expect("fixture payload") {
            sync_res::Payload::SingleConversation(page) => page,
            _ => panic!("fixture payload type"),
        }
    }

    #[test]
    fn psp_001_only_exact_raw_message_verifies() {
        assert_eq!(
            match_server_response(&expected(), &response()),
            State::Verified
        );
    }

    #[test]
    fn psp_002_empty_server_page_does_not_accept_local_or_pushed_copy() {
        let _optimistic_or_push_copy = response();
        let mut server = response();
        page(&mut server).items.clear();
        assert_eq!(match_server_response(&expected(), &server), State::NotFound);
    }

    #[test]
    fn psp_003_wrong_payload_cannot_verify_watermark() {
        assert_eq!(
            match_server_response(&expected(), &SyncRes::default()),
            State::Mismatch
        );
    }

    #[test]
    fn psp_004_skip_tombstone_and_absent_payload_fail() {
        for payload in [
            Some(sync_slice_item::Payload::Skip(SyncSkipItem::default())),
            Some(sync_slice_item::Payload::Tombstone(
                SyncTombstoneItem::default(),
            )),
            None,
        ] {
            let mut server = response();
            page(&mut server).items[0].payload = payload;
            assert_eq!(match_server_response(&expected(), &server), State::Mismatch);
        }
    }

    #[test]
    fn psp_005_all_eight_message_fields_and_item_sequence_are_exact() {
        for field in 0..9 {
            let mut server = response();
            let item = &mut page(&mut server).items[0];
            if field == 8 {
                item.conversation_seq += 1;
            } else if let Some(sync_slice_item::Payload::Message(message)) = item.payload.as_mut() {
                match field {
                    0 => message.server_id.push('x'),
                    1 => message.conversation_id.push('x'),
                    2 => message.client_msg_id.push('x'),
                    3 => message.sender_id.push('x'),
                    4 => message.channel_id.push('x'),
                    5 => message.conversation_seq += 1,
                    6 => message.message_type += 1,
                    7 => message.conversation_type += 1,
                    _ => unreachable!(),
                }
            }
            assert_eq!(
                match_server_response(&expected(), &server),
                State::Mismatch,
                "field {field}"
            );
        }
    }

    #[test]
    fn psp_006_wrong_page_identity_and_multiple_items_fail() {
        let mut wrong = response();
        page(&mut wrong).conversation_id.push('x');
        assert_eq!(match_server_response(&expected(), &wrong), State::Mismatch);
        let mut multiple = response();
        let copy = page(&mut multiple).items[0].clone();
        page(&mut multiple).items.push(copy);
        assert_eq!(
            match_server_response(&expected(), &multiple),
            State::Mismatch
        );
    }

    #[test]
    fn psp_007_sequence_one_uses_backfill_not_cold_latest_tail() {
        let sync = expected().sync_request("device");
        let Some(sync::Payload::SingleConversation(request)) = sync.payload else {
            panic!("typed request")
        };
        assert_eq!(request.after_conversation_seq, 0);
        assert_eq!(request.before_conversation_seq, 2);
        assert_eq!(request.limit, 1);
        assert!(request.cursor.is_empty());
    }

    #[test]
    fn psp_008_sequence_zero_and_signed_overflow_rejected() {
        for seq in [0, i64::MAX as u64, u64::MAX] {
            let mut item = expected();
            item.conversation_seq = seq;
            assert!(item.validate(&config()).is_err());
        }
        let mut max = expected();
        max.conversation_seq = (i64::MAX as u64) - 1;
        assert!(max.validate(&config()).is_ok());
    }

    #[test]
    fn psp_009_config_and_expected_context_cannot_drift() {
        let mut expected = expected();
        expected.conversation_id = "other".into();
        assert!(expected.validate(&config()).is_err());
        expected = super::tests::expected();
        expected.channel_id = "other".into();
        assert!(expected.validate(&config()).is_err());
        expected = super::tests::expected();
        expected.sender_id = "other".into();
        assert!(expected.validate(&config()).is_err());
    }

    #[test]
    fn psp_010_expected_canonical_fingerprint_matches_python_shape() {
        assert_eq!(
            expected().sha256(),
            "ebb5b8faed795664d037e12f57ab74b6a519bfbd3f69176933ea33789846a5f1"
        );
    }

    #[test]
    fn psp_011_remote_urls_and_credentials_in_url_rejected() {
        for raw in [
            "ws://example.com/ws",
            "ws://10.0.0.1/ws",
            "ws://127.0.0.1/ws?token=x",
            "ws://user:pass@localhost/ws",
            "ws://localhost/ws#x",
            "file:///tmp/ws",
        ] {
            assert!(local_endpoint(raw, true).is_err(), "{raw}");
        }
        for raw in [
            "ws://127.0.0.1/ws",
            "wss://p0e2e.localhost/ws",
            "ws://[::1]/ws",
        ] {
            assert!(local_endpoint(raw, true).is_ok());
        }
    }

    #[test]
    fn psp_012_protocol_refuses_extra_fields_booleans_and_noncanonical_ids() {
        let base = serde_json::json!({"schema":"flare-sdk-proof-request/1","request_id":"d34e7554-d0fa-44c2-9490-4bfb31999f20","timeout_ms":100,"expected":expected()});
        let mut extra = base.clone();
        extra["other"] = serde_json::json!(1);
        assert!(serde_json::from_value::<Request>(extra).is_err());
        let mut boolean = base.clone();
        boolean["expected"]["conversation_seq"] = serde_json::json!(true);
        assert!(serde_json::from_value::<Request>(boolean).is_err());
        assert!(!valid_request_id("D34E7554-D0FA-44C2-9490-4BFB31999F20"));
        assert!(!valid_request_id("not-an-id"));
    }

    #[test]
    fn psp_013_invalid_config_never_echoes_secret() {
        let mut config = config();
        config.ws_url = "ws://example.com/ws".into();
        assert!(config.validate().is_err());
        let result = ProofResult::new(
            "d34e7554-d0fa-44c2-9490-4bfb31999f20".into(),
            State::InvalidInput,
            0,
            Duration::ZERO,
        );
        let output = serde_json::to_string(&result).expect("closed output");
        assert!(!output.contains(&config.token));
        assert!(!output.contains(&config.ws_url));
        assert!(output.contains("\"observation_sha256\":null"));
    }

    #[tokio::test]
    async fn psp_014_outer_deadline_cancels_mutex_wait_without_late_dispatch() {
        let mutex = Arc::new(tokio::sync::Mutex::new(()));
        let held = mutex.lock().await;
        let dispatched = AtomicBool::new(false);
        let result = bounded(Instant::now() + Duration::from_millis(10), async {
            let _lock = mutex.lock().await;
            dispatched.store(true, Ordering::SeqCst);
        })
        .await;
        assert!(result.is_err());
        drop(held);
        tokio::task::yield_now().await;
        assert!(!dispatched.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn psp_015_expired_deadline_drops_pending_future() {
        struct Witness(Arc<AtomicBool>);
        impl Drop for Witness {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let witness = Witness(dropped.clone());
        let future = async move {
            let _witness = witness;
            std::future::pending::<()>().await;
        };
        assert!(bounded(Instant::now(), future).await.is_err());
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn psp_017_expired_deadline_never_polls_even_ready_future() {
        let dispatched = AtomicBool::new(false);
        assert!(
            bounded(Instant::now(), async {
                dispatched.store(true, Ordering::SeqCst);
            })
            .await
            .is_err()
        );
        assert!(!dispatched.load(Ordering::SeqCst));
    }

    #[test]
    fn psp_018_missing_corrupt_or_wrong_custom_body_cannot_verify() {
        for content in [
            None,
            Some(MessageContent::default()),
            Some(MessageContent {
                content: Some(flare_proto::common::message_content::Content::Custom(
                    CustomContent {
                        r#type: "other".into(),
                        payload: b"{\"text\":\"client-1\"}".to_vec(),
                        ..Default::default()
                    },
                )),
            }),
            Some(MessageContent {
                content: Some(flare_proto::common::message_content::Content::Custom(
                    CustomContent {
                        r#type: "http.json".into(),
                        payload: b"{\"text\":\"wrong\"}".to_vec(),
                        ..Default::default()
                    },
                )),
            }),
            Some(MessageContent {
                content: Some(flare_proto::common::message_content::Content::Custom(
                    CustomContent {
                        r#type: "http.json".into(),
                        payload: b"not-json".to_vec(),
                        ..Default::default()
                    },
                )),
            }),
        ] {
            let mut server = response();
            if let Some(sync_slice_item::Payload::Message(message)) =
                &mut page(&mut server).items[0].payload
            {
                message.content = content;
            }
            assert_eq!(match_server_response(&expected(), &server), State::Mismatch);
        }
    }

    #[test]
    fn psp_019_duplicate_or_extra_body_fields_fail() {
        for payload in [
            b"{\"text\":\"wrong\",\"text\":\"client-1\"}".as_slice(),
            b"{\"text\":\"client-1\",\"extra\":1}".as_slice(),
        ] {
            let mut server = response();
            if let Some(sync_slice_item::Payload::Message(message)) =
                &mut page(&mut server).items[0].payload
            {
                if let Some(flare_proto::common::message_content::Content::Custom(content)) =
                    message
                        .content
                        .as_mut()
                        .and_then(|body| body.content.as_mut())
                {
                    content.payload = payload.to_vec();
                }
            }
            assert_eq!(match_server_response(&expected(), &server), State::Mismatch);
        }
    }

    #[test]
    fn psp_020_non_user_thread_and_terminal_status_fail() {
        for status in [
            MessageStatus::Failed,
            MessageStatus::Recalled,
            MessageStatus::Deleted,
        ] {
            let mut server = response();
            if let Some(sync_slice_item::Payload::Message(message)) =
                &mut page(&mut server).items[0].payload
            {
                message.status = status as i32;
            }
            assert_eq!(match_server_response(&expected(), &server), State::Mismatch);
        }
        for field in 0..2 {
            let mut server = response();
            if let Some(sync_slice_item::Payload::Message(message)) =
                &mut page(&mut server).items[0].payload
            {
                if field == 0 {
                    message.source = MessageSource::System as i32;
                } else {
                    message.thread_id = Some("thread".into());
                }
            }
            assert_eq!(match_server_response(&expected(), &server), State::Mismatch);
        }
    }

    #[test]
    fn psp_021_closed_config_requires_explicit_trust_path_field() {
        let json = serde_json::json!({"schema":"flare-sdk-proof-config/1","ws_url":"ws://127.0.0.1/ws","http_url":"http://127.0.0.1","app_id":"30","user_id":"user-1","conversation_id":"conversation-1","channel_id":"user-2","device_id":"device-1","token":"secret","connect_timeout_ms":500,"lifetime_ms":30000});
        assert!(serde_json::from_value::<Config>(json.clone()).is_err());
        let mut explicit = json;
        explicit["tls_ca_cert_path"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<Config>(explicit).is_ok());
        for app_id in ["0", "default", "*"] {
            let mut config = config();
            config.app_id = app_id.into();
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn psp_022_typed_permission_denied_is_terminal_without_helper_retry() {
        let error = flare_im_core_sdk::FlareError::localized(
            flare_im_core_sdk::ErrorCode::PermissionDenied,
            "not parsed",
        );
        assert_eq!(classify_sdk_error(&error), State::PermissionDenied);
        assert!(!must_retire(State::PermissionDenied));
        assert!(must_retire(State::Timeout));
        assert!(must_retire(State::SdkError));
    }
}
