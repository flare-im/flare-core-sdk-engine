use std::collections::{HashMap, HashSet};
use std::env;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flare_im_core_sdk::SdkConfigOverlay;
use flare_im_core_sdk::model::conversation::ConversationType;
use flare_im_core_sdk::model::message::{SendAck, SendAckDurability, send_ack};
use flare_im_core_sdk::prelude::*;
use tokio::sync::mpsc;

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn run_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("two-user-{millis}")
}

fn default_token_secret_path() -> Option<String> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir.parent()?;
    Some(
        root.join("flare-core-server")
            .join("logs")
            .join(".dev-token-secret")
            .to_string_lossy()
            .to_string(),
    )
}

fn token_secret() -> std::result::Result<String, Box<dyn std::error::Error>> {
    if let Ok(secret) = env::var("TOKEN_SECRET")
        .or_else(|_| env::var("ACCESS_GATEWAY_TOKEN_SECRET"))
        .or_else(|_| env::var("FLARE_CORE_GATEWAY_TOKEN_SECRET"))
    {
        let trimmed = secret.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    if let Some(path) = default_token_secret_path()
        && let Ok(secret) = std::fs::read_to_string(&path)
    {
        let trimmed = secret.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }

    Err("missing TOKEN_SECRET / ACCESS_GATEWAY_TOKEN_SECRET and flare-core-server/logs/.dev-token-secret".into())
}

fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = ((sorted.len() - 1) * numerator) / denominator;
    sorted[index]
}

fn fmt_duration(duration: Duration) -> String {
    format!("{:.3} ms", duration.as_secs_f64() * 1000.0)
}

fn fmt_rate(count: usize, duration: Duration) -> String {
    if duration.is_zero() {
        return "inf msg/s".to_string();
    }
    format!("{:.2} msg/s", count as f64 / duration.as_secs_f64())
}

fn ack_label(ack: &SendAck) -> String {
    match ack.result.as_ref() {
        Some(send_ack::Result::Accepted(accepted)) => {
            format!(
                "accepted:{:?}:seq={}:server={}",
                accepted.durability(),
                accepted.conversation_seq,
                accepted.server_msg_id
            )
        }
        Some(send_ack::Result::Error(error)) => {
            format!("error:{}:{}:{}", error.code, error.reason, error.message)
        }
        None => "missing-result".to_string(),
    }
}

fn is_durable_ack(ack: &SendAck) -> bool {
    match ack.result.as_ref() {
        Some(send_ack::Result::Accepted(accepted)) => matches!(
            accepted.durability(),
            SendAckDurability::WalAccepted
                | SendAckDurability::BrokerAccepted
                | SendAckDurability::Persisted
        ),
        _ => false,
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let total = env_usize("FLARE_E2E_TOTAL", 200);
    let in_flight = env_usize("FLARE_E2E_IN_FLIGHT", 32).min(total);
    let ack_max_in_flight = env_usize("FLARE_E2E_ACK_MAX_IN_FLIGHT", 32);
    let receive_timeout = Duration::from_millis(env_u64("FLARE_E2E_RECEIVE_TIMEOUT_MS", 30_000));
    let tenant_id = env::var("TENANT_ID").unwrap_or_else(|_| "0".to_string());
    let issuer = env::var("TOKEN_ISSUER").unwrap_or_else(|_| "flare-im-core".to_string());
    let secret = token_secret()?;
    let run = run_id();
    let alice_user = env::var("FLARE_E2E_ALICE").unwrap_or_else(|_| format!("{run}-alice"));
    let bob_user = env::var("FLARE_E2E_BOB").unwrap_or_else(|_| format!("{run}-bob"));
    let ws_url =
        env::var("FLARE_IM_SERVER_URL").unwrap_or_else(|_| "ws://localhost:60051".to_string());

    println!("two-user server e2e");
    println!("  ws_url: {ws_url}");
    println!("  alice: {alice_user}");
    println!("  bob: {bob_user}");
    println!("  total: {total}");
    println!("  in_flight: {in_flight}");
    println!("  ack_max_in_flight: {ack_max_in_flight}");

    let alice = IMClient::new();
    let bob = IMClient::new();
    let overlay = SdkConfigOverlay {
        access: Some(AccessConfig::development(DevelopmentEndpoints {
            ws_url: Some(ws_url.clone()),
            ..Default::default()
        })),
        tenant_id: Some(tenant_id.clone()),
        ack_max_in_flight: Some(ack_max_in_flight),
        ..Default::default()
    };
    alice
        .init(Some(format!("{run}-alice-app")), Some(overlay.clone()))
        .await?;
    bob.init(Some(format!("{run}-bob-app")), Some(overlay))
        .await?;

    let alice_token = IMClient::generate_core_token(CoreTokenConfig {
        secret: secret.clone(),
        issuer: issuer.clone(),
        user_id: alice_user.clone(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: Some(tenant_id.clone()),
    })?;
    let bob_token = IMClient::generate_core_token(CoreTokenConfig {
        secret: secret.clone(),
        issuer: issuer.clone(),
        user_id: bob_user.clone(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: Some(tenant_id.clone()),
    })?;

    let alice_apis = alice
        .login(
            &alice_user,
            Some(&alice_token),
            LoginDbKind::Sqlite,
            |_, _| {},
        )
        .await?;
    let _bob_apis = bob
        .login(&bob_user, Some(&bob_token), LoginDbKind::Sqlite, |_, _| {})
        .await?;

    let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();
    let _alice_acks = alice.on_send_ack(move |ack| {
        let _ = ack_tx.send(ack.clone());
    })?;

    let (received_tx, mut received_rx) = mpsc::unbounded_channel();
    let _bob_messages = bob.on_message_batch(move |messages| {
        for message in messages {
            let _ = received_tx.send(message.clone());
        }
    })?;

    let conversation = alice_apis
        .conversation_api
        .get_one(&bob_user, &ConversationType::Single)
        .await?;

    let mut messages = Vec::with_capacity(total);
    for index in 0..total {
        let text = format!("e2e latency throughput {run} #{index}");
        let mut message = None;
        let mut last_error = None;
        for _ in 0..5 {
            match alice_apis
                .message_build_api
                .create_text(&conversation.conversation_id, &text, false, &[])
                .await
            {
                Ok(built) => {
                    message = Some(built);
                    break;
                }
                Err(error) if error.to_string().contains("单聊会话未在本地落库") => {
                    last_error = Some(error.to_string());
                    alice_apis
                        .conversation_api
                        .get_one(&bob_user, &ConversationType::Single)
                        .await?;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        let Some(message) = message else {
            return Err(format!(
                "message build failed at index {index}: {}",
                last_error.unwrap_or_else(|| "unknown error".to_string())
            )
            .into());
        };
        messages.push(message);
    }

    let mut sent_at = HashMap::with_capacity(total);
    let mut send_errors = 0usize;
    let send_started = Instant::now();
    let mut next = 0usize;
    let mut pending = futures::stream::FuturesUnordered::new();

    while next < total || !pending.is_empty() {
        while next < total && pending.len() < in_flight {
            let message = messages[next].clone();
            let client_msg_id = message.client_msg_id.clone();
            sent_at.insert(client_msg_id.clone(), Instant::now());
            let api = alice_apis.message_api.clone();
            pending.push(async move { api.send_no_oss(message).await.map(|_| client_msg_id) });
            next += 1;
        }

        if let Some(Err(error)) = futures::StreamExt::next(&mut pending).await {
            send_errors += 1;
            eprintln!("send error: {error}");
        }
    }
    let send_elapsed = send_started.elapsed();

    let receive_started = Instant::now();
    let deadline = Instant::now() + receive_timeout;
    let mut received_ids = HashSet::with_capacity(total);
    let mut duplicate_count = 0usize;
    let mut unknown_received_count = 0usize;
    let mut first_unknown_received = Vec::new();
    let mut latencies = Vec::with_capacity(total);
    let mut acked_ids = HashMap::with_capacity(total);
    let mut duplicate_ack_count = 0usize;
    let mut durable_ack_count = 0usize;
    let mut ack_latencies = Vec::with_capacity(total);

    while received_ids.len() < total && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            Some(message) = received_rx.recv() => {
                let client_msg_id = message.client_msg_id.clone();
                let Some(started_at) = sent_at.get(&client_msg_id).copied() else {
                    unknown_received_count += 1;
                    if first_unknown_received.len() < 5 {
                        first_unknown_received.push(client_msg_id);
                    }
                    continue;
                };
                if !received_ids.insert(client_msg_id.clone()) {
                    duplicate_count += 1;
                    continue;
                }
                latencies.push(started_at.elapsed());
            }
            Some(ack) = ack_rx.recv() => {
                let client_msg_id = ack.client_msg_id.clone();
                if is_durable_ack(&ack) {
                    durable_ack_count += 1;
                }
                if let Some(started_at) = sent_at.get(&client_msg_id).copied() {
                    ack_latencies.push(started_at.elapsed());
                }
                if acked_ids.insert(client_msg_id, ack_label(&ack)).is_some() {
                    duplicate_ack_count += 1;
                }
            }
            _ = tokio::time::sleep(remaining.min(Duration::from_millis(500))) => {}
        }
    }

    while let Ok(ack) = ack_rx.try_recv() {
        let client_msg_id = ack.client_msg_id.clone();
        if is_durable_ack(&ack) {
            durable_ack_count += 1;
        }
        if let Some(started_at) = sent_at.get(&client_msg_id).copied() {
            ack_latencies.push(started_at.elapsed());
        }
        if acked_ids.insert(client_msg_id, ack_label(&ack)).is_some() {
            duplicate_ack_count += 1;
        }
    }

    latencies.sort_unstable();
    ack_latencies.sort_unstable();
    let received = received_ids.len();
    let acked = acked_ids.len();
    let lost = total.saturating_sub(received);
    let unacked = total.saturating_sub(acked);
    let receive_elapsed = receive_started.elapsed();
    let end_to_end_elapsed = send_started.elapsed();

    println!();
    println!("summary");
    println!("  sent: {total}");
    println!("  send_errors: {send_errors}");
    println!("  send_acked: {acked}");
    println!("  send_unacked: {unacked}");
    println!("  durable_acks: {durable_ack_count}");
    println!("  duplicate_acks: {duplicate_ack_count}");
    println!("  received: {received}");
    println!("  lost: {lost}");
    println!("  duplicates: {duplicate_count}");
    println!("  unknown_received: {unknown_received_count}");
    println!("  send_enqueue_elapsed: {}", fmt_duration(send_elapsed));
    println!("  receive_wait_elapsed: {}", fmt_duration(receive_elapsed));
    println!("  end_to_end_elapsed: {}", fmt_duration(end_to_end_elapsed));
    println!(
        "  send_enqueue_throughput: {}",
        fmt_rate(total, send_elapsed)
    );
    println!(
        "  receive_throughput: {}",
        fmt_rate(received, receive_elapsed)
    );
    println!(
        "  end_to_end_throughput: {}",
        fmt_rate(received, end_to_end_elapsed)
    );
    println!(
        "  ack_latency_min: {}",
        fmt_duration(*ack_latencies.first().unwrap_or(&Duration::ZERO))
    );
    println!(
        "  ack_latency_p50: {}",
        fmt_duration(percentile(&ack_latencies, 50, 100))
    );
    println!(
        "  ack_latency_p95: {}",
        fmt_duration(percentile(&ack_latencies, 95, 100))
    );
    println!(
        "  ack_latency_p99: {}",
        fmt_duration(percentile(&ack_latencies, 99, 100))
    );
    println!(
        "  ack_latency_max: {}",
        fmt_duration(*ack_latencies.last().unwrap_or(&Duration::ZERO))
    );
    println!(
        "  latency_min: {}",
        fmt_duration(*latencies.first().unwrap_or(&Duration::ZERO))
    );
    println!(
        "  latency_p50: {}",
        fmt_duration(percentile(&latencies, 50, 100))
    );
    println!(
        "  latency_p95: {}",
        fmt_duration(percentile(&latencies, 95, 100))
    );
    println!(
        "  latency_p99: {}",
        fmt_duration(percentile(&latencies, 99, 100))
    );
    println!(
        "  latency_max: {}",
        fmt_duration(*latencies.last().unwrap_or(&Duration::ZERO))
    );
    if acked < total {
        let missing = messages
            .iter()
            .filter(|message| !acked_ids.contains_key(&message.client_msg_id))
            .take(5)
            .map(|message| message.client_msg_id.as_str())
            .collect::<Vec<_>>();
        println!("  first_unacked_client_msg_ids: {missing:?}");
    }
    if received < total {
        let missing = messages
            .iter()
            .filter(|message| !received_ids.contains(&message.client_msg_id))
            .take(5)
            .map(|message| message.client_msg_id.as_str())
            .collect::<Vec<_>>();
        println!("  first_lost_client_msg_ids: {missing:?}");
    }
    if !first_unknown_received.is_empty() {
        println!("  first_unknown_received_client_msg_ids: {first_unknown_received:?}");
    }
    if !acked_ids.is_empty() {
        let first_ack_states = acked_ids
            .iter()
            .take(5)
            .map(|(client_msg_id, state)| format!("{client_msg_id}={state}"))
            .collect::<Vec<_>>();
        println!("  first_ack_states: {first_ack_states:?}");
    }

    if send_errors > 0 || unacked > 0 || lost > 0 || duplicate_count > 0 {
        return Err(format!(
            "two-user e2e failed: send_errors={send_errors}, unacked={unacked}, lost={lost}, duplicates={duplicate_count}"
        )
        .into());
    }

    Ok(())
}
