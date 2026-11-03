//! 端到端验证：已读回执（read-cursor）。
//! alice 发一条消息;bob 收到后 mark_read 到该 seq;alice 应在 on_read_receipt 收到 bob 的已读光标。
//! 运行前需启动本地全栈（ws://localhost:60051）。

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flare_im_core_sdk::SdkConfigOverlay;
use flare_im_core_sdk::prelude::*;

type AnyErr = Box<dyn std::error::Error>;

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

fn token_secret() -> std::result::Result<String, AnyErr> {
    if let Ok(secret) = std::env::var("TOKEN_SECRET")
        .or_else(|_| std::env::var("ACCESS_GATEWAY_TOKEN_SECRET"))
        .or_else(|_| std::env::var("FLARE_CORE_GATEWAY_TOKEN_SECRET"))
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

async fn login_client(
    run: &str,
    user: &str,
    ws: &str,
    secret: &str,
    issuer: &str,
    tenant: &str,
) -> std::result::Result<(IMClient, ConnectedApis), AnyErr> {
    let client = IMClient::new();
    let device_id = format!("{run}-{user}-dev");
    let overlay = SdkConfigOverlay {
        ws_url: Some(ws.to_string()),
        tenant_id: Some(tenant.to_string()),
        device_id: Some(device_id.clone()),
        ..Default::default()
    };
    client
        .init(Some(format!("{run}-{user}")), Some(overlay))
        .await?;
    let token = IMClient::generate_core_token(CoreTokenConfig {
        secret: secret.to_string(),
        issuer: issuer.to_string(),
        user_id: user.to_string(),
        ttl_secs: 3600,
        device_id: Some(device_id),
        tenant_id: Some(tenant.to_string()),
    })?;
    let apis = client
        .login(
            user,
            Some(&token),
            LoginDbKind::IndexedDb(in_memory_im_provider()),
            |_, _| {},
        )
        .await?;
    Ok((client, apis))
}

#[tokio::main]
async fn main() -> std::result::Result<(), AnyErr> {
    let ws = std::env::var("FLARE_IM_SERVER_URL").unwrap_or_else(|_| "ws://localhost:60051".into());
    let secret = token_secret()?;
    let issuer = "flare-im-core";
    let tenant = "0";
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let run = format!("read-e2e-{millis}");
    let alice = format!("read_a_{millis}");
    let bob = format!("read_b_{millis}");
    let roster = vec![alice.clone(), bob.clone()];

    let (alice_c, alice_apis) = login_client(&run, &alice, &ws, &secret, issuer, tenant).await?;
    let (bob_c, bob_apis) = login_client(&run, &bob, &ws, &secret, issuer, tenant).await?;

    // alice 订阅已读回执，记录 bob 已读到的 seq。
    let receipt_seq = Arc::new(AtomicU64::new(0));
    let receipt_seq_cb = receipt_seq.clone();
    let bob_id = bob.clone();
    let _sub = alice_c.on_read_receipt(move |_conv, evt| {
        if evt.user_id == bob_id {
            receipt_seq_cb.store(evt.read_seq, Ordering::SeqCst);
        }
    })?;

    // bob 收到 alice 的消息后记录其 conversation_seq。
    let seen_seq = Arc::new(AtomicU64::new(0));
    let seen_seq_cb = seen_seq.clone();
    let alice_id = alice.clone();
    let _sub2 = bob_c.on_message_batch(move |messages| {
        for message in messages {
            if message.sender_id == alice_id && message.conversation_seq() > 0 {
                seen_seq_cb.store(message.conversation_seq(), Ordering::SeqCst);
            }
        }
    })?;

    tokio::time::sleep(Duration::from_millis(1500)).await;
    let conv = alice_apis
        .conversation_api
        .get_group_by_user_ids(&roster, Some("read receipt e2e"))
        .await?;
    let _ = bob_apis
        .conversation_api
        .get_group_by_user_ids(&roster, Some("read receipt e2e"))
        .await?;
    println!("conversation_id: {}", conv.conversation_id);
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let message = alice_apis
        .message_build_api
        .create_text(&conv.conversation_id, "read me", false, &[])
        .await?;
    alice_apis.message_api.send(message).await?;

    // 等 bob 收到消息拿到 seq。
    let mut bob_seq = 0;
    for _ in 0..60 {
        let s = seen_seq.load(Ordering::SeqCst);
        if s > 0 {
            bob_seq = s;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if bob_seq == 0 {
        return Err("READ_RECEIPT_FAIL: bob did not receive alice's message".into());
    }
    println!("bob received message at conversation_seq={bob_seq}, marking read");

    // bob 标记已读到该 seq。
    bob_apis
        .conversation_api
        .mark_read(&conv.conversation_id, bob_seq)
        .await?;

    // 等 alice 收到已读回执。
    for _ in 0..60 {
        if receipt_seq.load(Ordering::SeqCst) >= bob_seq {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let got = receipt_seq.load(Ordering::SeqCst);
    if got >= bob_seq {
        println!("READ_RECEIPT_OK: alice received bob's read cursor read_seq={got}");
        Ok(())
    } else {
        Err(format!("READ_RECEIPT_FAIL: alice did not receive bob's read receipt (got {got}, want >= {bob_seq})").into())
    }
}
