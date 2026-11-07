//! 端到端验证：网关轻量信令直转(typing)。
//! 两个客户端入同一群；alice 发一条消息建立订阅，再 typing(true)；bob 应在 on_typing 收到。
//! 运行前需启动本地全栈（ws://localhost:60051）。

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
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
        access: Some(AccessConfig::development(DevelopmentEndpoints {
            ws_url: Some(ws.to_string()),
            ..Default::default()
        })),
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
    let run = format!("typing-e2e-{millis}");
    let alice = format!("tping_a_{millis}");
    let bob = format!("tping_b_{millis}");
    let roster = vec![alice.clone(), bob.clone()];

    let (_alice_c, alice_apis) = login_client(&run, &alice, &ws, &secret, issuer, tenant).await?;
    let (bob_c, bob_apis) = login_client(&run, &bob, &ws, &secret, issuer, tenant).await?;

    let received = Arc::new(AtomicBool::new(false));
    let received_for_cb = received.clone();
    let alice_id = alice.clone();
    // 超大群标准：网关聚合下发"N 人正在输入"，订阅 on_typing_aggregate。
    let _sub = bob_c.on_typing_aggregate(move |_conv, agg| {
        if agg.typing_count >= 1 && agg.typing_user_ids.iter().any(|u| u == &alice_id) {
            received_for_cb.store(true, Ordering::SeqCst);
        }
    })?;

    tokio::time::sleep(Duration::from_millis(1500)).await;
    let conv = alice_apis
        .conversation_api
        .get_group_by_user_ids(&roster, Some("typing e2e"))
        .await?;
    let _ = bob_apis
        .conversation_api
        .get_group_by_user_ids(&roster, Some("typing e2e"))
        .await?;
    println!("conversation_id: {}", conv.conversation_id);
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // 发一条消息触发首投递成员 bootstrap，确保 bob 被订阅到会话。
    let message = alice_apis
        .message_build_api
        .create_text(&conv.conversation_id, "hello before typing", false, &[])
        .await?;
    alice_apis.message_api.send(message).await?;
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // 发 typing，期望 bob 端在线直转收到。
    alice_apis
        .message_api
        .typing(&conv.conversation_id, true)
        .await?;

    for _ in 0..60 {
        if received.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if received.load(Ordering::SeqCst) {
        println!("TYPING_AGGREGATE_OK: bob received gateway-aggregated 'N typing' including alice");
        Ok(())
    } else {
        Err("TYPING_AGGREGATE_FAIL: bob did not receive typing aggregate within 6s".into())
    }
}
