use flare_im_core_sdk::model::conversation::ConversationType;
use flare_im_core_sdk::prelude::*;

#[path = "common/dev_token.rs"]
mod dev_token;
#[path = "common/diagnose.rs"]
mod diagnose;
#[path = "common/send_confirmation.rs"]
mod send_confirmation;
#[path = "common/smoke_config.rs"]
mod smoke_config;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 失败时先把「下一步该做什么」打出来，再原样返回错误。
    // 默认冒泡出的是 Debug 结构体，对第一次跑示例的人几乎没有指导意义。
    diagnose::explain(run().await)
}

async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let secret = dev_token::require()?;
    let client = IMClient::new();
    client
        .init(Some("development".into()), smoke_config::overlay()?)
        .await?;
    let token = IMClient::generate_core_token(CoreTokenConfig {
        secret: secret.clone(),
        issuer: "flare-im-core".to_string(),
        user_id: "event_observer".to_string(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: Some("0".to_string()),
    })?;
    let apis = client
        .login(
            "event_observer",
            Some(&token),
            flare_im_core_sdk_sqlite_im::sqlite(),
            |_, _| {},
        )
        .await?;

    let _messages = client.on_message(|message| {
        println!(
            "message {} seq={}",
            message.client_msg_id, message.conversation_seq
        );
    })?;
    let conversation = apis
        .conversation_api
        .get_one("event_peer", &ConversationType::Single)
        .await?;
    let message = apis
        .message_build_api
        .create_text(&conversation.conversation_id, "hello events", false, &[])
        .await?;
    let confirmation =
        send_confirmation::SendConfirmation::observe(&client, &message.client_msg_id)?;
    apis.message_api.send_no_oss(message).await?;
    println!("persisted send ack: {:?}", confirmation.wait().await?);
    Ok(())
}
