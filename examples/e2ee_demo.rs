//! 端到端加密演示：**服务端只看得到密文**。
//!
//! ```bash
//! cd flare-im-core && ./scripts/start_server.sh     # 先起开源栈
//! cd ../flare-core-sdk-engine
//! cargo run --example e2ee_demo --features "lifecycle-sqlite e2ee"
//! ```
//!
//! 演示的是三件事，每一件都当场打印证据：
//!
//! 1. **发出去的是密文** —— 明文经 `ContentEncryptionInterceptor` 换成 Placeholder
//!    信封，原文不出现在线格式里。
//! 2. **等待持久化回执** —— 真正收到 Persisted ACK 后检查 SDK 消息视图。
//!    list 优先本地缓存，不能冒充服务端回读；Core 的容器 smoke 会独立查数据库。
//! 3. **只有持钥方能还原** —— 用 Bob 的密钥解出原文；用第三方的密钥解不开。
//!
//! 密钥协商在这里是**手工**的（两端各自生成 X25519 密钥、直接交换公钥）。
//! 真实产品里公钥的分发与轮换属于身份体系的职责，接口是
//! `E2eeKeyManager` —— 这一步刻意留在开源边界之外，见 flare-core-server/QUICKSTART.md。

use std::sync::Arc;

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

const CONVERSATION_PEER: &str = "e2ee_bob";

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // 失败时先把「下一步该做什么」打出来，再原样返回错误。
    // 默认冒泡出的是 Debug 结构体，对第一次跑示例的人几乎没有指导意义。
    diagnose::explain(run().await)
}

async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let secret = dev_token::require()?;

    // ── 1. 两端各自生成 X25519 密钥，交换公钥 ────────────────────────────
    // 私钥不出本端；线上只走过公钥。
    let (alice_secret, alice_public) = X25519AeadCodec::generate_keypair();
    let (bob_secret, bob_public) = X25519AeadCodec::generate_keypair();

    let alice_codec = Arc::new(X25519AeadCodec::new(alice_secret, bob_public)?);
    let bob_codec = X25519AeadCodec::new(bob_secret, alice_public)?;

    // ── 2. 把加密拦截器挂进客户端 ────────────────────────────────────────
    // 策略解析器决定哪些会话走 E2EE。这里对所有会话统一开启。
    let resolver = Arc::new(StaticConversationEncryptionPolicyResolver::new(
        ConversationEncryptionPolicy::e2e(),
    ));

    // conversation_encryption 是官方接线点：它同时登记 codec 与加密拦截器，
    // 比自己 new 一个 ContentEncryptionInterceptor 更不容易漏掉一半。
    let client = IMClient::builder()
        .stores(in_memory_im_provider())
        .conversation_encryption(resolver, alice_codec)
        .build()?;
    client
        .init(Some("e2ee-demo".into()), smoke_config::overlay()?)
        .await?;

    let token = IMClient::generate_core_token(CoreTokenConfig {
        secret,
        issuer: "flare-im-core".to_string(),
        user_id: "e2ee_alice".to_string(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: Some("0".to_string()),
    })?;
    let apis = client
        .login(
            "e2ee_alice",
            Some(&token),
            LoginDbKind::IndexedDb(in_memory_im_provider()),
            |_, _| {},
        )
        .await?;

    // ── 3. 发一条消息 ────────────────────────────────────────────────────
    let plaintext = "见面地点改到中山路 42 号";
    println!("原文            : {plaintext}");

    let conversation = apis
        .conversation_api
        .get_one(CONVERSATION_PEER, &ConversationType::Single)
        .await?;
    let message = apis
        .message_build_api
        .create_text(&conversation.conversation_id, plaintext, false, &[])
        .await?;
    let confirmation =
        send_confirmation::SendConfirmation::observe(&client, &message.client_msg_id)?;
    apis.message_api.send_no_oss(message).await?;
    confirmation.wait().await?;

    // ── 4. 检查 ACK 后的 SDK 消息视图（优先本地缓存，并非独立服务端读） ──
    // Core 的 smoke runner 另查真实 PostgreSQL，证明服务端实际只存密文。
    let stored = apis
        .message_api
        .list(&conversation.conversation_id, 0, 1)
        .await?;
    let Some(stored) = stored.into_iter().next() else {
        return Err("服务端没有返回刚发的消息".into());
    };

    let envelope =
        encrypted_content_envelope(&stored).ok_or("这条消息不是加密信封 —— 拦截器没有生效")?;

    println!("ACK 后视图类型  : {:?}（不是 Text）", stored.message_type);
    println!("ACK 后文本预览  : {:?}", stored.text_preview);
    println!("ACK 后视图载荷  : {} 字节密文", envelope.ciphertext.len());

    // 消息视图不含明文；服务端独立落盘核查由 Core 容器 smoke 完成。
    let leaked = envelope
        .ciphertext
        .windows(plaintext.len())
        .any(|w| w == plaintext.as_bytes())
        || stored.text_preview.contains(plaintext);
    if leaked {
        return Err("明文泄漏到了服务端载荷".into());
    }
    println!("明文是否泄漏    : 否 ✅");

    // ── 5. 只有持钥方解得开 ──────────────────────────────────────────────
    let opened = bob_codec.decode(&envelope.ciphertext)?;
    // 解出来的是序列化后的 MessageContent（protobuf），不是裸文本。**要按 protobuf 解**：
    // 早先图省事按 UTF-8 强转再滤掉控制字符，protobuf 的字段/长度标记里有可打印字节
    // （如 `$"`）滤不掉，于是这行「Bob 解出」会带上乱码前缀——恰恰是本 demo 唯一要
    // 证明的那件事（仅持钥方可还原）看着像失败了。
    let readable = decode_text(&opened.payload).ok_or("解出的内容不是可识别的文本消息")?;
    println!("Bob 解出        : {readable}");

    let (stranger_secret, _) = X25519AeadCodec::generate_keypair();
    let (_, stranger_peer) = X25519AeadCodec::generate_keypair();
    let stranger = X25519AeadCodec::new(stranger_secret, stranger_peer)?;
    match stranger.decode(&envelope.ciphertext) {
        Ok(_) => return Err("第三方竟然解开了密文".into()),
        Err(_) => println!("第三方解密      : 失败 ✅"),
    }

    if !readable.contains(plaintext) {
        return Err("Bob 解出的内容与原文不一致".into());
    }
    println!(
        "\n✅ 已收到持久化 ACK，SDK 密文视图与持钥方解密验证通过；服务端独立核查须运行 Core 容器 smoke"
    );
    Ok(())
}

/// 从序列化的 `MessageContent` 里取出文本。
///
/// 这里刻意走真正的 protobuf 解码而不是字节转字符串：前者拿到的就是原文，
/// 后者永远要跟编码框架里的杂字节缠斗。
fn decode_text(payload: &[u8]) -> Option<String> {
    use flare_proto::common::{MessageContent, message_content::Content};
    use prost::Message as _;

    match MessageContent::decode(payload).ok()?.content? {
        Content::Text(text) => Some(text.text),
        Content::RichText(rich) => Some(rich.plain_text),
        _ => None,
    }
}
