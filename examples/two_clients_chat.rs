//! 双终端文字聊天示例：每个终端只登录 `--user`，与 `--peer` 单聊。
//! 发送与接收均通过 `tracing` **info** 日志输出（请设置 `RUST_LOG=info`）。
//!
//! ## 启动服务端
//!
//! ```bash
//! cd flare-core-server/deploy
//! docker compose up -d consul redis postgres nats rustfs
//! cd ..
//! make start-core-fast
//! ```
//!
//! ## 双终端联调
//!
//! 开两个终端，分别运行下面两条命令（每终端仅一条 WebSocket 连接，不会互踢）：
//!
//! ```bash
//! cd flare-im-core-sdk
//!
//! # 终端 1
//! RUST_LOG=info cargo run --example two_clients_chat --features lifecycle-sqlite -- --user alice --peer bob
//!
//! # 终端 2
//! RUST_LOG=info cargo run --example two_clients_chat --features lifecycle-sqlite -- --user bob --peer alice
//! ```
//!
//! 输入：直接打字回车发送；`/quit` 退出。提示符为 `{user}> `。

use std::env;
use std::io::{self, Write};
use std::path::Path;

use flare_im_core_sdk::ErrorCode;
use flare_im_core_sdk::SdkConfigOverlay;
use flare_im_core_sdk::content::message_elem::Elem;
use flare_im_core_sdk::model::conversation::ConversationType;
use flare_im_core_sdk::prelude::*;
use tokio::sync::mpsc;
use tracing::{error, info};

struct ChatAccounts {
    user: String,
    peer: String,
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

fn token_secret() -> Result<String> {
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

    Err(FlareError::localized(
        ErrorCode::InvalidParameter,
        "missing TOKEN_SECRET / ACCESS_GATEWAY_TOKEN_SECRET and flare-core-server/logs/.dev-token-secret",
    ))
}

fn io_err(context: &str, error: io::Error) -> FlareError {
    FlareError::localized(ErrorCode::InvalidParameter, format!("{context}: {error}"))
}

fn non_empty_env(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(value) = env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn resolve_chat_accounts() -> Result<ChatAccounts> {
    let mut args = env::args().skip(1).peekable();
    let mut user = None::<String>;
    let mut peer = None::<String>;
    let mut positional = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--user" | "-u" => user = Some(next_arg_value(&mut args, "--user")?),
            "--peer" | "-p" => peer = Some(next_arg_value(&mut args, "--peer")?),
            other if other.starts_with('-') => {
                return Err(FlareError::localized(
                    ErrorCode::InvalidParameter,
                    format!("未知参数: {other}（使用 --help 查看用法）"),
                ));
            }
            other => positional.push(other.to_string()),
        }
    }

    if user.is_none() && !positional.is_empty() {
        user = Some(positional.remove(0));
    }
    if peer.is_none() && !positional.is_empty() {
        peer = Some(positional.remove(0));
    }
    if !positional.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            format!("多余 positional 参数: {}", positional.join(" ")),
        ));
    }

    if user.is_none() {
        user = non_empty_env(&["FLARE_CHAT_USER", "FLARE_CHAT_USER_A"]);
    }
    if peer.is_none() {
        peer = non_empty_env(&["FLARE_CHAT_PEER", "FLARE_CHAT_USER_B"]);
    }

    let user = match user {
        Some(value) => value,
        None => prompt_account("当前登录用户 (--user): ")?,
    };
    let peer = match peer {
        Some(value) => value,
        None => prompt_account("对方账号 (--peer): ")?,
    };

    if user == peer {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "当前用户与对方账号不能相同",
        ));
    }

    Ok(ChatAccounts { user, peer })
}

fn next_arg_value<I>(args: &mut std::iter::Peekable<I>, flag: &str) -> Result<String>
where
    I: Iterator<Item = String>,
{
    let value = args.next().ok_or_else(|| {
        FlareError::localized(ErrorCode::InvalidParameter, format!("{flag} 需要账号参数"))
    })?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            format!("{flag} 账号不能为空"),
        ));
    }
    Ok(trimmed.to_string())
}

fn print_usage() {
    eprintln!(
        r#"two_clients_chat — 双终端文字聊天（每终端只登录 --user）

终端 1:
  RUST_LOG=info cargo run --example two_clients_chat --features lifecycle-sqlite -- --user alice --peer bob

终端 2:
  RUST_LOG=info cargo run --example two_clients_chat --features lifecycle-sqlite -- --user bob --peer alice

选项:
  -u, --user <id>   本终端登录用户
  -p, --peer <id>   单聊对端 user_id（不替对端建连接）
  -h, --help

环境变量:
  FLARE_CHAT_USER / FLARE_CHAT_PEER
  FLARE_IM_SERVER_URL
"#
    );
}

fn prompt_account(label: &str) -> Result<String> {
    print!("{label}");
    io::stdout()
        .flush()
        .map_err(|e| io_err("stdout flush", e))?;
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|e| io_err("stdin read", e))?;
    let account = line.trim().to_string();
    if account.is_empty() {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "账号不能为空",
        ));
    }
    Ok(account)
}

fn message_text(message: &IMMessage) -> Option<String> {
    if let Some(Elem::Text(text)) = message.content.as_ref()
        && !text.text.is_empty()
    {
        return Some(text.text.clone());
    }
    if !message.text_preview.is_empty() {
        return Some(message.text_preview.clone());
    }
    None
}

struct ChatSide {
    user_id: String,
    peer_id: String,
    apis: ConnectedApis,
}

impl ChatSide {
    async fn send_text(&self, text: &str) -> Result<()> {
        let conversation = self
            .apis
            .conversation_api
            .get_one(&self.peer_id, &ConversationType::Single)
            .await?;
        let message = self
            .apis
            .message_build_api
            .create_text(&conversation.conversation_id, text, false, &[])
            .await?;
        let ack = self.apis.message_api.send_no_oss(message).await?;
        info!(
            sender = %self.user_id,
            peer = %self.peer_id,
            text,
            ack = %ack_label(&ack),
            "chat sent"
        );
        Ok(())
    }
}

fn ack_label(ack: &SendAck) -> String {
    use flare_im_core_sdk::model::message::send_ack;
    match ack.result.as_ref() {
        Some(send_ack::Result::Accepted(accepted)) => {
            format!("seq={}", accepted.conversation_seq)
        }
        Some(send_ack::Result::Error(error)) => {
            format!("error:{}:{}", error.code, error.message)
        }
        None => "missing-result".to_string(),
    }
}

fn parse_input_line(line: &str, user: &str) -> Option<String> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }
    if line.eq_ignore_ascii_case("/quit") || line.eq_ignore_ascii_case("quit") {
        return Some("__quit__".into());
    }

    if let Some(text) = strip_account_prefix(line, user) {
        if text.is_empty() {
            return None;
        }
        return Some(text.to_string());
    }

    Some(line.to_string())
}

fn strip_account_prefix<'a>(line: &'a str, account: &str) -> Option<&'a str> {
    let prefix = format!("{account}>");
    line.strip_prefix(&prefix)
        .or_else(|| line.strip_prefix(&format!("{account} >")))
        .or_else(|| line.strip_prefix(&format!("@{account}")))
        .map(str::trim)
}

fn spawn_stdin_reader(prompt: String) -> mpsc::UnboundedReceiver<String> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        let stdin = io::stdin();
        loop {
            print!("{prompt}");
            let _ = io::stdout().flush();
            let mut line = String::new();
            match stdin.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init()
        .ok();

    let tenant_id = env::var("TENANT_ID").unwrap_or_else(|_| "0".to_string());
    let issuer = env::var("TOKEN_ISSUER").unwrap_or_else(|_| "flare-im-core".to_string());
    let secret = token_secret()?;
    let ws_url =
        env::var("FLARE_IM_SERVER_URL").unwrap_or_else(|_| "ws://localhost:60051".to_string());

    let accounts = resolve_chat_accounts()?;
    let user = accounts.user.clone();
    let peer = accounts.peer.clone();

    info!(
        ws_url = %ws_url,
        user = %user,
        peer = %peer,
        "two_clients_chat starting"
    );

    let overlay = SdkConfigOverlay {
        ws_url: Some(ws_url),
        tenant_id: Some(tenant_id.clone()),
        ..Default::default()
    };

    let client = IMClient::new();
    client
        .init(
            Some(format!("two-clients-chat-{user}")),
            Some(overlay.clone()),
        )
        .await?;
    let token = IMClient::generate_core_token(CoreTokenConfig {
        secret: secret.clone(),
        issuer: issuer.clone(),
        user_id: user.clone(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: Some(tenant_id.clone()),
    })?;
    let apis = client
        .login(&user, Some(&token), LoginDbKind::Sqlite, |_, _| {})
        .await?;

    let side = ChatSide {
        user_id: user.clone(),
        peer_id: peer.clone(),
        apis,
    };

    let local = user.clone();
    let _sub = client.on_message(move |message| {
        let Some(text) = message_text(message) else {
            return;
        };
        if message.sender_id == local {
            return;
        }
        info!(
            receiver = %local,
            sender = %message.sender_id,
            text,
            client_msg_id = %message.client_msg_id,
            "chat received"
        );
    })?;

    info!(
        user = %user,
        peer = %peer,
        "logged in; type message and Enter, /quit to exit"
    );

    let prompt = format!("{user}> ");
    let mut stdin = spawn_stdin_reader(prompt);
    while let Some(line) = stdin.recv().await {
        let Some(text) = parse_input_line(&line, &user) else {
            continue;
        };
        if text == "__quit__" {
            info!("chat exiting");
            break;
        }
        if let Err(err) = side.send_text(&text).await {
            error!(sender = %user, ?err, "chat send failed");
        }
    }

    let _keep_client = client;
    Ok(())
}
