//! 接入验收用的真实客户端：一个进程一个引擎客户端，生产信任模式。
//!
//! `scripts/access_acceptance.py` 拉起它：配置走 JSON 文件，命令走 stdin（一行一个 JSON），
//! 发生的事走 stdout（一行一个 JSON，带本机毫秒时间戳）。
//!
//! ```sh
//! access_lab_client run --config client.json
//! access_lab_client keygen                      # 打印一把实验台在线密钥
//! access_lab_client sign < job.json             # 用它签一份信封，负向用例造文档用
//! access_lab_client upgrade-frame 30201         # 打印一帧 UPGRADE_REQUIRED 拒绝，给替身网关用
//! ```
//!
//! DNS（`*.localhost`）、TLS（私有 CA 走 `tls_ca_cert_path`）、验签、选路、链路状态机都是生产路径。
//! 与生产的差别只在本机没有 443：清单里的接入点主机不带端口，所以 [`AccessLab`] 把 IM 入口和
//! well-known 拉取改到回环上的 SNI 路由器；令牌由脚本向 api-gateway 申请后传入，不经过 Edge；
//! 这份运行时没装探活传输（平台实现不对外），客户端的 ping 探活不在这里跑。

use std::io::{BufRead, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flare_im_core_sdk::client::config::{SdkAppConfig, SdkConfig};
use flare_im_core_sdk::link::flare_access::store::MemoryAccessStore;
use flare_im_core_sdk::link::flare_access::trust::TrustRootConfig;
use flare_im_core_sdk::link::flare_access::trust::sign::{
    Signer, SigningKey, encode_public_key, sign_envelope,
};
use flare_im_core_sdk::link::flare_access::{FetchError, FetchRequest, FetchResponse, HttpFetch};
use flare_im_core_sdk::link::{AccessLab, AccessRuntime, AccessStoreSource, PlatformHttpFetch};
use flare_im_core_sdk::model::conversation::ConversationType;
use flare_im_core_sdk::prelude::*;
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientConfig {
    data_dir: PathBuf,
    user: String,
    tenant: String,
    bootstrap_id: String,
    trust_roots: Vec<TrustRootConfig>,
    /// 种子信封文件；没有时靠数据目录里的 LKG。
    seed_file: Option<PathBuf>,
    ca_file: PathBuf,
    /// 周期上报 `tick` 的间隔。
    #[serde(default = "default_tick")]
    tick_seconds: u64,
    /// 不登录，只装配接入目录（负向用例不需要长连接）：走平台默认运行时，没有任何接缝。
    #[serde(default)]
    no_login: bool,
    /// 脚本向 api-gateway 申请的 IM 令牌。
    token: Option<String>,
    route: Option<LoopbackRoute>,
    /// 宿主应用信息，更新中心按它挑发布清单、判断版本。
    #[serde(default)]
    app: SdkAppConfig,
    /// 链路和更新中心共用注入的运行时，不调 `init`：看服务端拒绝这个版本时更新中心的反应。
    #[serde(default)]
    single_runtime: bool,
}

/// 接入点主机名（不带端口）改到回环上的这个端口；镜像 URL 自带端口，不受影响。
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoopbackRoute {
    host_suffix: String,
    port: u16,
}

impl LoopbackRoute {
    fn apply(&self, url: &str) -> String {
        let Some((scheme, rest)) = url.split_once("://") else {
            return url.to_owned();
        };
        let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
        if authority.contains(':') || !authority.ends_with(&self.host_suffix) {
            return url.to_owned();
        }
        format!("{scheme}://{authority}:{}{path}", self.port)
    }
}

struct LoopbackFetch {
    http: PlatformHttpFetch,
    route: LoopbackRoute,
}

#[async_trait::async_trait]
impl HttpFetch for LoopbackFetch {
    async fn fetch(
        &self,
        request: &FetchRequest,
    ) -> std::result::Result<FetchResponse, FetchError> {
        let mut routed = request.clone();
        routed.url = self.route.apply(&request.url);
        self.http.fetch(&routed).await
    }
}

fn default_tick() -> u64 {
    5
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// 一行事件。stdout 只放这些，日志走 stderr。
fn emit(kind: &str, data: Value) {
    let line = json!({ "ts": now_ms(), "kind": kind, "data": data });
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

fn value_of<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

async fn tick(client: &IMClient) {
    let status = client.access_status().await.ok();
    let connection = client.connection_status().await;
    emit(
        "tick",
        json!({ "access": status.map(|s| value_of(&s)), "connection": value_of(&connection) }),
    );
}

async fn run_command(client: &IMClient, line: &str) -> bool {
    let command: Value = match serde_json::from_str(line) {
        Ok(command) => command,
        Err(error) => {
            emit(
                "error",
                json!({ "message": format!("bad command: {error}") }),
            );
            return true;
        }
    };
    let op = command["op"].as_str().unwrap_or_default();
    let id = command["id"].clone();
    match op {
        "quit" => return false,
        "status" => match client.access_status().await {
            Ok(status) => {
                let connection = client.connection_status().await;
                emit(
                    "status",
                    json!({ "id": id, "access": value_of(&status), "connection": value_of(&connection) }),
                );
            }
            Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
        },
        "refresh" => {
            let started = now_ms();
            match client.refresh_access().await {
                Ok(report) => emit(
                    "refresh",
                    json!({ "id": id, "elapsedMs": now_ms() - started, "report": value_of(&report) }),
                ),
                Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
            }
        }
        "import" => {
            let link = command["link"].as_str().unwrap_or_default();
            match client.import_access_recovery(link).await {
                Ok(result) => emit("import", json!({ "id": id, "result": value_of(&result) })),
                Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
            }
        }
        "update_state" => {
            let foreground = command["foreground"].as_bool().unwrap_or(false);
            match client.update_state(foreground).await {
                Ok(status) => emit("update", json!({ "id": id, "status": value_of(&status) })),
                Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
            }
        }
        "update_check" => {
            let manual = command["manual"].as_bool().unwrap_or(true);
            match client.check_update(manual).await {
                Ok(status) => emit("update", json!({ "id": id, "status": value_of(&status) })),
                Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
            }
        }
        "update_download" => match client.download_update(true).await {
            Ok(status) => emit("update", json!({ "id": id, "status": value_of(&status) })),
            Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
        },
        "update_install" => match client.install_update().await {
            Ok(plan) => emit("update", json!({ "id": id, "plan": value_of(&plan) })),
            Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
        },
        "update_resources" => match client.update_resources().await {
            Ok(resources) => emit(
                "update",
                json!({ "id": id, "resources": value_of(&resources) }),
            ),
            Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
        },
        "diagnostics" => match client.export_access_diagnostics().await {
            Ok(value) => emit("diagnostics", json!({ "id": id, "value": value })),
            Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
        },
        "send" => {
            let peer = command["to"].as_str().unwrap_or_default();
            let text = command["text"].as_str().unwrap_or("access acceptance");
            let result = async {
                let conversation = client
                    .conversation()?
                    .get_one(peer, &ConversationType::Single)
                    .await?;
                let draft = client
                    .message_build()?
                    .create_text(&conversation.conversation_id, text, false, &[])
                    .await?;
                let client_msg_id = draft.client_msg_id.clone();
                client.message()?.send(draft).await?;
                Ok::<_, flare_im_core_sdk::prelude::FlareError>(client_msg_id)
            }
            .await;
            match result {
                Ok(client_msg_id) => {
                    emit("sent", json!({ "id": id, "clientMsgId": client_msg_id }))
                }
                Err(error) => emit("error", json!({ "id": id, "message": error.to_string() })),
            }
        }
        other => emit(
            "error",
            json!({ "id": id, "message": format!("unknown op {other}") }),
        ),
    }
    true
}

async fn run(config_path: &str) -> anyhow::Result<()> {
    let config: ClientConfig = serde_json::from_slice(&std::fs::read(config_path)?)?;
    let seed = config
        .seed_file
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?;
    std::fs::create_dir_all(&config.data_dir)?;
    let access = AccessConfig::production(config.bootstrap_id.clone(), config.trust_roots, seed);
    let client = if config.no_login {
        IMClient::new()
    } else {
        let route = config
            .route
            .clone()
            .ok_or_else(|| anyhow::anyhow!("a logged-in client needs route"))?;
        let mut sdk = SdkConfig::with_access(access.clone());
        sdk.tenant_id = Some(config.tenant.clone());
        sdk.tls_ca_cert_path = Some(config.ca_file.display().to_string());
        sdk.ack_timeout_secs = Some(10);
        let dial = route.clone();
        let lab = AccessLab {
            http: Arc::new(LoopbackFetch {
                http: PlatformHttpFetch::new(sdk.core_tls_config()),
                route,
            }),
            dial: Arc::new(move |url: &str| dial.apply(url)),
        };
        sdk.app = config.app.clone();
        let runtime = Arc::new(
            AccessRuntime::new(
                Some(access.clone()),
                AccessStoreSource::SqliteAt(config.data_dir.clone()),
            )
            .with_lab(lab),
        );
        if config.single_runtime {
            runtime.install_lab_update(&sdk, config.data_dir.join("update"));
        }
        let client = IMClient::builder()
            .config(sdk)
            .stores(in_memory_im_provider())
            .access_runtime(runtime)
            .build()?;
        // init 另建一份平台运行时，connect 时换成上面这份。它不能碰数据目录里的 LKG。
        client
            .set_access_store(Some(Arc::new(MemoryAccessStore::new())))
            .await;
        client
    };
    let client = Arc::new(client);
    if !config.single_runtime {
        client
            .init(
                None,
                Some(SdkConfigOverlay {
                    data_url: Some(config.data_dir.display().to_string()),
                    access: Some(access),
                    tenant_id: Some(config.tenant.clone()),
                    tls_ca_cert_path: Some(config.ca_file.display().to_string()),
                    ack_timeout_secs: Some(10),
                    app: Some(config.app.clone()),
                    ..Default::default()
                }),
            )
            .await?;
    }
    // 事件总线按会话：不登录时没有可订阅的，结果都在命令的返回里。
    let _connection = if config.no_login {
        None
    } else {
        Some(client.on_event_kind(SdkEventKind::Connection, |event| {
            if let SdkEvent::Connection(event) = event.as_ref() {
                match event {
                    ConnectionEvent::Status(status) => emit("connection", value_of(status)),
                    ConnectionEvent::AccessSwitched(switched) => {
                        emit("switched", value_of(switched))
                    }
                    ConnectionEvent::AccessChanged(changed) => emit("changed", value_of(changed)),
                    _ => {}
                }
            }
        })?)
    };
    emit(
        "started",
        json!({ "user": config.user, "pid": std::process::id() }),
    );
    if !config.no_login {
        let login_started = now_ms();
        // 登录失败时进程留着：宿主这时还要读状态、弹提示。
        match client.connect(&config.user, config.token.as_deref()).await {
            Ok(_) => emit("login", json!({ "elapsedMs": now_ms() - login_started })),
            Err(error) => emit("loginFailed", json!({ "message": error.to_string() })),
        }
    }

    let (lines, mut incoming) = tokio::sync::mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        for line in std::io::stdin()
            .lock()
            .lines()
            .map_while(std::result::Result::ok)
        {
            if lines.send(line).is_err() {
                break;
            }
        }
    });
    let mut ticker = tokio::time::interval(Duration::from_secs(config.tick_seconds.max(1)));
    loop {
        tokio::select! {
            _ = ticker.tick() => tick(&client).await,
            line = incoming.recv() => match line {
                // stdin 关闭等同于父进程没了：退出，不留孤儿客户端。
                None => break,
                Some(line) if !run_command(&client, &line).await => break,
                Some(_) => {}
            },
        }
    }
    emit("stopped", Value::Null);
    Ok(())
}

fn keygen() {
    let mut seed = [0u8; 32];
    let mut random = std::fs::File::open("/dev/urandom").expect("/dev/urandom");
    random.read_exact(&mut seed).expect("read /dev/urandom");
    let key = SigningKey::from_bytes(&seed);
    let hex: String = seed.iter().map(|byte| format!("{byte:02x}")).collect();
    println!(
        "{}",
        json!({ "seedHex": hex, "publicKey": encode_public_key(&key.verifying_key()) })
    );
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignJob {
    seed_hex: String,
    key_id: String,
    payload_type: String,
    /// 原始载荷字节（UTF-8 JSON 文本）。
    payload: String,
    #[serde(default)]
    key_certs: Vec<String>,
}

fn sign() -> anyhow::Result<()> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    let job: SignJob = serde_json::from_str(&input)?;
    let mut seed = [0u8; 32];
    anyhow::ensure!(job.seed_hex.len() == 64, "seedHex is 32 bytes of hex");
    for (index, byte) in seed.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&job.seed_hex[index * 2..index * 2 + 2], 16)?;
    }
    let key = SigningKey::from_bytes(&seed);
    let envelope = sign_envelope(
        &job.payload_type,
        job.payload.as_bytes(),
        &[Signer {
            key_id: &job.key_id,
            key: &key,
        }],
        &job.key_certs,
    );
    std::io::stdout().write_all(&envelope.to_json_bytes())?;
    Ok(())
}

/// 服务端版本门禁还没做时，替身网关下发的那一帧：协商前 JSON，和真网关拒绝 CONNECT 的编码相同。
fn upgrade_frame(min_build: u64) -> anyhow::Result<()> {
    use flare_core::common::link::upgrade_required_command;
    use flare_core::common::message::parser::PRE_NEGOTIATION_PARSER;
    use flare_core::common::protocol::{Reliability, frame_with_system_command};
    let frame = frame_with_system_command(
        upgrade_required_command(min_build, Some("lab")),
        Reliability::AtLeastOnce,
    );
    let bytes = PRE_NEGOTIATION_PARSER.serialize(&frame)?;
    std::io::stdout().write_all(&bytes)?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match arguments.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["run", "--config", path] => run(path).await,
        ["keygen"] => {
            keygen();
            Ok(())
        }
        ["sign"] => sign(),
        ["upgrade-frame", min_build] => upgrade_frame(min_build.parse()?),
        _ => anyhow::bail!(
            "usage: access_lab_client run --config FILE | keygen | sign | upgrade-frame BUILD"
        ),
    }
}
