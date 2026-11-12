use super::*;
use base64::Engine;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

#[derive(Default)]
struct Model {
    id: String,
    active: bool,
    puts: usize,
    revokes: usize,
    auth: Vec<String>,
    lost_reply: bool,
    offline: bool,
    hold: bool,
    journal_root: Option<PathBuf>,
    expires_at_ms: Vec<i64>,
    /// 每次 PUT 登记的请求体。
    bodies: Vec<Value>,
}
struct Fixture {
    base: String,
    model: Arc<Mutex<Model>>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
    root: PathBuf,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("flare-push-sdk-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let model = Arc::new(Mutex::new(Model::default()));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (m, e, r) = (model.clone(), entered.clone(), release.clone());
        let task = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(socket);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let method = line.split_whitespace().next().unwrap().to_owned();
                let mut len = 0;
                let mut auth = String::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).await.unwrap();
                    if header.trim().is_empty() {
                        break;
                    }
                    let (name, value) = header.trim().split_once(':').unwrap();
                    if name.eq_ignore_ascii_case("content-length") {
                        len = value.trim().parse().unwrap();
                    }
                    if name.eq_ignore_ascii_case("authorization") {
                        auth = value.trim().to_owned();
                    }
                }
                assert!(len <= 16384);
                let mut body = vec![0; len];
                reader.read_exact(&mut body).await.unwrap();
                let value: Value = if body.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_slice(&body).unwrap()
                };
                let (status, data, lost, hold) = {
                    let mut m = m.lock().unwrap();
                    m.auth.push(auth);
                    if m.offline {
                        (503, json!({}), false, false)
                    } else if method == "PUT" {
                        let intent = std::fs::read_to_string(
                            m.journal_root
                                .as_ref()
                                .unwrap()
                                .join("push-revocations.json"),
                        )
                        .unwrap();
                        let intents: Vec<Intent> = serde_json::from_str(&intent).unwrap();
                        assert!(
                            intents
                                .iter()
                                .any(|i| digest(&i.revocation_token) == value["bindingId"])
                        );
                        assert!(!intent.contains("OS_PRIVATE") && !intent.contains("Bearer"));
                        m.id = value["bindingId"].as_str().unwrap().into();
                        m.active = true;
                        m.puts += 1;
                        m.expires_at_ms.push(value["expiresAtMs"].as_i64().unwrap());
                        m.bodies.push(value.clone());
                        let lost = std::mem::take(&mut m.lost_reply);
                        (200, json!({"success":true,"bindingId":m.id}), lost, m.hold)
                    } else if method == "GET" {
                        (
                            200,
                            json!({"version":m.id,"currentBindingId":if m.active { m.id.clone() } else { String::new() },"state":if m.active {"active"}else{"absent"}}),
                            false,
                            false,
                        )
                    } else {
                        assert_eq!(method, "POST");
                        assert!(
                            m.auth.last().unwrap().is_empty(),
                            "revocation must not retain a login bearer"
                        );
                        m.revokes += 1;
                        if digest(value["revocationToken"].as_str().unwrap()) == m.id {
                            m.active = false;
                        }
                        (200, json!({"success":true}), false, false)
                    }
                };
                if hold {
                    e.notify_one();
                    r.notified().await;
                }
                if lost {
                    continue;
                }
                let body = json!({"code":0,"data":data}).to_string();
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = reader.get_mut().write_all(response.as_bytes()).await;
            }
        });
        Self {
            base,
            model,
            entered,
            release,
            task,
            root,
        }
    }
    async fn attach(
        &self,
        lifecycle: &PushLifecycle,
        user: &str,
        generation: Arc<AtomicU64>,
    ) -> Result<()> {
        self.attach_with_clock(
            lifecycle,
            user,
            generation,
            crate::kernel::ServerClock::new(),
        )
        .await
    }
    async fn attach_with_clock(
        &self,
        lifecycle: &PushLifecycle,
        user: &str,
        generation: Arc<AtomicU64>,
        server_clock: crate::kernel::ServerClock,
    ) -> Result<()> {
        lifecycle
            .attach(AttachPushSession {
                epoch: generation.load(Ordering::Acquire),
                generation,
                user: user.into(),
                app: "app".into(),
                device: "device".into(),
                http: routed_http(&self.base),
                token: jwt(user),
                root: self.root.clone(),
                server_clock,
            })
            .await?;
        self.model.lock().unwrap().journal_root =
            Some(lifecycle.lock().journal.as_ref().unwrap().root().to_owned());
        Ok(())
    }
}
/// 开发模式下 Core API 指向替身服务的接入配置。
fn development_access(base: &str) -> flare_access::AccessConfig {
    flare_access::AccessConfig::development(flare_access::DevelopmentEndpoints {
        ws_url: Some("ws://127.0.0.1:9/ws".into()),
        core_api_url: Some(base.into()),
        ..Default::default()
    })
}

/// 与 SDK 一样按接入点路由的 Core HTTP 客户端（推送日志按部署标识归档，替身与 SDK 必须一致）。
fn routed_http(base: &str) -> HttpClient {
    let access = std::sync::Arc::new(crate::runtime::link::AccessRuntime::new(
        Some(development_access(base)),
        crate::runtime::link::AccessStoreSource::memory(),
    ));
    HttpClient::routed(
        crate::infrastructure::transport::http::router::HttpRouter::new(
            access,
            flare_access::ServiceClass::CoreApi,
        ),
        std::sync::Arc::new(crate::infrastructure::transport::http::HttpRequestContext::new()),
        flare_core::common::TlsConfig::none(),
    )
}

fn jwt(user: &str) -> String {
    format!(
        "e30.{}.test",
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(json!({"sub":user,"tenant_id":"app","device_id":"device"}).to_string())
    )
}
fn token(value: &str) -> PushTokenInput {
    PushTokenInput {
        provider: "fcm".into(),
        platform: "android".into(),
        token: value.into(),
        app_bundle: None,
        environment: None,
        locale: None,
        time_zone: None,
        app_version: None,
        capabilities: None,
    }
}

/// 带全套元数据的 APNs 登记。
fn apns_with_metadata() -> PushTokenInput {
    PushTokenInput {
        provider: "apns".into(),
        platform: "ios".into(),
        token: "ab12".into(),
        app_bundle: Some("com.example.app".into()),
        environment: Some("sandbox".into()),
        locale: Some("zh-CN".into()),
        time_zone: Some("Asia/Shanghai".into()),
        app_version: Some("1.2.3 (45)".into()),
        capabilities: Some(vec!["serviceExtension".into()]),
    }
}

/// 登记附带的元数据原样进 PUT（键名与输入相同）。同样的输入再报不重新登记；任何一项变了都像换了
/// 令牌一样：下一次对账撤销旧登记、按 CAS 重新登记。能力的先后不算变化。
#[tokio::test]
async fn binding_metadata_is_sent_and_a_change_re_registers() {
    let f = Fixture::new().await;
    let lifecycle = PushLifecycle::default();
    f.attach(&lifecycle, "alice", Arc::new(AtomicU64::new(1)))
        .await
        .unwrap();
    lifecycle.permission(true);
    let mut input = apns_with_metadata();
    lifecycle.update(input.clone()).unwrap();
    assert_eq!(lifecycle.reconcile().await.state, "registered");
    {
        let m = f.model.lock().unwrap();
        let body = m.bodies.last().unwrap();
        assert_eq!(body["appBundle"], "com.example.app");
        assert_eq!(body["environment"], "sandbox");
        assert_eq!(body["locale"], "zh-CN");
        assert_eq!(body["timeZone"], "Asia/Shanghai");
        assert_eq!(body["appVersion"], "1.2.3 (45)");
        assert_eq!(body["capabilities"], json!(["serviceExtension"]));
    }

    lifecycle.update(input.clone()).unwrap();
    assert_eq!(lifecycle.reconcile().await.state, "registered");
    assert_eq!(f.model.lock().unwrap().puts, 1, "同样的输入不重新登记");

    input.locale = Some("en-US".into());
    lifecycle.update(input.clone()).unwrap();
    assert_eq!(lifecycle.reconcile().await.state, "registered");
    {
        let m = f.model.lock().unwrap();
        assert_eq!(m.puts, 2);
        assert_eq!(m.revokes, 1, "旧登记先撤销");
        assert_eq!(m.bodies.last().unwrap()["locale"], "en-US");
    }

    let mut both = input.clone();
    both.capabilities = Some(vec!["dataMessages".into(), "serviceExtension".into()]);
    let mut swapped = input.clone();
    swapped.capabilities = Some(vec!["serviceExtension".into(), "dataMessages".into()]);
    assert_eq!(input_digest(&both), input_digest(&swapped));
    assert_ne!(input_digest(&both), input_digest(&input));
    let mut none = input.clone();
    none.app_version = None;
    assert_ne!(input_digest(&none), input_digest(&input));
}

/// 不带元数据时请求体与之前一样，没有多余的键。
#[tokio::test]
async fn a_binding_without_metadata_sends_no_extra_keys() {
    let f = Fixture::new().await;
    let lifecycle = PushLifecycle::default();
    f.attach(&lifecycle, "alice", Arc::new(AtomicU64::new(1)))
        .await
        .unwrap();
    lifecycle.permission(true);
    lifecycle.update(token("OS_PRIVATE")).unwrap();
    assert_eq!(lifecycle.reconcile().await.state, "registered");
    let m = f.model.lock().unwrap();
    let body = m.bodies.last().unwrap().as_object().unwrap();
    let mut keys: Vec<&str> = body.keys().map(String::as_str).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "bindingId",
            "expectedVersion",
            "expiresAtMs",
            "platform",
            "provider",
            "token"
        ]
    );
}

/// 元数据轻校验：环境只给 APNs、只有 sandbox / production；能力只认已知的、不重复；包名、语言、
/// 时区只许常见字符；版本不能空。
#[test]
fn binding_metadata_is_validated() {
    assert!(validate(&apns_with_metadata()).is_ok());
    let rejected = |change: fn(&mut PushTokenInput)| {
        let mut input = apns_with_metadata();
        change(&mut input);
        validate(&input).is_err()
    };
    assert!(rejected(
        |input| input.environment = Some("development".into())
    ));
    assert!(rejected(|input| {
        input.provider = "fcm".into();
        input.platform = "android".into();
    }));
    assert!(rejected(
        |input| input.capabilities = Some(vec!["pushKit".into()])
    ));
    assert!(rejected(|input| {
        input.capabilities = Some(vec!["serviceExtension".into(), "serviceExtension".into()])
    }));
    assert!(rejected(
        |input| input.app_bundle = Some("com.example app".into())
    ));
    assert!(rejected(|input| input.app_bundle = Some(String::new())));
    assert!(rejected(|input| input.locale = Some("z".repeat(36))));
    assert!(rejected(
        |input| input.time_zone = Some("Asia/Shang hai".into())
    ));
    assert!(rejected(|input| input.app_version = Some("  ".into())));
    assert!(rejected(|input| input.app_version = Some("1.0\n".into())));

    let mut fcm = token("OS_PRIVATE");
    fcm.app_bundle = Some("com.example.app".into());
    fcm.capabilities = Some(vec!["dataMessages".into()]);
    fcm.time_zone = Some("Etc/GMT+8".into());
    assert!(validate(&fcm).is_ok(), "FCM 不带环境");

    let parsed: PushTokenInput = serde_json::from_value(json!({
        "provider": "fcm",
        "platform": "android",
        "token": "t",
        "appBundle": "com.example.app",
        "timeZone": "Asia/Shanghai",
        "capabilities": ["dataMessages"]
    }))
    .unwrap();
    assert_eq!(parsed.app_bundle.as_deref(), Some("com.example.app"));
    assert_eq!(parsed.time_zone.as_deref(), Some("Asia/Shanghai"));
    assert!(
        serde_json::from_value::<PushTokenInput>(json!({
            "provider": "fcm", "platform": "android", "token": "t", "bundle": "x"
        }))
        .is_err(),
        "不认识的键直接拒绝"
    );
}

#[tokio::test]
async fn unknown_put_readback_rotation_and_prelogin_restart_recovery() {
    let f = Fixture::new().await;
    let lifecycle = PushLifecycle::default();
    lifecycle.update(token("OS_PRIVATE_1")).unwrap();
    assert_eq!(lifecycle.reconcile().await.state, "disabled");
    let epoch = Arc::new(AtomicU64::new(1));
    f.attach(&lifecycle, "alice", epoch.clone()).await.unwrap();
    assert_eq!(lifecycle.reconcile().await.state, "disabled");
    lifecycle.permission(true);
    f.model.lock().unwrap().lost_reply = true;
    assert_eq!(lifecycle.reconcile().await.state, "pending");
    assert_eq!(f.model.lock().unwrap().puts, 1);
    assert_eq!(lifecycle.reconcile().await.state, "registered");
    assert_eq!(
        f.model.lock().unwrap().puts,
        1,
        "unknown PUT is inspected, not replayed"
    );
    lifecycle.update(token("OS_PRIVATE_2")).unwrap();
    assert_eq!(lifecycle.reconcile().await.state, "registered");
    assert_eq!(f.model.lock().unwrap().revokes, 1);
    epoch.store(2, Ordering::Release);
    assert_eq!(lifecycle.status().registered_providers, 0);
    lifecycle.detach();
    f.model.lock().unwrap().offline = true;
    assert_eq!(lifecycle.reconcile().await.pending_revocations, 1);
    drop(lifecycle);
    f.model.lock().unwrap().offline = false;
    let restarted = PushLifecycle::default();
    restarted
        .configure(routed_http(&f.base), f.root.clone(), "device")
        .await
        .unwrap();
    assert_eq!(restarted.reconcile().await.pending_revocations, 0);
    assert!(
        !f.model.lock().unwrap().active,
        "restart can revoke from login page without JWT"
    );
    assert_eq!(f.model.lock().unwrap().puts, 2);
}

#[tokio::test]
async fn permission_revoked_during_put_and_account_auth_is_pinned() {
    let f = Fixture::new().await;
    let lifecycle = Arc::new(PushLifecycle::default());
    let epoch = Arc::new(AtomicU64::new(1));
    f.attach(&lifecycle, "alice", epoch.clone()).await.unwrap();
    lifecycle.update(token("OS_PRIVATE")).unwrap();
    lifecycle.permission(true);
    f.model.lock().unwrap().hold = true;
    let owned = lifecycle.clone();
    let work = tokio::spawn(async move { owned.reconcile().await });
    f.entered.notified().await;
    lifecycle.permission(false);
    epoch.store(2, Ordering::Release);
    lifecycle.detach();
    f.model.lock().unwrap().hold = false;
    f.release.notify_one();
    assert_eq!(work.await.unwrap().registered_providers, 0);
    lifecycle.reconcile().await;
    assert!(!f.model.lock().unwrap().active);
    f.attach(&lifecycle, "bob", epoch).await.unwrap();
    lifecycle.permission(true);
    assert_eq!(lifecycle.reconcile().await.state, "registered");
    let m = f.model.lock().unwrap();
    assert!(
        m.auth
            .iter()
            .any(|a| a == &format!("Bearer {}", jwt("alice")))
    );
    assert!(
        m.auth
            .iter()
            .any(|a| a == &format!("Bearer {}", jwt("bob")))
    );
}

#[tokio::test]
async fn cancelled_put_keeps_revoke_intent_after_server_effect() {
    let f = Fixture::new().await;
    let lifecycle = Arc::new(PushLifecycle::default());
    f.attach(&lifecycle, "alice", Arc::new(AtomicU64::new(1)))
        .await
        .unwrap();
    lifecycle.update(token("OS_PRIVATE")).unwrap();
    lifecycle.permission(true);
    f.model.lock().unwrap().hold = true;
    let owned = lifecycle.clone();
    let task = tokio::spawn(async move { owned.reconcile().await });
    f.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    lifecycle.detach();
    f.model.lock().unwrap().hold = false;
    f.release.notify_one();
    assert_eq!(lifecycle.reconcile().await.pending_revocations, 0);
    assert!(!f.model.lock().unwrap().active);
    assert_eq!(f.model.lock().unwrap().puts, 1);
}

#[tokio::test]
async fn storage_lease_endpoint_fence_and_stale_session_fail_closed() {
    let f = Fixture::new().await;
    let lifecycle = PushLifecycle::default();
    let epoch = Arc::new(AtomicU64::new(1));
    f.attach(&lifecycle, "alice", epoch.clone()).await.unwrap();
    let root = lifecycle.lock().journal.as_ref().unwrap().root().to_owned();
    assert!(
        Journal::open(root.clone()).is_err(),
        "exclusive journal lease"
    );
    lifecycle.update(token("OS_PRIVATE")).unwrap();
    lifecycle.permission(true);
    assert_eq!(lifecycle.reconcile().await.state, "registered");
    assert!(
        lifecycle
            .configure(
                HttpClient::new("http://127.0.0.1:1"),
                f.root.clone(),
                "device"
            )
            .await
            .is_err()
    );
    assert_eq!(f.model.lock().unwrap().puts, 1);
    epoch.store(2, Ordering::Release);
    let result = lifecycle
        .attach(AttachPushSession {
            epoch: 1,
            generation: epoch,
            user: "alice".into(),
            app: "app".into(),
            device: "device".into(),
            http: routed_http(&f.base),
            token: jwt("alice"),
            root: f.root.clone(),
            server_clock: crate::kernel::ServerClock::new(),
        })
        .await;
    assert!(result.is_err());
    let bytes = std::fs::read_to_string(root.join("push-revocations.json")).unwrap();
    assert!(
        !bytes.contains("OS_PRIVATE") && !bytes.contains(&f.base) && !bytes.contains(&jwt("alice"))
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(root.join("push-revocations.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn validates_os_input_and_private_journal_bounds() {
    assert!(validate(&token("OS_PRIVATE")).is_ok());
    for bad in ["", "x y", "x\ny"] {
        assert!(validate(&token(bad)).is_err());
    }
    assert_eq!(
        path("a/b?c#d e%"),
        "/api/v1/devices/a%2Fb%3Fc%23d%20e%25/push-token"
    );
    assert!(!format!("{:?}", token("OS_PRIVATE")).contains("OS_PRIVATE"));
    let root = std::env::temp_dir().join(format!("flare-push-journal-{}", uuid::Uuid::new_v4()));
    {
        let mut j = Journal::open(root.clone()).unwrap();
        for i in 0..64 {
            j.add(Intent {
                revocation_token: format!("{i:064x}"),
                endpoint: "a".repeat(64),
                provider: "fcm".into(),
            })
            .unwrap();
        }
        assert!(j.add(j.entries[0].clone()).is_err());
    }
    std::fs::write(root.join("push-revocations.json"), "x".repeat(65537)).unwrap();
    assert!(Journal::open(root.clone()).is_err());
    #[cfg(unix)]
    {
        std::fs::remove_file(root.join("push-revocations.json")).unwrap();
        std::os::unix::fs::symlink("push-revocations.lock", root.join("push-revocations.json"))
            .unwrap();
        assert!(Journal::open(root.clone()).is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn normal_init_recovers_pending_revoke_before_login() {
    let f = Fixture::new().await;
    {
        let old = PushLifecycle::default();
        f.attach(&old, "alice", Arc::new(AtomicU64::new(1)))
            .await
            .unwrap();
        old.update(token("OS_PRIVATE")).unwrap();
        old.permission(true);
        assert_eq!(old.reconcile().await.state, "registered");
    }
    let client = crate::client::IMClient::new();
    client
        .init(
            None,
            Some(crate::client::lifecycle::SdkConfigOverlay {
                data_url: Some(f.root.to_str().unwrap().into()),
                access: Some(development_access(&f.base)),
                device_id: Some("device".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if f.model.lock().unwrap().revokes == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!f.model.lock().unwrap().active);
    assert_eq!(f.model.lock().unwrap().puts, 1);
}

#[tokio::test]
async fn public_reconcile_reopens_journal_after_old_instance_releases_lease() {
    let f = Fixture::new().await;
    let old = PushLifecycle::default();
    f.attach(&old, "alice", Arc::new(AtomicU64::new(1)))
        .await
        .unwrap();
    old.update(token("OS_PRIVATE")).unwrap();
    old.permission(true);
    assert_eq!(old.reconcile().await.state, "registered");
    let client = crate::client::IMClient::new();
    client
        .init(
            None,
            Some(crate::client::lifecycle::SdkConfigOverlay {
                data_url: Some(f.root.to_str().unwrap().into()),
                access: Some(development_access(&f.base)),
                device_id: Some("device".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert!(
        f.model.lock().unwrap().active,
        "a competing journal lease cannot be bypassed"
    );
    drop(old);
    assert_eq!(
        client.reconcile_push().await.unwrap().pending_revocations,
        0
    );
    assert!(!f.model.lock().unwrap().active);
}

#[tokio::test]
async fn revoke_capability_is_not_forwarded_on_http_redirect() {
    let trap = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let target = format!("http://{}/capture", trap.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4096];
        let _ = socket.read(&mut bytes).await.unwrap();
        socket.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    });
    let result: Result<HttpApiResponse<Revoked>> = io(HttpClient::new(base).post(
        "/api/v1/push/bindings/revoke",
        &json!({"revocationToken":"a".repeat(64)}),
    ))
    .await;
    assert!(result.is_err());
    peer.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), trap.accept())
            .await
            .is_err()
    );
}

/// 宿主没给设备 id 时推送不开恢复日志：拿临时 id 去开的话，每次启动都是新 id，日志目录
/// 一次一个堆在数据根下，里面的撤销意图下次启动谁也接不上。宿主给了设备才开。
#[tokio::test]
async fn a_client_without_the_hosts_device_opens_no_push_journal() {
    let f = Fixture::new().await;
    let client = crate::client::IMClient::new();
    client
        .init(
            None,
            Some(crate::client::lifecycle::SdkConfigOverlay {
                data_url: Some(f.root.to_str().unwrap().into()),
                access: Some(development_access(&f.base)),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let journals = f.root.join("push-lifecycle");
    assert!(
        !journals.exists(),
        "no device of the host's, no push journal"
    );
    client.bind_device_id(Some("device".into())).await;
    assert!(journals.is_dir(), "the host's device opens its journal");
}

/// The server refuses a binding deadline outside its own next ten minutes.
/// With the device clock minutes behind (or ahead of) the server, the
/// deadline is still counted from the server's clock.
#[tokio::test]
async fn binding_deadline_is_counted_from_the_server_clock() {
    for ahead_ms in [-5 * 60_000_i64, 15 * 60_000] {
        let f = Fixture::new().await;
        let lifecycle = PushLifecycle::default();
        lifecycle.update(token("OS_PRIVATE_1")).unwrap();
        lifecycle.permission(true);
        let clock = crate::kernel::server_clock::test_support::clock_with_device_ahead_by(ahead_ms);
        f.attach_with_clock(
            &lifecycle,
            "alice",
            Arc::new(AtomicU64::new(1)),
            clock.clone(),
        )
        .await
        .unwrap();
        assert_eq!(lifecycle.reconcile().await.state, "registered");
        let deadline = *f.model.lock().unwrap().expires_at_ms.last().unwrap();
        let server_now = clock.now_ms();
        assert!(
            deadline > server_now && deadline <= server_now + 600_000,
            "deadline {deadline} outside the server's window from {server_now} (device ahead by {ahead_ms} ms)"
        );
    }
}
