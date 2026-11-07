use std::sync::Arc;
use std::time::Duration;

use super::{HeartbeatAppState, IMClient, NetworkChangeEvent, SdkConfigOverlay, SdkState};
use crate::infrastructure::persistence::in_memory_empty_im_provider;
use crate::infrastructure::transport::http::HttpRequestContext;
use crate::shared::error::ErrorCode;
use crate::shared::util::CoreTokenConfig;

#[tokio::test]
async fn app_state_snapshot_updates_without_session() {
    let client = IMClient::new();
    assert!(client.is_app_foreground_snapshot());

    client
        .set_app_state(HeartbeatAppState::Background)
        .await
        .expect("background app state should be accepted before login");
    assert!(!client.is_app_foreground_snapshot());

    client
        .set_app_state(HeartbeatAppState::Foreground)
        .await
        .expect("foreground app state should be accepted before login");
    assert!(client.is_app_foreground_snapshot());
}

#[test]
fn generate_core_token_requires_explicit_signing_config() {
    let err = IMClient::generate_core_token(CoreTokenConfig {
        secret: String::new(),
        issuer: "flare-im-core".to_string(),
        user_id: "alice".to_string(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: None,
    })
    .expect_err("production build must not mint unsigned or default-signed tokens");

    assert_eq!(
        err.code(),
        Some(crate::shared::error::ErrorCode::ConfigurationError)
    );
}

#[tokio::test]
async fn uninit_clears_init_configuration() {
    let client = IMClient::new();
    let data_root =
        std::env::temp_dir().join(format!("flare-im-uninit-test-{}", std::process::id()));
    client
        .init(
            Some("dev".to_string()),
            Some(SdkConfigOverlay {
                data_url: Some(format!("file://{}", data_root.display())),
                access: Some(development_access()),
                ..SdkConfigOverlay::default()
            }),
        )
        .await
        .expect("init sdk");

    assert!(client.data_root().await.is_some());
    client.uninit().await.expect("uninit sdk");

    let (environment, sdk_config) = client.config_snapshot().await;
    assert!(environment.is_none());
    assert!(sdk_config.is_none());
    assert!(client.data_root().await.is_none());
    assert!(!client.session_active_sync());
    let _ = tokio::fs::remove_dir_all(data_root).await;
}

#[tokio::test]
async fn session_active_sync_is_false_for_prepared_but_disconnected_user() {
    let client = IMClient::new();
    {
        let mut inner = client.inner.write().await;
        inner.current_user_id = Some("alice".to_string());
    }

    assert!(!client.session_active_sync());
}

#[tokio::test]
async fn network_change_without_session_only_updates_the_fingerprint() {
    let client = IMClient::new();
    client
        .notify_network_change(NetworkChangeEvent {
            available: true,
            interface: Some("wifi".to_string()),
            fingerprint_hint: Some("10.0.0".to_string()),
            ..Default::default()
        })
        .await
        .expect("network change before init is accepted");
    assert_eq!(
        client.connection_status().await.phase,
        crate::kernel::event::ConnectionPhase::Idle
    );
}

#[test]
fn weak_client_upgrade_fails_after_last_strong_handle_drops() {
    let client = IMClient::new();
    let weak = client.downgrade();

    {
        let upgraded = weak.upgrade();
        assert!(upgraded.is_some());
    }

    drop(client);

    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn view_api_refresh_worker_does_not_keep_released_api_alive() {
    for iteration in 0..32 {
        let weak_view = {
            let client = IMClient::builder()
                .stores(in_memory_empty_im_provider())
                .build()
                .expect("build client");
            let view_api = {
                let inner = client.inner.read().await;
                inner.view_api.clone().expect("view api")
            };
            let weak_view = Arc::downgrade(&view_api);
            drop(view_api);
            drop(client);
            weak_view
        };

        for _ in 0..10 {
            if weak_view.upgrade().is_none() {
                break;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        assert!(
            weak_view.upgrade().is_none(),
            "ViewApi stayed alive after release on iteration {iteration}"
        );
    }
}

#[tokio::test]
async fn session_active_sync_is_independent_of_transport_state() {
    let client = IMClient::builder()
        .stores(in_memory_empty_im_provider())
        .build()
        .expect("build client");

    assert!(!client.session_active_sync());

    let apis = {
        let inner = client.inner.read().await;
        IMClient::connected_apis_from_inner(&inner).expect("connected apis")
    };
    client.store_connected_apis_snapshot(apis);
    assert!(client.session_active_sync());

    client.store_state_snapshot(SdkState::Ready);
    assert!(client.session_active_sync());

    client.clear_session_snapshot();
    assert!(!client.session_active_sync());
}

#[tokio::test]
async fn api_getters_use_session_snapshot_when_inner_lock_is_busy() {
    let client = IMClient::builder()
        .stores(in_memory_empty_im_provider())
        .build()
        .expect("build client");
    let apis = {
        let inner = client.inner.read().await;
        IMClient::connected_apis_from_inner(&inner).expect("connected apis")
    };
    client.store_connected_apis_snapshot(apis);
    client.store_state_snapshot(SdkState::Ready);

    let _locked = client.inner.write().await;

    client.message().expect("message api from snapshot");
    client
        .message_build()
        .expect("message builder from snapshot");
    client
        .conversation()
        .expect("conversation api from snapshot");
    client.media().expect("media api from snapshot");
    client.capability().expect("capability api from snapshot");
    client.presence().expect("presence api from snapshot");
    client
        .capability_registry()
        .expect("capability registry from snapshot");
}

/// 宿主拿到的服务端时钟就是引擎采样出来的那一份：设备快 5 分钟、慢 3 分钟都照实给出，还没有样本时
/// 说「还不知道」而不是「偏差为 0」。
#[tokio::test]
async fn the_server_clock_handed_to_hosts_is_the_engine_estimate() {
    for ahead_ms in [5 * 60_000_i64, -3 * 60_000] {
        let client = IMClient::builder()
            .stores(in_memory_empty_im_provider())
            .build()
            .expect("build client");
        let estimate = client.server_clock().await;
        assert_eq!((estimate.offset_ms, estimate.estimated), (0, false));

        let device_ms = crate::shared::util::now_millis() as i64;
        client
            .inner
            .read()
            .await
            .engine
            .as_ref()
            .expect("engine")
            .server_clock()
            .observe_server_ms((device_ms - ahead_ms) as u64);
        let estimate = client.server_clock().await;
        assert!(estimate.estimated);
        assert!(
            (estimate.offset_ms + ahead_ms).abs() < 1_000,
            "device ahead by {ahead_ms} ms gave offset {} ms",
            estimate.offset_ms
        );
    }
}

#[tokio::test]
async fn update_access_token_refreshes_core_bearer_without_overwriting_bff_bearer() {
    let context = Arc::new(HttpRequestContext::new());
    context
        .set_gateway_context(
            "old-gateway-token".to_string(),
            "tenant-a".to_string(),
            "alice".to_string(),
            None,
        )
        .await;
    let client = IMClient::new();
    context
        .set_auth_context("old-im-token".to_string(), None)
        .await;
    {
        let mut inner = client.inner.write().await;
        inner.current_user_id = Some("alice".to_string());
        inner.connect_token = Some("old-im-token".to_string());
        inner.http_request_context = Some(context.clone());
    }
    client.store_state_snapshot(SdkState::Ready);

    client
        .update_access_token("new-im-token", Some("tenant-a"))
        .await
        .expect("update token");

    let headers = context.build_headers().await;
    assert_eq!(
        headers.get("Authorization").map(String::as_str),
        Some("Bearer old-gateway-token")
    );
    assert_eq!(
        headers.get("x-tenant-id").map(String::as_str),
        Some("tenant-a")
    );
    assert_eq!(headers.get("x-user-id").map(String::as_str), Some("alice"));
    let core_headers = context.build_core_headers().await;
    assert_eq!(
        core_headers.get("Authorization").map(String::as_str),
        Some("Bearer new-im-token")
    );
    assert_eq!(client.access_token().await.as_deref(), Some("new-im-token"));
}

#[tokio::test]
async fn update_access_token_rejects_prepared_but_disconnected_session() {
    let context = Arc::new(HttpRequestContext::new());
    context
        .set_gateway_context(
            "old-gateway-token".to_string(),
            "tenant-a".to_string(),
            "alice".to_string(),
            None,
        )
        .await;
    let client = IMClient::new();
    context
        .set_auth_context("old-im-token".to_string(), None)
        .await;
    {
        let mut inner = client.inner.write().await;
        inner.current_user_id = Some("alice".to_string());
        inner.connect_token = Some("old-im-token".to_string());
        inner.http_request_context = Some(context.clone());
    }

    let err = client
        .update_access_token("new-gateway-token", Some("tenant-b"))
        .await
        .expect_err("prepared but disconnected session must not refresh HTTP auth");
    assert_eq!(err.code(), Some(ErrorCode::NotConnected));

    let headers = context.build_headers().await;
    assert_eq!(
        headers.get("Authorization").map(String::as_str),
        Some("Bearer old-gateway-token")
    );
    assert_eq!(
        headers.get("x-tenant-id").map(String::as_str),
        Some("tenant-a")
    );
    assert_eq!(
        context
            .build_core_headers()
            .await
            .get("Authorization")
            .map(String::as_str),
        Some("Bearer old-im-token")
    );
}

#[tokio::test]
async fn disconnect_clears_shared_http_auth_context() {
    let context = Arc::new(HttpRequestContext::new());
    context.set_auth_context("im-token".to_string(), None).await;
    context
        .set_gateway_context(
            "gateway-token".to_string(),
            "tenant-a".to_string(),
            "alice".to_string(),
            None,
        )
        .await;
    let client = IMClient::new();
    {
        let mut inner = client.inner.write().await;
        inner.current_user_id = Some("alice".to_string());
        inner.connect_token = Some("im-token".to_string());
        inner.http_request_context = Some(context.clone());
    }

    client.disconnect().await.expect("disconnect");

    let headers = context.build_headers().await;
    assert_eq!(headers.get("Authorization"), None);
    assert_eq!(headers.get("x-user-id"), None);
    assert_eq!(headers.get("x-tenant-id").map(String::as_str), Some("0"));
}

/// 契约：设备标识必须由**同一处**决定，供登录签 token 与 CONNECT 上报共用。
/// 网关在两侧都非空且不等时直接拒连，此前两处各自取值无对齐机制，只能给登录传空绕过。
#[tokio::test]
async fn bind_device_id_writes_then_reads_back_the_same_value() {
    let client = IMClient::new();

    // 未配置：只读返回 None（连接侧届时回退到进程级临时标识）
    assert_eq!(client.bind_device_id(None).await, None);

    // 写入后回显，且后续只读拿到同一个值 —— 登录与连接因此同源
    assert_eq!(
        client
            .bind_device_id(Some("device-alpha".to_string()))
            .await,
        Some("device-alpha".to_string())
    );
    assert_eq!(
        client.bind_device_id(None).await,
        Some("device-alpha".to_string()),
        "a later read must not invent a different device id"
    );

    // 空白输入按未提供处理：不得清空已绑定值
    assert_eq!(
        client.bind_device_id(Some("   ".to_string())).await,
        Some("device-alpha".to_string()),
        "blank input is a read, not a reset"
    );
}

/// `init` 已配置 device_id 时，只读路径要能取到它（社交登录据此复用同一个值）。
#[tokio::test]
async fn bind_device_id_reads_value_configured_at_init() {
    let client = IMClient::new();
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                device_id: Some("device-from-init".to_string()),
                ..Default::default()
            }),
        )
        .await
        .expect("init must succeed");

    assert_eq!(
        client.bind_device_id(None).await,
        Some("device-from-init".to_string())
    );
}

#[tokio::test]
async fn prepared_session_searches_offline_and_invalidates_old_user() {
    use crate::client::lifecycle::LoginDbKind;
    let client = IMClient::new();
    let root = std::env::temp_dir().join(format!("flare-offline-session-{}", std::process::id()));
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(format!("file://{}", root.display())),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    client
        .prepare(
            "alice",
            LoginDbKind::IndexedDb(in_memory_empty_im_provider()),
        )
        .await
        .unwrap();
    let alice_generation = client.session_generation_snapshot();
    assert_eq!(client.state(), SdkState::Disconnected);
    assert!(client.session_active_sync());
    let alice_api = client.message().unwrap();
    assert!(
        alice_api
            .search_in_conversation("c1", "hello", 10)
            .await
            .unwrap()
            .is_empty()
    );
    // A failed connection must not revoke local API access.
    client.clear_session_snapshot();
    assert!(client.connected_apis().await.is_ok());
    client
        .prepare("bob", LoginDbKind::IndexedDb(in_memory_empty_im_provider()))
        .await
        .unwrap();
    assert!(client.session_generation_snapshot() > alice_generation);
    assert_eq!(client.current_user_id().await.as_deref(), Some("bob"));
    assert!(alice_api.search("hello", 10).await.is_err());
    let bob_api = client.message().unwrap();
    client.logout().await.unwrap();
    assert!(!client.session_active_sync());
    assert!(client.message().is_err());
    assert!(bob_api.search("hello", 10).await.is_err());
}

/// 记下被调了几次的宿主续期回调。
struct CountingRefresher {
    token: Option<String>,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl crate::client::token_provider::ConnectTokenRefresher for CountingRefresher {
    async fn refresh_connect_token(&self) -> Option<String> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.token.clone()
    }
}

fn counting_refresher(token: Option<&str>) -> Arc<CountingRefresher> {
    Arc::new(CountingRefresher {
        token: token.map(str::to_string),
        calls: std::sync::atomic::AtomicUsize::new(0),
    })
}

async fn host_managed_client(generation: u64, refresher: Arc<CountingRefresher>) -> IMClient {
    let client = IMClient::new();
    {
        let mut g = client.inner.write().await;
        g.session_generation = generation;
        g.connect_token = Some("expired".to_string());
        g.connect_token_refresher = Some(refresher);
    }
    client
}

#[tokio::test]
async fn a_rejected_connect_takes_a_fresh_token_from_the_host() {
    let refresher = counting_refresher(Some("fresh"));
    let client = host_managed_client(3, refresher.clone()).await;

    assert_eq!(
        client.renew_connect_token(3, "expired").await.as_deref(),
        Some("fresh")
    );
    assert_eq!(
        client.inner.read().await.connect_token.as_deref(),
        Some("fresh"),
        "the next reconnect attempt must read the new token"
    );
    assert_eq!(refresher.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_same_token_handed_back_is_not_a_swap() {
    // 宿主刷新没成功（冷却中、刷新令牌已用过）时原样返回旧令牌：不能当成换到了，
    // 否则重连会从头计退避、对着网关快速空转。
    for unchanged in [Some("expired"), Some("  "), None] {
        let refresher = counting_refresher(unchanged);
        let client = host_managed_client(3, refresher).await;
        assert_eq!(client.renew_connect_token(3, "expired").await, None);
        assert_eq!(
            client.inner.read().await.connect_token.as_deref(),
            Some("expired")
        );
    }
}

#[tokio::test]
async fn a_superseded_session_does_not_ask_the_host() {
    let refresher = counting_refresher(Some("fresh"));
    let client = host_managed_client(4, refresher.clone()).await;

    assert_eq!(client.renew_connect_token(3, "expired").await, None);
    assert_eq!(refresher.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        client.inner.read().await.connect_token.as_deref(),
        Some("expired")
    );
}

#[tokio::test]
async fn sdk_managed_tokens_are_left_to_the_gateway_issuer() {
    let refresher = counting_refresher(Some("fresh"));
    let client = host_managed_client(3, refresher.clone()).await;
    // SDK 托管：续期只走 Core API（这里没有接入运行时，换不到），宿主回调也不该被问到。
    client.inner.write().await.sdk_config = Some(SdkConfigOverlay {
        auth: Some(crate::client::config::SdkAuthConfig::default()),
        ..Default::default()
    });

    assert_eq!(client.renew_connect_token(3, "expired").await, None);
    assert_eq!(refresher.calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn the_host_refresher_survives_the_login_rebuild() {
    use crate::client::lifecycle::LoginDbKind;
    let client = IMClient::new();
    let root = std::env::temp_dir().join(format!("flare-refresher-{}", std::process::id()));
    client
        .init(
            None,
            Some(SdkConfigOverlay {
                data_url: Some(format!("file://{}", root.display())),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    client
        .set_connect_token_refresher(Some(counting_refresher(Some("fresh"))))
        .await;
    client
        .prepare(
            "alice",
            LoginDbKind::IndexedDb(in_memory_empty_im_provider()),
        )
        .await
        .unwrap();
    assert!(client.inner.read().await.connect_token_refresher.is_some());
    client.logout().await.unwrap();
}

/// 模拟宿主的另一条推送路径：刷新成功后先把新令牌写进核心，再把它返回。
struct PushingRefresher {
    client: std::sync::Mutex<Option<IMClient>>,
}

#[async_trait::async_trait]
impl crate::client::token_provider::ConnectTokenRefresher for PushingRefresher {
    async fn refresh_connect_token(&self) -> Option<String> {
        let client = self.client.lock().unwrap().clone()?;
        // 没配 HTTP 上下文时同步那一步会报错，但令牌已先写入，正是要模拟的情形。
        let _ = client.update_access_token("fresh", None).await;
        Some("fresh".to_string())
    }
}

#[tokio::test]
async fn a_token_pushed_in_by_the_host_first_still_counts_as_a_swap() {
    let refresher = Arc::new(PushingRefresher {
        client: std::sync::Mutex::new(None),
    });
    let client = IMClient::new();
    {
        let mut g = client.inner.write().await;
        g.session_generation = 3;
        g.connect_token = Some("expired".to_string());
        g.connect_token_refresher = Some(refresher.clone());
    }
    *refresher.client.lock().unwrap() = Some(client.clone());

    assert_eq!(
        client.renew_connect_token(3, "expired").await.as_deref(),
        Some("fresh")
    );
    assert_eq!(
        client.inner.read().await.connect_token.as_deref(),
        Some("fresh")
    );
}

/// 构建期配好的接入配置在登录重建会话时不丢；接入运行时（健康簿、熔断）跨会话共用同一个。
#[tokio::test]
async fn the_build_time_access_config_and_runtime_survive_the_login_rebuild() {
    use crate::client::config::SdkConfig;
    use crate::client::lifecycle::LoginDbKind;
    let client = IMClient::builder()
        .config(SdkConfig::with_access(development_access()))
        .stores(in_memory_empty_im_provider())
        .build()
        .unwrap();
    let before = client
        .inner
        .read()
        .await
        .access
        .clone()
        .expect("access runtime");
    client
        .prepare(
            "alice",
            LoginDbKind::IndexedDb(in_memory_empty_im_provider()),
        )
        .await
        .unwrap();
    let g = client.inner.read().await;
    assert_eq!(
        g.configured_config
            .as_ref()
            .and_then(|config| config.access.clone()),
        Some(development_access())
    );
    assert!(Arc::ptr_eq(&before, g.access.as_ref().unwrap()));
    drop(g);
    client.logout().await.unwrap();
}

#[test]
fn an_overlay_still_overrides_the_build_time_config() {
    use crate::client::config::SdkConfig;
    let mut base = SdkConfig::with_access(development_access());
    base.tenant_id = Some("t-base".to_string());
    let other = flare_access::AccessConfig::development(flare_access::DevelopmentEndpoints {
        ws_url: Some("ws://127.0.0.1:61051/ws".into()),
        ..Default::default()
    });
    let merged = crate::client::lifecycle::merge_sdk_config_onto(
        base,
        Some(&SdkConfigOverlay {
            access: Some(other.clone()),
            ..Default::default()
        }),
    );
    assert_eq!(merged.access, Some(other));
    assert_eq!(merged.tenant_id.as_deref(), Some("t-base"));
}

/// 同一用户重新 prepare（含冷启动）不清空发件箱；换了用户才清空。
#[tokio::test]
async fn the_same_user_keeps_the_outbox_across_prepare() {
    use crate::client::lifecycle::LoginDbKind;
    use crate::domain::PendingSendVo;
    let client = IMClient::builder()
        .config(crate::client::config::SdkConfig::with_access(
            development_access(),
        ))
        .stores(in_memory_empty_im_provider())
        .build()
        .unwrap();
    let stores = crate::infrastructure::persistence::memory_im::in_memory_im_provider();
    let (reader, writer) = stores.pending_sends().expect("pending store");
    let mut message = crate::model::IMMessage::new(flare_proto::common::Message::default());
    message.client_msg_id = "m-fresh".into();
    message.conversation_id = "c1".into();
    message.sender_id = "alice".into();
    let mut stale = message.clone();
    stale.client_msg_id = "m-stale".into();
    let now = crate::shared::util::now_millis();
    writer
        .push(PendingSendVo {
            client_msg_id: "m-fresh".into(),
            conversation_id: "c1".into(),
            message,
            enqueued_at_ms: now,
        })
        .await
        .unwrap();
    writer
        .push(PendingSendVo {
            client_msg_id: "m-stale".into(),
            conversation_id: "c1".into(),
            message: stale,
            enqueued_at_ms: now - super::PENDING_RESUME_MAX_AGE_MS - 1_000,
        })
        .await
        .unwrap();
    client
        .prepare("alice", LoginDbKind::IndexedDb(stores.clone()))
        .await
        .unwrap();
    client.logout().await.unwrap();
    client
        .prepare("alice", LoginDbKind::IndexedDb(stores.clone()))
        .await
        .unwrap();
    let left: Vec<String> = reader
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.client_msg_id)
        .collect();
    assert_eq!(
        left,
        vec!["m-fresh".to_owned()],
        "同一用户：新的留着，超过 24 小时的判失败"
    );

    client.logout().await.unwrap();
    client
        .prepare("bob", LoginDbKind::IndexedDb(stores))
        .await
        .unwrap();
    assert!(reader.list().await.unwrap().is_empty(), "换了用户：清空");
    client.logout().await.unwrap();
}

fn development_access() -> flare_access::AccessConfig {
    flare_access::AccessConfig::development(flare_access::DevelopmentEndpoints {
        ws_url: Some("ws://127.0.0.1:9/ws".into()),
        core_api_url: Some("http://127.0.0.1:9".into()),
        ..Default::default()
    })
}

/// 把当前线程的 tracing 输出收进内存，供断言「会话结束后没有再记重连成功」。
#[derive(Clone, Default)]
struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

struct CapturedLogsWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogsWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogsWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedLogsWriter(self.0.clone())
    }
}

impl CapturedLogs {
    /// 只在当前线程生效：`#[tokio::test]` 的单线程运行时里，SDK 后台任务也在这条线程上跑。
    fn install(&self) -> tracing::subscriber::DefaultGuard {
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(self.clone())
                .with_max_level(tracing::Level::DEBUG)
                .with_ansi(false)
                .finish(),
        )
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer")).into_owned()
    }
}

#[derive(Clone, Copy, Debug)]
enum SessionEnd {
    Logout,
    Disconnect,
    DropClient,
}

/// 会话结束（登出、断开、客户端销毁）时，状态快照与令牌刷新两个监视器都立刻收手。
/// 此前令牌刷新监视器要睡到下一次刷新（这里约 55 分钟后）才发现会话已结束。
async fn session_watchers_stop_when_the_session_ends(end: SessionEnd) {
    use crate::client::config::{SdkAuthConfig, SdkConfig};
    use crate::shared::util::generate_core_token;

    let logs = CapturedLogs::default();
    let _logs = logs.install();

    // 开发端点指向本机不监听的端口：本用例不会真去连，只要不碰外网。
    let client = IMClient::builder()
        .config(SdkConfig::with_access(development_access()))
        .stores(in_memory_empty_im_provider())
        .build()
        .expect("build client");
    // SDK 托管令牌，令牌一小时后到期：刷新监视器会睡到到期前 5 分钟。
    let token = generate_core_token(&CoreTokenConfig {
        secret: "a-strong-shared-secret-with-more-than-32-bytes!".into(),
        issuer: "flare-im-core".into(),
        user_id: "alice".into(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: None,
    })
    .expect("token");
    let bus = {
        let mut inner = client.inner.write().await;
        inner.current_user_id = Some("alice".to_string());
        inner.connect_token = Some(token);
        inner.sdk_config = Some(SdkConfigOverlay {
            auth: Some(SdkAuthConfig::default()),
            ..SdkConfigOverlay::default()
        });
        inner.engine.as_ref().expect("engine").bus().clone()
    };
    let generation = client.session_generation_snapshot();
    client.spawn_state_snapshot_watcher(generation, bus.clone());
    client.spawn_token_refresh_watcher(generation);
    // 让两个监视器都进入等待
    tokio::time::sleep(Duration::from_millis(50)).await;

    match end {
        SessionEnd::Logout => client.logout().await.expect("logout"),
        SessionEnd::Disconnect => client.disconnect().await.expect("disconnect"),
        SessionEnd::DropClient => drop(client),
    }
    let ended_at = std::time::Instant::now();
    for watcher in [
        "SDK state snapshot watcher stopped",
        "SDK token refresh watcher stopped",
    ] {
        while !logs.text().contains(watcher) {
            assert!(
                ended_at.elapsed() < Duration::from_millis(500),
                "{end:?}: {watcher:?} must happen as soon as the session ends\n{}",
                logs.text()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    // 测试持有总线，它不会因客户端销毁而关闭
    drop(bus);
}

#[tokio::test]
async fn session_watchers_stop_on_logout() {
    session_watchers_stop_when_the_session_ends(SessionEnd::Logout).await;
}

#[tokio::test]
async fn session_watchers_stop_on_disconnect() {
    session_watchers_stop_when_the_session_ends(SessionEnd::Disconnect).await;
}

#[tokio::test]
async fn session_watchers_stop_when_the_client_is_destroyed() {
    session_watchers_stop_when_the_session_ends(SessionEnd::DropClient).await;
}

/// 宿主注入的登录前全局存储：装上后 init 建的接入运行时用它；传 `None` 后再 init 还原平台默认。
#[tokio::test]
async fn init_uses_the_injected_access_store_until_it_is_cleared() {
    let client = IMClient::new();
    let data_root =
        std::env::temp_dir().join(format!("flare-im-access-store-test-{}", std::process::id()));
    let overlay = || SdkConfigOverlay {
        data_url: Some(format!("file://{}", data_root.display())),
        access: Some(development_access()),
        ..SdkConfigOverlay::default()
    };
    let injected: Arc<dyn flare_access::AccessStore> =
        Arc::new(flare_access::store::MemoryAccessStore::new());
    let is_injected = |store: &Arc<dyn flare_access::AccessStore>| {
        std::ptr::addr_eq(Arc::as_ptr(store), Arc::as_ptr(&injected))
    };

    client.set_access_store(Some(injected.clone())).await;
    client.init(None, Some(overlay())).await.expect("init sdk");
    let access = client
        .inner
        .read()
        .await
        .access
        .clone()
        .expect("access runtime");
    let kernel = access.kernel().await.expect("development directory");
    assert!(
        is_injected(kernel.store()),
        "init must use the injected store"
    );

    client.set_access_store(None).await;
    client
        .init(None, Some(overlay()))
        .await
        .expect("init sdk again");
    let access = client
        .inner
        .read()
        .await
        .access
        .clone()
        .expect("access runtime");
    let kernel = access.kernel().await.expect("development directory");
    assert!(
        !is_injected(kernel.store()),
        "None restores the platform default"
    );

    client.uninit().await.expect("uninit sdk");
    let _ = tokio::fs::remove_dir_all(data_root).await;
}

/// 注入的登录前全局存储跨会话保留：prepare（登录重建客户端）之后再 init，用的仍是注入的那份。
#[tokio::test]
async fn the_injected_access_store_survives_the_login_rebuild() {
    use crate::client::lifecycle::LoginDbKind;
    let client = IMClient::new();
    let data_root = std::env::temp_dir().join(format!(
        "flare-im-access-store-rebuild-{}",
        std::process::id()
    ));
    let overlay = || SdkConfigOverlay {
        data_url: Some(format!("file://{}", data_root.display())),
        access: Some(development_access()),
        ..SdkConfigOverlay::default()
    };
    let injected: Arc<dyn flare_access::AccessStore> =
        Arc::new(flare_access::store::MemoryAccessStore::new());

    client.set_access_store(Some(injected.clone())).await;
    client.init(None, Some(overlay())).await.expect("init sdk");
    client
        .prepare(
            "alice",
            LoginDbKind::IndexedDb(in_memory_empty_im_provider()),
        )
        .await
        .expect("prepare");
    client
        .init(None, Some(overlay()))
        .await
        .expect("init after login");

    let access = client
        .inner
        .read()
        .await
        .access
        .clone()
        .expect("access runtime");
    let kernel = access.kernel().await.expect("development directory");
    assert!(
        std::ptr::addr_eq(Arc::as_ptr(kernel.store()), Arc::as_ptr(&injected)),
        "init after the login rebuild must still use the injected store"
    );

    client.logout().await.expect("logout");
    client.uninit().await.expect("uninit sdk");
    let _ = tokio::fs::remove_dir_all(data_root).await;
}
