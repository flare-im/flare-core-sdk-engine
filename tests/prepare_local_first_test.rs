//! prepare（本地半段登录）后、connect 之前，本地优先 API 必须可用：
//! 热启动路径依赖 prepare → bootstrap_startup_home 本地出图 → 后台 connect。

use flare_im_core_sdk::client::IMClient;
use flare_im_core_sdk::client::lifecycle::LoginDbKind;
use flare_im_core_sdk::model::StartupHomeSyncRequest;
use flare_im_core_sdk::storage::in_memory_im_provider;

#[tokio::test]
async fn prepare_then_bootstrap_startup_home_serves_local_before_connect() {
    let client = IMClient::new();
    client.init(None, None).await.expect("init");
    client
        .prepare(
            "hot-start-user",
            LoginDbKind::Provided(in_memory_im_provider()),
        )
        .await
        .expect("prepare must succeed without network");

    client
        .conversation_async()
        .await
        .expect("conversation api must be assembled by prepare");

    let response = client
        .bootstrap_startup_home(StartupHomeSyncRequest {
            conversation_limit: 50,
            start_background_convergence: false,
            ..Default::default()
        })
        .await
        .expect("bootstrap_startup_home must serve local snapshot before connect");

    assert!(!response.served_from_local, "empty local store");
    assert!(
        response.degraded_reason.is_some(),
        "offline cold sync should degrade, not fail"
    );
}

#[tokio::test]
async fn prepare_rejects_an_incomplete_account_backend_before_exposing_apis() {
    use flare_im_core_sdk::storage::{
        AccountStoreOpener, StoreProvider, in_memory_empty_im_provider,
    };
    use std::{path::Path, sync::Arc};

    struct MissingWriter;
    #[async_trait::async_trait]
    impl AccountStoreOpener for MissingWriter {
        async fn open(
            &self,
            _root: &Path,
            tenant: &str,
            user: &str,
        ) -> flare_im_core_sdk::Result<StoreProvider> {
            assert_eq!((tenant, user), ("audit-tenant", "audit-user"));
            let mut stores = in_memory_im_provider();
            stores.pending_send_writer = None;
            Ok(stores)
        }
    }

    let root = std::env::temp_dir().join(format!("account-opener-{}", uuid::Uuid::new_v4()));
    let client = IMClient::new();
    client
        .init(
            None,
            Some(flare_im_core_sdk::prelude::SdkConfigOverlay {
                data_url: Some(root.display().to_string()),
                tenant_id: Some("audit-tenant".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    for backend in [
        LoginDbKind::Open(Arc::new(MissingWriter)),
        LoginDbKind::Provided(in_memory_empty_im_provider()),
    ] {
        let error = client.prepare("audit-user", backend).await.unwrap_err();
        assert_eq!(
            error.code(),
            Some(flare_im_core_sdk::ErrorCode::ConfigurationError)
        );
        assert!(client.conversation_async().await.is_err());
    }
    client
        .prepare("audit-user", LoginDbKind::Provided(in_memory_im_provider()))
        .await
        .unwrap();
    assert!(client.conversation_async().await.is_ok());
    client.uninit().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
