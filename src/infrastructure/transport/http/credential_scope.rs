//! Explicit, volatile credentials for an externally owned HTTP session.
//! Transport lifecycle code cannot replace this owner's credentials.

use std::sync::Arc;

use crate::shared::error::{ErrorCode, FlareError, Result};

/// The producer's opaque session generation, not an identity inferred from a JWT.
#[derive(Clone, PartialEq, Eq)]
pub struct HttpCredentialOwner {
    pub tenant_id: String,
    pub user_id: String,
    pub generation: u64,
}

/// One coherent credential/principal snapshot. Deliberately not Debug or Serialize.
#[derive(Clone)]
pub struct HttpCredentials {
    pub owner: HttpCredentialOwner,
    pub bearer: String,
    pub session_id: String,
}

/// Reads local authority only: no network, token renewal, persistence or logging.
#[async_trait::async_trait]
pub trait HttpCredentialSource: Send + Sync {
    async fn current(&self) -> Option<HttpCredentials>;
}

/// A request stays with this owner through redirects/retries/renewal.
/// The source may replace a token within the same session; another owner cannot
/// be adopted by an already-started request.
#[derive(Clone)]
pub struct HttpCredentialLease {
    source: Arc<dyn HttpCredentialSource>,
    owner: HttpCredentialOwner,
}

impl HttpCredentialLease {
    pub fn owner(&self) -> &HttpCredentialOwner {
        &self.owner
    }

    pub async fn capture(source: Arc<dyn HttpCredentialSource>) -> Result<Self> {
        let credentials = source.current().await.ok_or_else(scope_changed)?;
        validate(&credentials)?;
        Ok(Self {
            source,
            owner: credentials.owner,
        })
    }

    pub async fn credentials(&self) -> Result<HttpCredentials> {
        let credentials = self.source.current().await.ok_or_else(scope_changed)?;
        validate(&credentials)?;
        if credentials.owner != self.owner {
            return Err(scope_changed());
        }
        Ok(credentials)
    }

    pub async fn ensure_current(&self) -> Result<()> {
        self.credentials().await.map(|_| ())
    }
}

fn validate(credentials: &HttpCredentials) -> Result<()> {
    if credentials.owner.tenant_id.trim().is_empty()
        || credentials.owner.user_id.trim().is_empty()
        || credentials.bearer.trim().is_empty()
    {
        return Err(scope_changed());
    }
    Ok(())
}

fn scope_changed() -> FlareError {
    FlareError::localized(
        ErrorCode::NotConnected,
        "HTTP request cancelled: credential owner changed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::RwLock;

    struct Source(RwLock<Option<HttpCredentials>>);
    #[async_trait::async_trait]
    impl HttpCredentialSource for Source {
        async fn current(&self) -> Option<HttpCredentials> {
            self.0.read().await.clone()
        }
    }
    fn credentials(user: &str, tenant: &str, generation: u64, bearer: &str) -> HttpCredentials {
        HttpCredentials {
            owner: HttpCredentialOwner {
                tenant_id: tenant.into(),
                user_id: user.into(),
                generation,
            },
            bearer: bearer.into(),
            session_id: "fixture-session".into(),
        }
    }

    #[tokio::test]
    async fn token_refresh_keeps_owner_but_account_tenant_and_session_changes_cancel() {
        for next in [
            credentials("bob", "app-a", 1, "other"),
            credentials("alice", "app-b", 1, "other"),
            credentials("alice", "app-a", 2, "other"),
        ] {
            let source = Arc::new(Source(RwLock::new(Some(credentials(
                "alice", "app-a", 1, "first",
            )))));
            let lease = HttpCredentialLease::capture(source.clone()).await.unwrap();
            *source.0.write().await = Some(credentials("alice", "app-a", 1, "refreshed"));
            assert_eq!(lease.credentials().await.unwrap().bearer, "refreshed");
            *source.0.write().await = Some(next);
            assert!(lease.ensure_current().await.is_err());
        }
    }

    #[tokio::test]
    async fn logout_invalidates_a_held_request() {
        let source = Arc::new(Source(RwLock::new(Some(credentials(
            "alice", "app-a", 1, "first",
        )))));
        let lease = HttpCredentialLease::capture(source.clone()).await.unwrap();
        *source.0.write().await = None;
        assert!(lease.credentials().await.is_err());
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn endpoint() -> (String, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (send, receive) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut byte = [0];
                if stream.read(&mut byte).await.unwrap() == 0 {
                    break;
                }
                head.push(byte[0]);
            }
            let _ = send.send(String::from_utf8(head).unwrap().to_ascii_lowercase());
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
        });
        (url, receive)
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn core_selection_discards_social_lease_and_never_falls_back() {
        use crate::infrastructure::transport::http::{HttpClient, HttpRequestContext};
        let source = Arc::new(Source(RwLock::new(Some(credentials(
            "alice",
            "app-a",
            1,
            "access-fixture",
        )))));
        let lease = HttpCredentialLease::capture(source.clone()).await.unwrap();
        let context = Arc::new(HttpRequestContext::new());
        context
            .set_auth_context("connect-fixture".into(), None)
            .await;
        context
            .set_gateway_context(
                "access-fixture".into(),
                "app-a".into(),
                "alice".into(),
                None,
            )
            .await;
        let (url, head) = endpoint().await;
        let client = HttpClient::with_context(url, context.clone())
            .with_gateway_credentials(lease)
            .with_core_bearer();
        *source.0.write().await = None;
        let _: serde_json::Value = client.get("/", None).await.unwrap();
        let head = head.await.unwrap();
        assert!(head.contains("authorization: bearer connect-fixture\r\n"));
        assert!(!head.contains("access-fixture"));
        context.set_auth_context(String::new(), None).await;
        assert!(
            !context
                .build_core_headers()
                .await
                .contains_key("Authorization")
        );
        assert!(
            context
                .build_headers()
                .await
                .get("Authorization")
                .is_some_and(|value| value == "Bearer access-fixture")
        );
        context.clear_gateway_context().await;
        context
            .set_auth_context("connect-fixture".into(), None)
            .await;
        assert!(!context.build_headers().await.contains_key("Authorization"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn anonymous_clone_drops_invalidated_source_and_every_principal_header() {
        use crate::infrastructure::transport::http::{HttpClient, HttpRequestContext};
        let source = Arc::new(Source(RwLock::new(Some(credentials(
            "alice",
            "app-a",
            1,
            "access-fixture",
        )))));
        let lease = HttpCredentialLease::capture(source.clone()).await.unwrap();
        let context = Arc::new(HttpRequestContext::new());
        context
            .set_gateway_context(
                "access-fixture".into(),
                "app-a".into(),
                "alice".into(),
                None,
            )
            .await;
        context
            .set_gateway_session_id("session-fixture".into())
            .await;
        let (url, head) = endpoint().await;
        let client = HttpClient::with_context(url, context)
            .with_gateway_credentials(lease)
            .without_request_context();
        *source.0.write().await = None;
        let _: serde_json::Value = client.get("/", None).await.unwrap();
        let head = head.await.unwrap();
        for forbidden in [
            "authorization:",
            "cookie:",
            "x-user-id:",
            "x-tenant-id:",
            "x-session-id:",
        ] {
            assert!(
                !head.contains(forbidden),
                "anonymous request retained {forbidden}"
            );
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_late_response_from_the_old_owner_is_rejected() {
        use crate::infrastructure::transport::http::HttpClient;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let source = Arc::new(Source(RwLock::new(Some(credentials(
            "alice",
            "app-a",
            1,
            "access-fixture",
        )))));
        let lease = HttpCredentialLease::capture(source.clone()).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (arrived, arrival) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            while !head.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
            }
            let _ = arrived.send(());
            let _ = released.await;
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
        });
        let client = HttpClient::new(url).with_gateway_credentials(lease);
        let pending = tokio::spawn(async move { client.get::<serde_json::Value>("/", None).await });
        arrival.await.unwrap();
        *source.0.write().await = Some(credentials("alice", "app-a", 2, "new-session-fixture"));
        let _ = release.send(());
        assert!(pending.await.unwrap().is_err());
        server.await.unwrap();
    }
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn object_capability_get_and_presigned_put_never_send_session_credentials() {
        use crate::infrastructure::transport::http::{HttpClient, HttpRequestContext};
        use std::collections::HashMap;
        let source = Arc::new(Source(RwLock::new(Some(credentials(
            "alice",
            "app-a",
            1,
            "access-fixture",
        )))));
        let lease = HttpCredentialLease::capture(source.clone()).await.unwrap();
        let context = Arc::new(HttpRequestContext::new());
        context
            .set_gateway_context(
                "access-fixture".into(),
                "app-a".into(),
                "alice".into(),
                None,
            )
            .await;
        context
            .set_gateway_session_id("session-fixture".into())
            .await;
        let client =
            HttpClient::with_context("http://127.0.0.1", context).with_gateway_credentials(lease);
        *source.0.write().await = None;
        let (url, head) = endpoint().await;
        let grant = HashMap::from([("X-Private-Grant".to_string(), "object-fixture".to_string())]);
        client
            .get_private_bytes_full_url(&url, &grant, 1024)
            .await
            .unwrap();
        let read = head.await.unwrap();
        assert!(read.contains("x-private-grant: object-fixture"));
        let (url, head) = endpoint().await;
        client
            .put_private_bytes_full_url(&url, b"fixture", "application/octet-stream", 1024)
            .await
            .unwrap();
        let write = head.await.unwrap();
        for head in [read, write] {
            for forbidden in [
                "authorization:",
                "cookie:",
                "x-user-id:",
                "x-tenant-id:",
                "x-session-id:",
            ] {
                assert!(!head.contains(forbidden));
            }
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_core_request_cannot_retry_as_the_next_prepared_session() {
        use crate::infrastructure::transport::http::HttpRequestContext;
        use crate::prelude::{
            CoreEndpoints, IMClient, LoginDbKind, NetworkChangeEvent, SdkConfig, SdkConfigOverlay,
            in_memory_im_provider,
        };
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        struct Renewal;
        #[async_trait::async_trait]
        impl crate::client::token_provider::ConnectTokenRefresher for Renewal {
            async fn refresh_connect_token(&self) -> Option<String> {
                Some("fresh-connect-fixture".into())
            }
        }
        for (next_user, overlay_tenant) in [("bob", None), ("alice", Some("702"))] {
            let expected_tenant = overlay_tenant.unwrap_or("701");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let (arrived, arrival) = tokio::sync::oneshot::channel();
            let arrived = Arc::new(std::sync::Mutex::new(Some(arrived)));
            let (release, released) = tokio::sync::oneshot::channel();
            let released = Arc::new(tokio::sync::Mutex::new(Some(released)));
            let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = observed.clone();
            let server = tokio::spawn(async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    let (arrived, released, seen) =
                        (arrived.clone(), released.clone(), seen.clone());
                    tokio::spawn(async move {
                        let mut head = Vec::new();
                        let mut byte = [0];
                        while !head.windows(4).any(|p| p == b"\r\n\r\n") {
                            if socket.read(&mut byte).await.unwrap() == 0 {
                                return;
                            }
                            head.push(byte[0]);
                        }
                        let head = String::from_utf8(head).unwrap();
                        let probe = head
                            .lines()
                            .next()
                            .is_some_and(|line| line.starts_with("POST /owner-probe "));
                        let first = if probe {
                            let mut seen = seen.lock().unwrap();
                            seen.push(head.clone());
                            seen.len() == 1
                        } else {
                            false
                        };
                        let (status, body) = if first {
                            let _ = arrived.lock().unwrap().take().unwrap().send(());
                            let result = released.lock().await.take().unwrap();
                            let _ = result.await;
                            (401, r#"{"code":401,"reason":"TOKEN_EXPIRED","message":""}"#)
                        } else {
                            (200, r#"{"code":0,"data":{}}"#)
                        };
                        let reply = format!(
                            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = socket.write_all(reply.as_bytes()).await;
                    });
                }
            });
            let context = Arc::new(HttpRequestContext::new());
            let mut config = SdkConfig::with_endpoints(CoreEndpoints {
                websocket_url: "ws://127.0.0.1:9/ws".into(),
                base_url: url,
                quic_url: None,
                media_base_url: None,
            });
            config.tenant_id = Some("701".into());
            let im = IMClient::builder()
                .environment(Some("development".into()))
                .config(config)
                .stores(in_memory_im_provider())
                .http_request_context(context.clone())
                .build()
                .unwrap();
            let data = std::env::temp_dir().join(format!(
                "flare-core-owner-{}",
                crate::spi::new_idempotency_key()
            ));
            im.init(
                None,
                Some(SdkConfigOverlay {
                    tenant_id: overlay_tenant.map(str::to_string),
                    data_url: Some(data.to_string_lossy().into_owned()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
            im.notify_network_change(NetworkChangeEvent {
                available: false,
                ..Default::default()
            })
            .await
            .unwrap();
            let _ = im
                .login(
                    "alice",
                    Some("old-connect-fixture"),
                    LoginDbKind::provided(in_memory_im_provider()),
                    |_, _| {},
                )
                .await;
            assert_eq!(im.current_user_id().await.as_deref(), Some("alice"));
            use sha2::Digest as _;
            let generation = im.session_generation_snapshot();
            let old_fingerprint: [u8; 32] = sha2::Sha256::digest(b"old-connect-fixture").into();
            for (user, app) in [("bob", expected_tenant), ("alice", "another-app")] {
                assert!(
                    im.update_access_token_for_session(
                        "wrong-owner-fixture".into(),
                        user,
                        app,
                        generation,
                        old_fingerprint
                    )
                    .await
                    .is_err()
                );
            }
            let epoch = context.core_epoch().await;
            im.update_access_token_for_session(
                "rotated-connect-fixture".into(),
                "alice",
                expected_tenant,
                generation,
                old_fingerprint,
            )
            .await
            .unwrap();
            assert_eq!(
                context.core_epoch().await,
                epoch,
                "a valid Connect refresh keeps its logical owner"
            );
            let rotated_fingerprint: [u8; 32] =
                sha2::Sha256::digest(b"rotated-connect-fixture").into();
            im.set_connect_token_refresher(Some(Arc::new(Renewal)))
                .await;
            let http = im
                .http_client(
                    crate::infrastructure::transport::http::router::CoreHttpService::CoreApi,
                )
                .await
                .unwrap();
            // Initialization ran offline to avoid waiting for a transport
            // handshake. Bring the network up to exercise the HTTP request.
            im.notify_network_change(NetworkChangeEvent {
                available: true,
                ..Default::default()
            })
            .await
            .unwrap();
            let request = tokio::spawn(async move {
                http.post::<_, serde_json::Value>("/owner-probe", &serde_json::json!({}))
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(3), arrival)
                .await
                .unwrap()
                .unwrap();
            im.notify_network_change(NetworkChangeEvent {
                available: false,
                ..Default::default()
            })
            .await
            .unwrap();
            let _ = im
                .login(
                    next_user,
                    Some("next-connect-fixture"),
                    LoginDbKind::provided(in_memory_im_provider()),
                    |_, _| {},
                )
                .await;
            assert_eq!(im.current_user_id().await.as_deref(), Some(next_user));
            assert!(
                im.update_access_token_for_session(
                    "late-connect-fixture".into(),
                    "alice",
                    expected_tenant,
                    generation,
                    rotated_fingerprint
                )
                .await
                .is_err()
            );
            assert_eq!(
                im.access_token().await.as_deref(),
                Some("next-connect-fixture")
            );
            im.set_connect_token_refresher(Some(Arc::new(Renewal)))
                .await;
            let _ = release.send(());
            let result = request.await.unwrap();
            assert!(
                result.is_err(),
                "an old write was retried as a new Core session"
            );
            assert!(
                observed.lock().unwrap()[0]
                    .to_ascii_lowercase()
                    .contains(&format!("x-tenant-id: {expected_tenant}\r\n")),
                "Core must use overlay then builder application identity"
            );
            assert_eq!(
                observed.lock().unwrap().len(),
                1,
                "new session must not send the old body"
            );
            server.abort();
            let _ = im.logout().await;
            let _ = tokio::fs::remove_dir_all(data).await;
        }
    }
    #[tokio::test]
    async fn an_already_queued_transport_answer_is_rechecked_before_json_delivery() {
        use crate::infrastructure::transport::http::{HttpClient, HttpRequestContext};
        let source = Arc::new(Source(RwLock::new(Some(credentials(
            "alice",
            "app-a",
            1,
            "access-fixture",
        )))));
        let lease = HttpCredentialLease::capture(source.clone()).await.unwrap();
        let http = HttpClient::new("http://127.0.0.1").with_gateway_credentials(lease);
        let (send, receive) = tokio::sync::oneshot::channel();
        send.send(b"{}".to_vec()).unwrap();
        *source.0.write().await = Some(credentials("alice", "app-a", 2, "new-session-fixture"));
        let bytes = receive.await.unwrap();
        assert!(
            http.decode_scoped_json::<serde_json::Value>(&bytes, None)
                .await
                .is_err()
        );
        let context = Arc::new(HttpRequestContext::new());
        context
            .set_auth_context("connect-fixture".into(), None)
            .await;
        let epoch = context.core_epoch().await;
        let http = HttpClient::with_context("http://127.0.0.1", context.clone()).with_core_bearer();
        context.set_auth_context(String::new(), None).await;
        context
            .set_auth_context("new-connect-fixture".into(), None)
            .await;
        assert!(
            http.decode_scoped_json::<serde_json::Value>(&bytes, Some(epoch))
                .await
                .is_err()
        );
    }
}
