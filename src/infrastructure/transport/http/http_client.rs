use std::collections::HashMap;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

#[cfg(target_arch = "wasm32")]
use super::http_client_wasm as wasm_http;
#[cfg(not(target_arch = "wasm32"))]
use super::http_error_from_response_status;
#[cfg(not(target_arch = "wasm32"))]
use super::ip_hints::IpHintResolver;
use crate::shared::error::{FlareError, Result};
use base64::Engine as _;
#[cfg(not(target_arch = "wasm32"))]
use flare_core::common::TlsConfig as CoreTlsConfig;
#[cfg(not(target_arch = "wasm32"))]
use flare_core::common::cert::create_client_config_with_tls;
use tokio::sync::RwLock;
use uuid::Uuid;

/// 直传上传的进度回调：参数是已经交给网络层的累计字节数。
///
/// 单次 PUT 以前只能报「开始」和「完成」两个点，大文件上传期间进度条一直停在 0。
pub type SentBytesCallback = Arc<dyn Fn(u64) + Send + Sync>;

/// 原生端流式上传的分块大小：进度粒度，也是每次交给 hyper 的最大块。
#[cfg(not(target_arch = "wasm32"))]
const UPLOAD_PROGRESS_CHUNK: usize = 64 * 1024;

/// 把内存里的字节按块交给 hyper，每取走一块就报一次「此前的块已交出」。
///
/// 报的是块的起点而不是终点：取走一块只说明它进了发送缓冲，还没写出去。
/// 全部发完由调用方在响应成功后补报总字节数。
#[cfg(not(target_arch = "wasm32"))]
fn counted_bytes_body(data: bytes::Bytes, on_sent: SentBytesCallback) -> reqwest::Body {
    let total = data.len();
    let chunks = (0..total).step_by(UPLOAD_PROGRESS_CHUNK).map(move |start| {
        on_sent(start as u64);
        let end = (start + UPLOAD_PROGRESS_CHUNK).min(total);
        Ok::<_, std::io::Error>(data.slice(start..end))
    });
    reqwest::Body::wrap_stream(futures_util::stream::iter(chunks))
}

#[cfg(not(target_arch = "wasm32"))]
fn counted_file_body(file: tokio::fs::File, on_sent: SentBytesCallback) -> reqwest::Body {
    use futures_util::StreamExt as _;
    let mut sent = 0u64;
    let stream = tokio_util::io::ReaderStream::with_capacity(file, UPLOAD_PROGRESS_CHUNK).map(
        move |chunk| {
            if let Ok(bytes) = &chunk {
                on_sent(sent);
                sent += bytes.len() as u64;
            }
            chunk
        },
    );
    reqwest::Body::wrap_stream(stream)
}

/// 从 Bearer JWT payload 提取 `sub`（不校验签名，仅用于补齐 `x-user-id` header）。
fn jwt_sub_unverified(token: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?.trim();
    if payload.is_empty() {
        return None;
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(payload))
        .ok()?;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    v.get("sub")
        .and_then(|s| s.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Renews the Core bearer after Core rejected it as expired.
///
/// The long connection authenticates once at CONNECT, so an expired bearer only
/// surfaces on Core HTTP (capabilities, plugins, media). The host owns refresh
/// single-flight and replay protection; the core asks at most once per rejected bearer.
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait CoreBearerRenewal: Send + Sync {
    /// The current Core bearer (a fresh one after a successful refresh), or `None`.
    async fn renew(&self) -> Option<String>;
}

#[derive(Default)]
pub struct HttpRequestContext {
    /// IM 长连接 / 媒体等 core HTTP 使用的 Bearer
    auth_token: RwLock<String>,
    /// Social Gateway 等业务 BFF 使用的 Bearer（与 IM token 分离，避免建连时互相覆盖）
    gateway_token: RwLock<String>,
    tenant_id: RwLock<String>,
    user_id: RwLock<String>,
    session_id: RwLock<String>,
    trace_id: RwLock<String>,
    language: RwLock<String>,
    /// Installed with the IM session; absent means an expired bearer fails as before.
    core_renewal: RwLock<Option<Arc<dyn CoreBearerRenewal>>>,
    /// Concurrent expiries renew once; later waiters reuse the bearer already renewed.
    core_renewal_gate: tokio::sync::Mutex<()>,
    /// Invalidates a pending renewal when the host replaces or clears its session.
    core_session_epoch: std::sync::atomic::AtomicU64,
}

impl HttpRequestContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn set_core_bearer_renewal(&self, renewal: Option<Arc<dyn CoreBearerRenewal>>) {
        *self.core_renewal.write().await = renewal;
        self.core_session_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// After Core rejected `rejected` as expired: the bearer to retry with, if any.
    pub(crate) async fn renew_core_bearer(&self, rejected: &str) -> Option<String> {
        let epoch = self
            .core_session_epoch
            .load(std::sync::atomic::Ordering::SeqCst);
        let renewal = self.core_renewal.read().await.clone()?;
        let _gate = self.core_renewal_gate.lock().await;
        if self
            .core_session_epoch
            .load(std::sync::atomic::Ordering::SeqCst)
            != epoch
        {
            return None;
        }
        let current = self.auth_token.read().await.clone();
        if current.trim().is_empty() {
            return None;
        }
        if !current.trim().is_empty() && current != rejected {
            return Some(current);
        }
        let fresh = renewal
            .renew()
            .await
            .filter(|token| !token.trim().is_empty() && token != rejected)?;
        let mut current = self.auth_token.write().await;
        if self
            .core_session_epoch
            .load(std::sync::atomic::Ordering::SeqCst)
            != epoch
            || current.trim().is_empty()
        {
            return None;
        }
        // The host may already have pushed a newer token while renewal awaited.
        if *current != rejected {
            return Some(current.clone());
        }
        *current = fresh.clone();
        Some(fresh)
    }

    pub async fn set_auth_context(&self, token: String, language: Option<String>) {
        if token.trim().is_empty() {
            self.core_session_epoch
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        *self.auth_token.write().await = token;
        if let Some(language) = language {
            *self.language.write().await = language;
        }
    }

    /// Gateway / BFF 会话：Bearer + `x-tenant-id` / `x-user-id` / `x-trace-id`
    pub async fn set_gateway_context(
        &self,
        token: String,
        tenant_id: String,
        user_id: String,
        language: Option<String>,
    ) {
        *self.gateway_token.write().await = token;
        *self.tenant_id.write().await = tenant_id;
        *self.user_id.write().await = user_id;
        if self.trace_id.read().await.is_empty() {
            *self.trace_id.write().await = Uuid::new_v4().to_string();
        }
        if let Some(language) = language {
            *self.language.write().await = language;
        }
    }

    pub async fn clear_gateway_context(&self) {
        *self.gateway_token.write().await = String::new();
        *self.tenant_id.write().await = String::new();
        *self.user_id.write().await = String::new();
        *self.session_id.write().await = String::new();
    }

    /// Set the refresh-token session represented by the current Gateway bearer.
    /// The Gateway forwards this as request context so device management can
    /// distinguish this session from the account's other active sessions.
    pub async fn set_gateway_session_id(&self, session_id: String) {
        *self.session_id.write().await = session_id.trim().to_string();
    }

    pub async fn gateway_token_is_empty(&self) -> bool {
        self.gateway_token.read().await.trim().is_empty()
    }

    /// 补齐或刷新 `x-user-id` / `x-tenant-id`（IM `login` 后 Background 社交同步依赖此字段）。
    pub async fn ensure_identity(&self, user_id: &str, tenant_id: &str) {
        let uid = user_id.trim();
        if !uid.is_empty() {
            *self.user_id.write().await = uid.to_string();
        }
        let tid = tenant_id.trim();
        if !tid.is_empty() {
            *self.tenant_id.write().await = crate::shared::util::normalize_tenant_id(tid);
        }
    }

    /// 从当前 Gateway/IM Bearer 解析 `sub` 并写回 `user_id`（JWT 形态：`a.b.c`）。
    pub async fn sync_user_id_from_token(&self) {
        let gateway_token = self.gateway_token.read().await.clone();
        let im_token = self.auth_token.read().await.clone();
        let token = if !gateway_token.trim().is_empty() {
            gateway_token
        } else {
            im_token
        };
        if let Some(sub) = jwt_sub_unverified(token.trim()) {
            *self.user_id.write().await = sub;
        }
    }

    pub async fn set_trace_id(&self, trace_id: String) {
        *self.trace_id.write().await = trace_id;
    }

    /// Headers for business gateway (BFF) requests: the gateway Bearer when set.
    pub async fn build_headers(&self) -> HashMap<String, String> {
        self.build_headers_preferring(false).await
    }

    /// Headers for Core HTTP (media, presence): the IM Bearer when set. The
    /// gateway token belongs to the business backend and Core does not accept it.
    pub async fn build_core_headers(&self) -> HashMap<String, String> {
        self.build_headers_preferring(true).await
    }

    async fn build_headers_preferring(&self, im_first: bool) -> HashMap<String, String> {
        let gateway_token = self.gateway_token.read().await.clone();
        let im_token = self.auth_token.read().await.clone();
        let (preferred, fallback) = if im_first {
            (im_token, gateway_token)
        } else {
            (gateway_token, im_token)
        };
        let token = if !preferred.trim().is_empty() {
            preferred
        } else {
            fallback
        };
        let mut tenant_id =
            crate::shared::util::normalize_tenant_id(self.tenant_id.read().await.trim());
        let mut user_id = self.user_id.read().await.trim().to_string();
        if user_id.is_empty()
            && !token.trim().is_empty()
            && let Some(sub) = jwt_sub_unverified(token.trim())
        {
            user_id = sub.clone();
            *self.user_id.write().await = sub;
        }
        if tenant_id.is_empty() {
            tenant_id = "0".to_string();
        }
        *self.tenant_id.write().await = tenant_id.clone();
        let trace_id = self.trace_id.read().await.clone();
        let session_id = self.session_id.read().await.clone();
        let language = self.language.read().await.clone();
        let mut headers = HashMap::new();
        if !token.trim().is_empty() {
            headers.insert(
                "Authorization".to_string(),
                format!("Bearer {}", token.trim()),
            );
        }
        if !tenant_id.is_empty() {
            headers.insert("x-tenant-id".to_string(), tenant_id);
        }
        if !user_id.is_empty() {
            headers.insert("x-user-id".to_string(), user_id);
        }
        if !session_id.is_empty() {
            headers.insert("x-session-id".to_string(), session_id);
        }
        if trace_id.is_empty() {
            headers.insert("x-trace-id".to_string(), Uuid::new_v4().to_string());
        } else {
            headers.insert("x-trace-id".to_string(), trace_id);
        }
        headers.insert("x-request-id".to_string(), Uuid::new_v4().to_string());
        if !language.trim().is_empty() {
            headers.insert("Accept-Language".to_string(), language);
        }
        headers
    }
}

/// 请求发送失败时的错误。
///
/// 连接阶段就失败（DNS 解析、拒绝连接、网络不可达、TLS 握手）时请求根本没离开本机，
/// 标成 [`ErrorCode::NetworkUnreachable`]：调用方据此可以放心重发一次性的请求——典型是
/// 刷新令牌，服务端把同一枚刷新令牌的第二次使用当成泄露，会吊销该用户全部会话，所以
/// 「可能已送达」的失败绝不能重发，而「肯定没发出去」的必须能重发，否则合盖唤醒那一下
/// 网络还没起来，会话续期就被永久卡死。连接中途断开、超时等分不清服务端是否已处理，
/// 仍按系统错误。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn send_failure(error: reqwest::Error) -> FlareError {
    let reason = format!("http request failed: {error}");
    if error.is_connect() {
        FlareError::localized(crate::shared::error::ErrorCode::NetworkUnreachable, reason)
    } else {
        FlareError::system(reason)
    }
}

#[cfg(test)]
mod tests {
    use super::HttpRequestContext;

    /// 起一个只收一次 PUT 的本地服务：返回请求头（小写）和请求体。
    #[cfg(not(target_arch = "wasm32"))]
    async fn one_shot_put_server() -> (
        String,
        tokio::task::JoinHandle<(std::collections::HashMap<String, String>, Vec<u8>)>,
    ) {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/bucket/object", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(socket);
            let mut headers = std::collections::HashMap::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let line = line.trim_end();
                if line.is_empty() {
                    break;
                }
                if let Some((key, value)) = line.split_once(':') {
                    headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
                }
            }
            let len: usize = headers
                .get("content-length")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let mut body = vec![0u8; len];
            reader.read_exact(&mut body).await.unwrap();
            reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nETag: \"abc\"\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            (headers, body)
        });
        (url, handle)
    }

    fn recorded_progress() -> (
        super::SentBytesCallback,
        std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
    ) {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = seen.clone();
        (
            std::sync::Arc::new(move |sent: u64| sink.lock().unwrap().push(sent)),
            seen,
        )
    }

    fn assert_progress_walks_to(seen: &[u64], total: u64) {
        assert!(seen.len() > 2, "应当逐块上报，实际只有 {seen:?}");
        assert!(
            seen.windows(2).all(|pair| pair[0] <= pair[1]),
            "进度不能倒退: {seen:?}"
        );
        assert_eq!(seen.last().copied(), Some(total));
        assert!(
            seen[..seen.len() - 1].iter().all(|sent| *sent < total),
            "响应成功前不能报满: {seen:?}"
        );
    }

    /// 带进度的 PUT 是流式 body：必须显式带 Content-Length（对象存储预签名 PUT
    /// 拒收 chunked），字节不能错，进度逐块递增、成功后才报满。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn put_bytes_with_progress_streams_with_content_length() {
        let (url, server) = one_shot_put_server().await;
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let (on_sent, seen) = recorded_progress();
        let mut headers = std::collections::HashMap::new();
        headers.insert("Content-Type".to_string(), "application/pdf".to_string());
        let response = super::HttpClient::new("http://unused")
            .put_bytes_full_url_with_progress(&url, &data, &headers, Some(on_sent))
            .await
            .expect("put");
        let (request_headers, body) = server.await.unwrap();
        assert_eq!(
            request_headers.get("content-length").map(String::as_str),
            Some("300000")
        );
        assert!(!request_headers.contains_key("transfer-encoding"));
        assert_eq!(
            request_headers.get("content-type").map(String::as_str),
            Some("application/pdf")
        );
        assert_eq!(body, data);
        assert_eq!(response.get("etag").map(String::as_str), Some("\"abc\""));
        assert_progress_walks_to(&seen.lock().unwrap(), 300_000);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn private_put_is_bounded_and_omits_request_identity() {
        let (url, server) = one_shot_put_server().await;
        let context = std::sync::Arc::new(HttpRequestContext::new());
        context
            .set_auth_context("synthetic-im-token".into(), None)
            .await;
        context
            .set_gateway_context(
                "synthetic-gateway-token".into(),
                "synthetic-app".into(),
                "synthetic-user".into(),
                None,
            )
            .await;
        let client = super::HttpClient::with_context("http://unused", context);
        client
            .put_private_bytes_full_url(
                &url,
                b"synthetic-private-bytes",
                "application/octet-stream",
                1024,
            )
            .await
            .unwrap();
        let (headers, body) = server.await.unwrap();
        assert_eq!(body, b"synthetic-private-bytes");
        for forbidden in ["authorization", "cookie", "x-user-id", "x-tenant-id"] {
            assert!(!headers.contains_key(forbidden));
        }
        let error = client
            .put_private_bytes_full_url(
                "http://127.0.0.1:1",
                b"too large",
                "application/octet-stream",
                1,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            Some(crate::shared::error::ErrorCode::InvalidParameter)
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn private_put_never_forwards_a_body_to_a_redirect_target() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let sink = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sink_url = format!("http://{}/must-not-receive", sink.local_addr().unwrap());
        let source = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let source_url = format!(
            "http://{}/private?synthetic_token=opaque",
            source.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let (mut socket, _) = source.accept().await.unwrap();
            let mut buffer = [0; 4096];
            let _ = socket.read(&mut buffer).await.unwrap();
            socket.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {sink_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        });
        let client = super::HttpClient::new("http://unused");
        let error = client
            .put_private_bytes_full_url(
                &source_url,
                b"synthetic-private-bytes",
                "application/octet-stream",
                1024,
            )
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("synthetic_token"));
        server.await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), sink.accept())
                .await
                .is_err()
        );
        assert_eq!(client.private_clients.lock().unwrap().len(), 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn private_get_server(
        response: Vec<u8>,
    ) -> (
        String,
        tokio::task::JoinHandle<std::collections::HashMap<String, String>>,
    ) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/private?synthetic_url_secret=opaque",
            listener.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(socket);
            let mut headers = std::collections::HashMap::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                if line.trim().is_empty() {
                    break;
                }
                if let Some((key, value)) = line.split_once(':') {
                    headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
                }
            }
            reader.get_mut().write_all(&response).await.unwrap();
            headers
        });
        (url, server)
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn private_get_sends_only_explicit_capability_and_no_ambient_identity() {
        let context = std::sync::Arc::new(HttpRequestContext::new());
        context
            .set_auth_context("synthetic-im-secret".into(), Some("en".into()))
            .await;
        context
            .set_gateway_context(
                "synthetic-gateway-secret".into(),
                "synthetic-app".into(),
                "synthetic-user".into(),
                None,
            )
            .await;
        context
            .set_gateway_session_id("synthetic-session".into())
            .await;
        let client = super::HttpClient::with_context("http://unused", context);
        let explicit = std::collections::HashMap::from([(
            "x-object-capability".into(),
            "synthetic-object-grant".into(),
        )]);
        for _ in 0..2 {
            let (url, server) = private_get_server(b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nSet-Cookie: ambient=synthetic-response-secret; Path=/\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc".to_vec()).await;
            let (bytes, content_type) = client
                .get_private_bytes_full_url(&url, &explicit, 3)
                .await
                .unwrap();
            assert_eq!(bytes, b"abc");
            assert_eq!(content_type, "image/png");
            let headers = server.await.unwrap();
            assert_eq!(headers["x-object-capability"], "synthetic-object-grant");
            assert_eq!(headers["cache-control"], "no-store");
            for forbidden in [
                "authorization",
                "cookie",
                "referer",
                "origin",
                "accept-language",
                "x-user-id",
                "x-tenant-id",
                "x-session-id",
                "x-trace-id",
                "x-request-id",
            ] {
                assert!(
                    !headers.contains_key(forbidden),
                    "unexpected ambient header {forbidden}"
                );
            }
        }
        let denied = std::collections::HashMap::from([(
            "Authorization".into(),
            "Bearer synthetic-explicit-secret".into(),
        )]);
        let error = client
            .get_private_bytes_full_url(
                "http://127.0.0.1:1/private?synthetic_url_secret=opaque",
                &denied,
                3,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            Some(crate::shared::error::ErrorCode::InvalidParameter)
        );
        assert!(!format!("{error:?}").contains("synthetic"));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn private_get_does_not_follow_redirects_or_expose_error_bodies() {
        let sink = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect = format!(
            "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{}/sink?synthetic_redirect_secret=opaque\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            sink.local_addr().unwrap()
        );
        let (url, server) = private_get_server(redirect.into_bytes()).await;
        let client = super::HttpClient::new("http://unused");
        let explicit = std::collections::HashMap::from([(
            "x-object-capability".into(),
            "synthetic-header-secret".into(),
        )]);
        let error = client
            .get_private_bytes_full_url(&url, &explicit, 1024)
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            Some(crate::shared::error::ErrorCode::ServiceUnavailable)
        );
        assert!(!format!("{error:?}").contains("synthetic"));
        server.await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), sink.accept())
                .await
                .is_err()
        );
        let body = "synthetic-response-secret";
        let (url, server) = private_get_server(format!("HTTP/1.1 403 Forbidden\r\nX-Private: synthetic-header-secret\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).into_bytes()).await;
        let error = client
            .get_private_bytes_full_url(&url, &explicit, 1)
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            Some(crate::shared::error::ErrorCode::PermissionDenied)
        );
        assert!(!format!("{error:?}").contains("synthetic"));
        server.await.unwrap();
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn private_get_rejects_declared_and_streamed_oversize_bodies() {
        let client = super::HttpClient::new("http://unused");
        for response in [
            b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: 9999\r\nConnection: close\r\n\r\n".to_vec(),
            b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n".to_vec(),
        ] {
            let (url, server) = private_get_server(response).await;
            let error = client.get_private_bytes_full_url(&url, &std::collections::HashMap::new(), 4).await.unwrap_err();
            assert_eq!(error.code(), Some(crate::shared::error::ErrorCode::ResourceExhausted));
            assert!(!format!("{error:?}").contains("synthetic"));
            server.await.unwrap();
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn private_get_timeout_includes_the_body_and_has_no_url_diagnostic() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let source = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/private?synthetic_url_secret=opaque",
            source.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let (socket, _) = source.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            loop {
                let mut line = String::new();
                assert!(socket.read_line(&mut line).await.unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
            }
            socket
                .get_mut()
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: 8\r\n\r\na",
                )
                .await
                .unwrap();
            futures::future::pending::<()>().await;
        });
        let client = super::HttpClient::new("http://unused");
        client.private_clients.lock().unwrap().insert(
            Vec::new(),
            Ok(reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(std::time::Duration::from_millis(100))
                .build()
                .unwrap()),
        );
        let error = client
            .get_private_bytes_full_url(&url, &std::collections::HashMap::new(), 8)
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            Some(crate::shared::error::ErrorCode::OperationTimeout)
        );
        assert!(!format!("{error:?}").contains("synthetic"));
        server.abort();
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn put_file_with_progress_streams_the_whole_file() {
        let (url, server) = one_shot_put_server().await;
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 13) as u8).collect();
        let path =
            std::env::temp_dir().join(format!("flare-put-progress-{}.bin", std::process::id()));
        std::fs::write(&path, &data).unwrap();
        let (on_sent, seen) = recorded_progress();
        super::HttpClient::new("http://unused")
            .put_file_full_url_with_progress(
                &url,
                &path,
                data.len() as u64,
                &std::collections::HashMap::new(),
                Some(on_sent),
            )
            .await
            .expect("put");
        let (request_headers, body) = server.await.unwrap();
        std::fs::remove_file(&path).ok();
        assert_eq!(
            request_headers.get("content-length").map(String::as_str),
            Some("200000")
        );
        assert_eq!(body, data);
        assert_progress_walks_to(&seen.lock().unwrap(), 200_000);
    }

    #[tokio::test]
    async fn clear_gateway_context_falls_back_to_im_auth_without_stale_identity() {
        let context = HttpRequestContext::new();
        context.set_auth_context("im-token".to_string(), None).await;
        context
            .set_gateway_context(
                "old-gateway-token".to_string(),
                "tenant-a".to_string(),
                "alice".to_string(),
                None,
            )
            .await;
        context
            .set_gateway_session_id("session-a".to_string())
            .await;

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
        assert_eq!(
            headers.get("x-session-id").map(String::as_str),
            Some("session-a")
        );

        // Core HTTP never receives the business gateway's token.
        let core = context.build_core_headers().await;
        assert_eq!(
            core.get("Authorization").map(String::as_str),
            Some("Bearer im-token")
        );
        assert_eq!(
            core.get("x-tenant-id").map(String::as_str),
            Some("tenant-a")
        );

        context.clear_gateway_context().await;

        let headers = context.build_headers().await;
        assert_eq!(
            headers.get("Authorization").map(String::as_str),
            Some("Bearer im-token")
        );
        assert_eq!(headers.get("x-tenant-id").map(String::as_str), Some("0"));
        assert_eq!(headers.get("x-user-id"), None);
        assert_eq!(headers.get("x-session-id"), None);
    }

    /// Serves one request, answers `{}` and returns its Authorization header.
    #[cfg(not(target_arch = "wasm32"))]
    async fn authorization_of_one_request() -> (String, tokio::task::JoinHandle<Option<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut buf = [0u8; 1024];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                head.extend_from_slice(&buf[..n]);
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}")
                .await
                .unwrap();
            String::from_utf8_lossy(&head).lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("authorization")
                    .then(|| value.trim().to_string())
            })
        });
        (url, server)
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn core_clients_send_the_im_token_beside_a_gateway_session() {
        let context = std::sync::Arc::new(HttpRequestContext::new());
        context.set_auth_context("im-token".into(), None).await;
        context
            .set_gateway_context(
                "gateway-token".into(),
                "tenant-a".into(),
                "alice".into(),
                None,
            )
            .await;
        for (core, expected) in [(true, "Bearer im-token"), (false, "Bearer gateway-token")] {
            let (url, server) = authorization_of_one_request().await;
            let mut client = super::HttpClient::with_context(url, std::sync::Arc::clone(&context));
            if core {
                client = client.with_core_bearer();
            }
            let _: serde_json::Value = client.get("/x", None).await.expect("request");
            assert_eq!(server.await.unwrap().as_deref(), Some(expected));
        }
    }

    /// Answers each request in order with the given status/body and records its Authorization.
    #[cfg(not(target_arch = "wasm32"))]
    async fn scripted_server(
        replies: Vec<(u16, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut seen = Vec::new();
            for (status, body) in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = socket.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    head.extend_from_slice(&buf[..n]);
                }
                seen.push(
                    String::from_utf8_lossy(&head)
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("authorization")
                                .then(|| value.trim().to_string())
                        })
                        .unwrap_or_default(),
                );
                let reply = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(reply.as_bytes()).await.unwrap();
            }
            seen
        });
        (url, server)
    }

    struct CountingRenewal(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl super::CoreBearerRenewal for CountingRenewal {
        async fn renew(&self) -> Option<String> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Some("fresh-token".into())
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn an_expired_core_bearer_is_renewed_once_and_the_request_retried() {
        let context = std::sync::Arc::new(HttpRequestContext::new());
        context.set_auth_context("expired-token".into(), None).await;
        let renewal = std::sync::Arc::new(CountingRenewal(Default::default()));
        context
            .set_core_bearer_renewal(Some(
                renewal.clone() as std::sync::Arc<dyn super::CoreBearerRenewal>
            ))
            .await;
        let (url, server) = scripted_server(vec![
            (401, r#"{"code":401,"reason":"TOKEN_EXPIRED","message":""}"#),
            (200, "{}"),
        ])
        .await;
        let client = super::HttpClient::with_context(url, std::sync::Arc::clone(&context))
            .with_core_bearer();
        let _: serde_json::Value = client
            .post("/x", &serde_json::json!({}))
            .await
            .expect("retried");
        assert_eq!(
            server.await.unwrap(),
            vec![
                "Bearer expired-token".to_string(),
                "Bearer fresh-token".to_string()
            ]
        );
        assert_eq!(renewal.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        // The renewed bearer stays for later Core requests.
        assert_eq!(
            context
                .build_core_headers()
                .await
                .get("Authorization")
                .map(String::as_str),
            Some("Bearer fresh-token")
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn other_rejections_and_gateway_clients_are_not_renewed() {
        let context = std::sync::Arc::new(HttpRequestContext::new());
        context.set_auth_context("im-token".into(), None).await;
        let renewal = std::sync::Arc::new(CountingRenewal(Default::default()));
        context
            .set_core_bearer_renewal(Some(
                renewal.clone() as std::sync::Arc<dyn super::CoreBearerRenewal>
            ))
            .await;
        // A revoked (not expired) bearer is final.
        let (url, server) = scripted_server(vec![(401, r#"{"error":"TOKEN_REVOKED"}"#)]).await;
        let client = super::HttpClient::with_context(url, std::sync::Arc::clone(&context))
            .with_core_bearer();
        assert!(
            client
                .post::<_, serde_json::Value>("/x", &serde_json::json!({}))
                .await
                .is_err()
        );
        assert_eq!(server.await.unwrap().len(), 1);
        // Business gateway clients carry their own session and never renew the Core bearer.
        let (url, server) = scripted_server(vec![(
            401,
            r#"{"code":401,"reason":"TOKEN_EXPIRED","message":""}"#,
        )])
        .await;
        let client = super::HttpClient::with_context(url, std::sync::Arc::clone(&context));
        assert!(
            client
                .post::<_, serde_json::Value>("/x", &serde_json::json!({}))
                .await
                .is_err()
        );
        assert_eq!(server.await.unwrap().len(), 1);
        assert_eq!(renewal.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrent_core_expiries_share_one_renewal() {
        let context = std::sync::Arc::new(HttpRequestContext::new());
        context.set_auth_context("expired-token".into(), None).await;
        let renewal = std::sync::Arc::new(CountingRenewal(Default::default()));
        context.set_core_bearer_renewal(Some(renewal.clone())).await;
        let calls = (0..16).map(|_| context.renew_core_bearer("expired-token"));
        assert!(
            futures::future::join_all(calls)
                .await
                .iter()
                .all(|token| token.as_deref() == Some("fresh-token"))
        );
        assert_eq!(renewal.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    struct HeldRenewal {
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl super::CoreBearerRenewal for HeldRenewal {
        async fn renew(&self) -> Option<String> {
            self.started.notify_one();
            self.release.notified().await;
            Some("late-old-session-token".into())
        }
    }

    #[tokio::test]
    async fn a_renewal_finishing_after_logout_cannot_revive_the_bearer() {
        let context = std::sync::Arc::new(HttpRequestContext::new());
        context.set_auth_context("expired-token".into(), None).await;
        let renewal = std::sync::Arc::new(HeldRenewal {
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        context.set_core_bearer_renewal(Some(renewal.clone())).await;
        let c = context.clone();
        let task = tokio::spawn(async move { c.renew_core_bearer("expired-token").await });
        renewal.started.notified().await;
        context.set_auth_context(String::new(), None).await;
        context
            .set_auth_context("new-login-token".into(), None)
            .await;
        renewal.release.notify_one();
        assert_eq!(task.await.unwrap(), None);
        assert_eq!(
            context.build_core_headers().await["Authorization"],
            "Bearer new-login-token"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_second_expiry_is_final_and_message_text_cannot_trigger_refresh() {
        let context = std::sync::Arc::new(HttpRequestContext::new());
        context.set_auth_context("expired-token".into(), None).await;
        let renewal = std::sync::Arc::new(CountingRenewal(Default::default()));
        context.set_core_bearer_renewal(Some(renewal.clone())).await;
        let expired = r#"{"code":401,"reason":"TOKEN_EXPIRED","message":""}"#;
        let (url, server) = scripted_server(vec![(401, expired), (401, expired)]).await;
        let client = super::HttpClient::with_context(url, context.clone()).with_core_bearer();
        assert!(client.get::<serde_json::Value>("/x", None).await.is_err());
        assert_eq!(server.await.unwrap().len(), 2);
        assert_eq!(renewal.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        let (url, server) = scripted_server(vec![(
            401,
            r#"{"code":401,"reason":"TOKEN_REVOKED","message":"TOKEN_EXPIRED"}"#,
        )])
        .await;
        let client = super::HttpClient::with_context(url, context).with_core_bearer();
        assert!(client.get::<serde_json::Value>("/x", None).await.is_err());
        assert_eq!(server.await.unwrap().len(), 1);
        assert_eq!(renewal.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn a_private_ca_client_loads_the_ca_and_reports_an_unreadable_one() {
        use flare_core::common::TlsConfig;
        let cert = rcgen::generate_simple_self_signed(vec!["im.example.test".to_string()]).unwrap();
        let path = std::env::temp_dir().join(format!("flare-http-ca-{}.pem", uuid::Uuid::new_v4()));
        std::fs::write(&path, cert.cert.pem()).unwrap();
        let trusted = super::HttpClient::with_tls(
            "https://im.example.test",
            TlsConfig::none().with_ca_cert(path.clone()),
        );
        std::fs::remove_file(&path).unwrap();
        assert!(trusted.tls_config_error.is_none());
        assert!(trusted.context.is_none());

        let missing = super::HttpClient::with_tls(
            "https://im.example.test",
            TlsConfig::none().with_ca_cert(path),
        );
        assert!(missing.tls_config_error.is_some());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_request_that_never_left_is_marked_network_unreachable() {
        // 1 号端口没人监听：连接被拒，请求没发出去。
        let client = super::HttpClient::new("http://127.0.0.1:1");
        let error = client
            .post::<_, serde_json::Value>("/api/v1/auth/refresh", &serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            Some(crate::shared::error::ErrorCode::NetworkUnreachable),
            "{error}"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_request_cut_off_after_connecting_is_not_claimed_unsent() {
        // 连上后对方一字不回就断开：请求可能已被处理，不能说它没发出去。
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                drop(socket);
            }
        });
        let client = super::HttpClient::new(format!("http://{addr}"));
        let error = client
            .post::<_, serde_json::Value>("/api/v1/auth/refresh", &serde_json::json!({}))
            .await
            .unwrap_err();
        assert_ne!(
            error.code(),
            Some(crate::shared::error::ErrorCode::NetworkUnreachable),
            "{error}"
        );
    }

    /// 跑一条发送路径，对着只回 TLS 告警的服务端：DoH 要被问到一次。主机名直接指到本机，解析这一步
    /// 不会要求查询，看到的只有发送失败之后的要求。
    #[cfg(not(target_arch = "wasm32"))]
    async fn asks_doh_after_a_tls_failure<F, T>(
        target: impl FnOnce(u16) -> String,
        send: impl FnOnce(super::HttpClient, String) -> F,
    ) where
        F: std::future::Future<Output = crate::shared::error::Result<T>>,
    {
        use super::super::doh::tests::{scripted_doh, settle, tls_alert_server};
        use super::super::router::HttpRouter;
        use crate::runtime::link::{AccessRuntime, AccessStoreSource};

        let (port, server) = tls_alert_server().await;
        let (cache, lookup) = scripted_doh(vec!["198.51.100.20".parse().unwrap()]);
        let config = flare_access::AccessConfig::development(flare_access::DevelopmentEndpoints {
            ws_url: Some("ws://127.0.0.1:1/ws".into()),
            social_api_url: Some(format!("https://{DOH_TEST_HOST}:{port}")),
            ..Default::default()
        });
        let access =
            AccessRuntime::new(Some(config), AccessStoreSource::memory()).with_doh_cache(cache);
        let direct = reqwest::Client::builder()
            .no_proxy()
            .resolve(DOH_TEST_HOST, ([127, 0, 0, 1], port).into())
            .build()
            .unwrap();
        let mut http = super::HttpClient::new("http://unused");
        http.router = Some(HttpRouter::new(
            std::sync::Arc::new(access),
            flare_access::ServiceClass::SocialApi,
        ));
        http.client = direct.clone();
        http.private_clients
            .lock()
            .unwrap()
            .insert(Vec::new(), Ok(direct));

        let error = send(http, target(port))
            .await
            .err()
            .expect("the server only answers with an alert");
        settle().await;
        assert_eq!(lookup.calls(), 1, "{error:?}");
        server.abort();
    }

    #[cfg(not(target_arch = "wasm32"))]
    const DOH_TEST_HOST: &str = "ap-1.flare.invalid";

    #[cfg(not(target_arch = "wasm32"))]
    fn object_url(port: u16) -> String {
        format!("https://{DOH_TEST_HOST}:{port}/object")
    }

    /// 主请求循环之外的发送路径同样报：媒体上传下载、私有传输都在这里。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn every_full_url_send_path_reports_a_tls_failure_to_doh() {
        use std::collections::HashMap;

        let no_headers = HashMap::new;
        asks_doh_after_a_tls_failure(object_url, |http, url| async move {
            http.get_bytes_direct_url(&url).await
        })
        .await;
        asks_doh_after_a_tls_failure(object_url, |http, url| async move {
            http.get_response_direct_url(&url).await
        })
        .await;
        asks_doh_after_a_tls_failure(object_url, |http, url| async move {
            http.put_bytes_full_url_with_progress(&url, b"data", &no_headers(), None)
                .await
        })
        .await;
        asks_doh_after_a_tls_failure(object_url, |http, url| async move {
            let file = std::env::temp_dir().join(format!("flare-doh-{}", uuid::Uuid::new_v4()));
            tokio::fs::write(&file, b"data").await.unwrap();
            let sent = http
                .put_file_full_url_with_progress(&url, &file, 4, &no_headers(), None)
                .await;
            let _ = tokio::fs::remove_file(&file).await;
            sent
        })
        .await;
        asks_doh_after_a_tls_failure(object_url, |http, url| async move {
            http.put_private_bytes_full_url(&url, b"data", "text/plain", 8)
                .await
        })
        .await;
        asks_doh_after_a_tls_failure(object_url, |http, url| async move {
            http.get_private_bytes_full_url(&url, &no_headers(), 8)
                .await
        })
        .await;
    }

    /// 走选路的两条：主请求循环和 `send_bytes`。主请求循环失败后换接入点重试一次，目录里只有这一个。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn the_routed_send_paths_report_a_tls_failure_to_doh() {
        let path = |_port: u16| "/api/v1/ping".to_owned();
        asks_doh_after_a_tls_failure(path, |http, path| async move {
            http.send_bytes(&path, b"data").await
        })
        .await;
        asks_doh_after_a_tls_failure(path, |http, path| async move {
            http.get::<serde_json::Value>(&path, None).await
        })
        .await;
    }

    /// 接入点钉扎各自一个客户端（克隆间共享缓存）；钉扎无效时报配置错误，不退回只做 CA 校验的客户端。
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn access_point_pins_get_their_own_client_and_never_fall_back() {
        use super::HttpClient;
        use flare_core::common::TlsConfig;

        let http = HttpClient::with_tls("https://ap.example", TlsConfig::none());
        http.client_for(&[]).expect("no pins: the CA-only client");
        assert!(http.pinned_clients.lock().unwrap().is_empty());

        let pin = format!("spki-sha256/{}", "ab".repeat(32));
        http.client_for(std::slice::from_ref(&pin))
            .expect("a valid pin");
        http.clone()
            .client_for(std::slice::from_ref(&pin))
            .expect("the same pin from a clone");
        assert_eq!(http.pinned_clients.lock().unwrap().len(), 1);

        let error = http
            .client_for(&["not-a-pin".to_string()])
            .expect_err("an invalid pin must not fall back to an unpinned client");
        assert_eq!(
            error.code(),
            Some(crate::shared::error::ErrorCode::ConfigurationError)
        );
    }
}

#[derive(Clone)]
pub struct HttpClient {
    base_url: String,
    context: Option<Arc<HttpRequestContext>>,
    /// 按服务路由：每次请求向选路器取接入点（REST 跟随 IM 当前接入点）并按 §3.9 重试。
    /// 没有时 `base_url` 是固定入口（显式端点：令牌签发、测试替身）。
    router: Option<super::router::HttpRouter>,
    /// Core HTTP client: send the IM Bearer rather than the business gateway's.
    core_bearer: bool,
    #[cfg(not(target_arch = "wasm32"))]
    client: reqwest::Client,
    #[cfg(not(target_arch = "wasm32"))]
    tls_config_error: Option<String>,
    /// 信任根（系统根 + 配置的私有 CA）。接入点的 SPKI 钉扎在它上面按请求叠加。
    #[cfg(not(target_arch = "wasm32"))]
    tls: CoreTlsConfig,
    /// 路由客户端的 DNS 解析器（DNS 失败或结果可疑时按接入点的 `ipHints` 直连）。固定入口没有接入点，不用它。
    #[cfg(not(target_arch = "wasm32"))]
    ip_hint_resolver: Option<Arc<IpHintResolver>>,
    /// 按钉扎集合缓存的客户端（克隆共享）：同一接入点的请求复用连接池。
    #[cfg(not(target_arch = "wasm32"))]
    pinned_clients: Arc<std::sync::Mutex<HashMap<Vec<String>, PinnedClient>>>,
    /// Pools for private raw transfers. These fail closed when the strict
    /// no-redirect client cannot be built; ordinary API clients are unchanged.
    #[cfg(not(target_arch = "wasm32"))]
    private_clients: Arc<std::sync::Mutex<HashMap<Vec<String>, PinnedClient>>>,
}

/// 某组钉扎对应的客户端；钉扎本身无效时记下原因，绝不退回不校验钉扎的客户端。
#[cfg(not(target_arch = "wasm32"))]
type PinnedClient = std::result::Result<reqwest::Client, String>;

/// 缓存的钉扎客户端上限：钉扎集合与接入点一一对应，正常远小于它；超出（清单频繁轮换）就整体重建。
#[cfg(not(target_arch = "wasm32"))]
const MAX_PINNED_CLIENTS: usize = 32;

impl HttpClient {
    #[cfg(not(target_arch = "wasm32"))]
    fn build_reqwest_client(
        tls: Option<&CoreTlsConfig>,
        ip_hint_resolver: Option<&Arc<IpHintResolver>>,
    ) -> (reqwest::Client, Option<String>) {
        let (builder, tls_config_error) = Self::reqwest_builder(tls, ip_hint_resolver);
        let client = match builder.build() {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!("build reqwest client with timeout failed: {err}");
                reqwest::Client::new()
            }
        };
        (client, tls_config_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn reqwest_builder(
        tls: Option<&CoreTlsConfig>,
        ip_hint_resolver: Option<&Arc<IpHintResolver>>,
    ) -> (reqwest::ClientBuilder, Option<String>) {
        // 时限与 wasm 端共用一份（`super::HTTP_REQUEST_TIMEOUT`），两端行为不再各自漂移。
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            // Auth and revoke-only capabilities are endpoint-bound. Core APIs
            // must return an explicit final response, never forward a write body.
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(super::HTTP_CONNECT_TIMEOUT)
            .timeout(super::HTTP_REQUEST_TIMEOUT);
        if let Some(resolver) = ip_hint_resolver {
            builder = builder.dns_resolver(resolver.clone());
        }
        let mut tls_config_error = None;
        if let Some(tls) = tls
            && tls.requires_custom_client_tls()
        {
            match create_client_config_with_tls(tls) {
                Ok(config) => builder = builder.use_preconfigured_tls(config),
                Err(err) => {
                    let message = format!("build reqwest pinned TLS config failed: {err}");
                    tracing::warn!("{message}");
                    tls_config_error = Some(message);
                }
            }
        }
        (builder, tls_config_error)
    }

    pub fn new(base_url: impl Into<String>) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let (client, tls_config_error) = Self::build_reqwest_client(None, None);
        Self {
            base_url: base_url.into(),
            context: None,
            router: None,
            core_bearer: false,
            #[cfg(not(target_arch = "wasm32"))]
            client,
            #[cfg(not(target_arch = "wasm32"))]
            tls_config_error,
            #[cfg(not(target_arch = "wasm32"))]
            tls: CoreTlsConfig::none(),
            #[cfg(not(target_arch = "wasm32"))]
            ip_hint_resolver: None,
            #[cfg(not(target_arch = "wasm32"))]
            pinned_clients: Arc::default(),
            #[cfg(not(target_arch = "wasm32"))]
            private_clients: Arc::default(),
        }
    }

    pub fn with_context(base_url: impl Into<String>, context: Arc<HttpRequestContext>) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let (client, tls_config_error) = Self::build_reqwest_client(None, None);
        Self {
            base_url: base_url.into(),
            context: Some(context),
            router: None,
            core_bearer: false,
            #[cfg(not(target_arch = "wasm32"))]
            client,
            #[cfg(not(target_arch = "wasm32"))]
            tls_config_error,
            #[cfg(not(target_arch = "wasm32"))]
            tls: CoreTlsConfig::none(),
            #[cfg(not(target_arch = "wasm32"))]
            ip_hint_resolver: None,
            #[cfg(not(target_arch = "wasm32"))]
            pinned_clients: Arc::default(),
            #[cfg(not(target_arch = "wasm32"))]
            private_clients: Arc::default(),
        }
    }

    /// A client without request context (no bearer token or refresh), such as
    /// login and token refresh, that verifies the server with `tls`.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_tls(base_url: impl Into<String>, tls: CoreTlsConfig) -> Self {
        let (client, tls_config_error) = Self::build_reqwest_client(Some(&tls), None);
        Self {
            base_url: base_url.into(),
            context: None,
            router: None,
            core_bearer: false,
            client,
            tls_config_error,
            tls,
            ip_hint_resolver: None,
            pinned_clients: Arc::default(),
            private_clients: Arc::default(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_context_and_tls(
        base_url: impl Into<String>,
        context: Arc<HttpRequestContext>,
        tls: CoreTlsConfig,
    ) -> Self {
        let (client, tls_config_error) = Self::build_reqwest_client(Some(&tls), None);
        Self {
            base_url: base_url.into(),
            context: Some(context),
            router: None,
            core_bearer: false,
            client,
            tls_config_error,
            tls,
            ip_hint_resolver: None,
            pinned_clients: Arc::default(),
            private_clients: Arc::default(),
        }
    }

    /// 按服务路由的客户端（引擎内各 API 与社交 SDK 都用它）：每次请求按服务类型取接入点。
    /// `tls` 是信任根（不含钉扎）；选中接入点的 SPKI 钉扎按请求叠加。主机名 DNS 失败或结果可疑时按
    /// 接入点的 `ipHints` 直连，TLS 仍按主机名。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn routed(
        router: super::router::HttpRouter,
        context: Arc<HttpRequestContext>,
        tls: CoreTlsConfig,
    ) -> Self {
        let resolver = Arc::new(router.ip_hint_resolver());
        let (client, tls_config_error) = Self::build_reqwest_client(Some(&tls), Some(&resolver));
        Self {
            base_url: String::new(),
            context: Some(context),
            router: Some(router),
            core_bearer: false,
            client,
            tls_config_error,
            tls,
            ip_hint_resolver: Some(resolver),
            pinned_clients: Arc::default(),
            private_clients: Arc::default(),
        }
    }

    /// 按服务路由的客户端（浏览器）。TLS 与证书校验由浏览器负责，接入点的钉扎在这里不生效。
    #[cfg(target_arch = "wasm32")]
    pub fn routed(router: super::router::HttpRouter, context: Arc<HttpRequestContext>) -> Self {
        let mut client = Self::with_context(String::new(), context);
        client.router = Some(router);
        client
    }

    /// 同一接入运行时、另一类服务（例如媒体）的路由客户端；固定入口的客户端原样复制。
    pub fn for_service(&self, service: flare_access::ServiceClass) -> Self {
        let mut client = self.clone();
        client.router = self
            .router
            .as_ref()
            .map(|router| router.for_service(service));
        client
    }

    pub fn is_routed(&self) -> bool {
        self.router.is_some()
    }

    /// 稳定的部署标识：固定入口为 base URL；路由客户端不随接入点切换变化。
    pub fn endpoint_identity(&self) -> String {
        match &self.router {
            Some(router) => router.deployment_identity(),
            None => self.base_url.clone(),
        }
    }

    /// 带幂等键的 POST：重试（换接入点、结果未知后再发）都复用同一个键，服务端按键返回首次结果。
    /// 键由调用方为「一次用户操作」生成一次（见 [`super::router::new_idempotency_key`]）。
    pub async fn post_idempotent<B, T>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: &str,
    ) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let headers = HashMap::from([(
            super::router::IDEMPOTENCY_KEY_HEADER.to_owned(),
            idempotency_key.to_owned(),
        )]);
        self.post_with_headers(path, body, &headers).await
    }

    /// Authenticate with the IM token: for Core HTTP endpoints sharing a request
    /// context with a business gateway client.
    pub fn with_core_bearer(mut self) -> Self {
        self.core_bearer = true;
        self
    }

    /// A lifecycle operation must never borrow the next account's mutable bearer.
    pub(crate) async fn pinned_core_auth(&self, token: String, user: &str, app: &str) -> Self {
        let mut client = self.clone();
        let context = Arc::new(HttpRequestContext::new());
        context.set_auth_context(token, None).await;
        context.ensure_identity(user, app).await;
        client.context = Some(context);
        client.core_bearer = true;
        client
    }
    pub(crate) fn anonymous_clone(&self) -> Self {
        let mut client = self.clone();
        client.context = None;
        client
    }

    /// 同一路由、不带请求上下文（不发鉴权与身份头）的客户端：登录前的认证接口用。
    pub fn without_request_context(&self) -> Self {
        self.anonymous_clone()
    }

    /// 服务内路径在当前入口上的完整 URL：路由客户端按选路取接入点，固定入口直接拼接。
    /// 服务端返回的相对地址（如静态图）要交给宿主直接加载时用。
    pub async fn url_for(&self, path: &str) -> Result<String> {
        Ok(self
            .resolve(&RequestTarget::path(path), &[], None)
            .await?
            .url)
    }

    async fn headers_from(&self, context: &HttpRequestContext) -> HashMap<String, String> {
        if self.core_bearer {
            context.build_core_headers().await
        } else {
            context.build_headers().await
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// 一次发送失败了：是 TLS 失败或建连超时时让 DoH 查一次主机。固定入口没有接入点，不查。
    #[cfg(not(target_arch = "wasm32"))]
    fn note_send_failure(&self, error: &reqwest::Error) {
        if let Some(router) = &self.router {
            router.note_send_failure(error);
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn ensure_tls_ready(&self) -> Result<()> {
        if let Some(error) = &self.tls_config_error {
            return Err(FlareError::localized(
                crate::shared::error::ErrorCode::ConfigurationError,
                error.clone(),
            ));
        }
        Ok(())
    }

    /// 带这组 SPKI 钉扎的客户端；没有钉扎时就是只做 CA 校验的基础客户端。钉扎无效时返回错误，
    /// 绝不退回不校验钉扎的客户端。
    #[cfg(not(target_arch = "wasm32"))]
    fn client_for(&self, spki_pins: &[String]) -> Result<reqwest::Client> {
        if spki_pins.is_empty() {
            return Ok(self.client.clone());
        }
        let mut key = spki_pins.to_vec();
        key.sort();
        let mut cache = self
            .pinned_clients
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !cache.contains_key(&key) {
            if cache.len() >= MAX_PINNED_CLIENTS {
                cache.clear();
            }
            let tls = self.tls.clone().with_spki_sha256_pins(key.clone());
            let built = match Self::build_reqwest_client(Some(&tls), self.ip_hint_resolver.as_ref())
            {
                (_, Some(error)) => Err(error),
                (client, None) => Ok(client),
            };
            cache.insert(key.clone(), built);
        }
        match &cache[&key] {
            Ok(client) => Ok(client.clone()),
            Err(error) => Err(FlareError::localized(
                crate::shared::error::ErrorCode::ConfigurationError,
                error.clone(),
            )),
        }
    }

    /// 完整 URL（预签名直链等）的客户端：主机属于某个接入点时带上它的钉扎。
    #[cfg(not(target_arch = "wasm32"))]
    fn client_for_url(&self, url: &str) -> Result<reqwest::Client> {
        match &self.router {
            Some(router) => self.client_for(&router.spki_pins_for_url(url)),
            None => Ok(self.client.clone()),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn private_client_for_url(&self, url: &str) -> Result<reqwest::Client> {
        self.ensure_tls_ready()?;
        let mut pins = self
            .router
            .as_ref()
            .map(|router| router.spki_pins_for_url(url))
            .unwrap_or_default();
        pins.sort();
        let mut cache = self
            .private_clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !cache.contains_key(&pins) && cache.len() >= MAX_PINNED_CLIENTS {
            cache.clear();
        }
        cache
            .entry(pins)
            .or_insert_with_key(|pins| {
                let tls = self.tls.clone().with_spki_sha256_pins(pins.clone());
                let (builder, tls_error) =
                    Self::reqwest_builder(Some(&tls), self.ip_hint_resolver.as_ref());
                if tls_error.is_some() {
                    Err("private HTTP TLS configuration unavailable".to_owned())
                } else {
                    builder
                        .redirect(reqwest::redirect::Policy::none())
                        .referer(false)
                        .build()
                        .map_err(|_| "private HTTP client unavailable".to_owned())
                }
            })
            .clone()
            .map_err(|_| {
                FlareError::localized(
                    crate::shared::error::ErrorCode::ConfigurationError,
                    "private HTTP transport unavailable",
                )
            })
    }

    /// Bounded raw PUT for object-bound private transfer URLs. Never forwards
    /// request identity, cookies or a redirect, and never logs the signed URL
    /// or response body. Uses the normal TLS roots/pins and request timeout.
    pub async fn put_private_bytes_full_url(
        &self,
        url: &str,
        data: &[u8],
        content_type: &str,
        max_bytes: usize,
    ) -> Result<()> {
        // 带 userinfo 的地址在解析时就被拒绝。
        let target = flare_access::ParsedUrl::parse(url).map_err(|_| {
            FlareError::localized(
                crate::shared::error::ErrorCode::InvalidParameter,
                "invalid private upload URL",
            )
        })?;
        if !matches!(target.scheme(), "http" | "https")
            || target.fragment().is_some()
            || max_bytes == 0
            || data.len() > max_bytes
            || content_type.is_empty()
            || content_type.len() > 256
            || content_type.chars().any(char::is_control)
        {
            return Err(FlareError::localized(
                crate::shared::error::ErrorCode::InvalidParameter,
                "invalid private upload input",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let response = self
                .private_client_for_url(url)?
                .put(url)
                .header(reqwest::header::CONTENT_TYPE, content_type)
                .body(data.to_vec())
                .send()
                .await
                .map_err(|error| {
                    self.note_send_failure(&error);
                    FlareError::localized(
                        if error.is_timeout() {
                            crate::shared::error::ErrorCode::OperationTimeout
                        } else {
                            crate::shared::error::ErrorCode::ServiceUnavailable
                        },
                        "private upload transport failed",
                    )
                })?;
            if !response.status().is_success() {
                return Err(FlareError::localized(
                    crate::shared::error::ErrorCode::ServiceUnavailable,
                    "private upload rejected",
                ));
            }
            Ok(())
        }
        #[cfg(target_arch = "wasm32")]
        {
            wasm_http::put_private_bytes(url.to_owned(), data.to_vec(), content_type.to_owned())
                .await
        }
    }

    /// An object-capability read with only caller-supplied, non-identity headers.
    /// Never merges the current session, follows a redirect, caches private bytes
    /// or includes a URL, credential, response header or body in diagnostics.
    /// `max_bytes` bounds the streamed body (up to 64 MiB); the whole transfer uses
    /// the normal request deadline. Only a final HTTP403 is PermissionDenied.
    pub async fn get_private_bytes_full_url(
        &self,
        url: &str,
        explicit_headers: &HashMap<String, String>,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, String)> {
        use super::private_download as private;
        private::validate(url, explicit_headers, max_bytes)?;
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut request = self
                .private_client_for_url(url)
                .map_err(|_| private::configuration())?
                .get(url)
                .header(reqwest::header::CACHE_CONTROL, "no-store")
                .header(reqwest::header::PRAGMA, "no-cache");
            for (key, value) in explicit_headers {
                request = request.header(key, value);
            }
            let mut response = request.send().await.map_err(|error| {
                self.note_send_failure(&error);
                private::transport(error.is_timeout())
            })?;
            if !response.status().is_success() {
                return Err(private::rejected(response.status().as_u16()));
            }
            if response
                .content_length()
                .is_some_and(|size| size > max_bytes as u64)
            {
                return Err(private::too_large());
            }
            let content_type = private::content_type(
                response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(private::response)?,
            )?;
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| private::transport(error.is_timeout()))?
            {
                private::append(&mut bytes, &chunk, max_bytes)?;
            }
            Ok((bytes, content_type))
        }
        #[cfg(target_arch = "wasm32")]
        {
            wasm_http::get_private_bytes(url.to_owned(), explicit_headers.clone(), max_bytes).await
        }
    }

    /// 发 Social Gateway 请求前补齐身份（先写显式 user/tenant，再从 JWT 回填）。
    pub async fn ensure_request_identity(&self, user_id: &str, tenant_id: &str) {
        if let Some(ctx) = &self.context {
            ctx.ensure_identity(user_id, tenant_id).await;
            ctx.sync_user_id_from_token().await;
        }
    }

    async fn context_headers(&self) -> HashMap<String, String> {
        if let Some(ctx) = &self.context {
            self.headers_from(ctx).await
        } else {
            HashMap::new()
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn get<T>(&self, path: &str, query: Option<&HashMap<String, String>>) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request(
            reqwest::Method::GET,
            RequestTarget::path(path),
            query,
            None::<&()>,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn get_with_headers<T>(
        &self,
        path: &str,
        query: Option<&HashMap<String, String>>,
        headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request_with_headers(
            reqwest::Method::GET,
            RequestTarget::path(path),
            query,
            None::<&()>,
            headers,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn get_full_url<T>(
        &self,
        url: &str,
        query: Option<&HashMap<String, String>>,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request(
            reqwest::Method::GET,
            RequestTarget::url(url),
            query,
            None::<&()>,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn post<B, T>(&self, path: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.request(
            reqwest::Method::POST,
            RequestTarget::path(path),
            None,
            Some(body),
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn post_with_headers<B, T>(
        &self,
        path: &str,
        body: &B,
        headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.request_with_headers(
            reqwest::Method::POST,
            RequestTarget::path(path),
            None,
            Some(body),
            headers,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn post_full_url<B, T>(&self, url: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.request(
            reqwest::Method::POST,
            RequestTarget::url(url),
            None,
            Some(body),
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn put<B, T>(&self, path: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.request(
            reqwest::Method::PUT,
            RequestTarget::path(path),
            None,
            Some(body),
        )
        .await
    }

    /// PUT 带额外请求头（如 `Idempotency-Key`）。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn put_with_headers<B, T>(
        &self,
        path: &str,
        body: &B,
        headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.request_with_headers(
            reqwest::Method::PUT,
            RequestTarget::path(path),
            None,
            Some(body),
            headers,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn put_full_url<B, T>(&self, url: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.request(
            reqwest::Method::PUT,
            RequestTarget::url(url),
            None,
            Some(body),
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn put_bytes_full_url(
        &self,
        url: &str,
        data: &[u8],
        headers: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>> {
        self.put_bytes_full_url_with_progress(url, data, headers, None)
            .await
    }

    /// 同 [`Self::put_bytes_full_url`]，上传过程中按块回报已发送字节数。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn put_bytes_full_url_with_progress(
        &self,
        url: &str,
        data: &[u8],
        headers: &HashMap<String, String>,
        on_sent: Option<SentBytesCallback>,
    ) -> Result<HashMap<String, String>> {
        self.ensure_tls_ready()?;
        let client = self.client_for_url(url)?;
        let total = data.len() as u64;
        let mut req = match on_sent.clone() {
            // 流式 body 长度未知，不显式给 Content-Length 就会变成 chunked，
            // 对象存储的预签名 PUT 不接受 chunked。
            Some(on_sent) => client
                .put(url.to_string())
                .body(counted_bytes_body(
                    bytes::Bytes::copy_from_slice(data),
                    on_sent,
                ))
                .header(reqwest::header::CONTENT_LENGTH, total.to_string()),
            None => client.put(url.to_string()).body(data.to_vec()),
        };
        for (key, value) in headers {
            if on_sent.is_some()
                && key.eq_ignore_ascii_case(reqwest::header::CONTENT_LENGTH.as_str())
            {
                continue;
            }
            req = req.header(key, value);
        }
        let resp = req.send().await.map_err(|e| {
            self.note_send_failure(&e);
            FlareError::system(format!("http put bytes failed: {e}"))
        })?;
        let status = resp.status();
        if !status.is_success() {
            return Err(FlareError::general_error(format!(
                "http put bytes status not success: {status}"
            )));
        }
        if let Some(on_sent) = on_sent.as_ref() {
            on_sent(total);
        }
        let mut out = HashMap::new();
        for (key, value) in resp.headers() {
            if let Ok(text) = value.to_str() {
                out.insert(key.as_str().to_string(), text.to_string());
            }
        }
        Ok(out)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn put_file_full_url(
        &self,
        url: &str,
        path: &std::path::Path,
        content_len: u64,
        headers: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>> {
        self.put_file_full_url_with_progress(url, path, content_len, headers, None)
            .await
    }

    /// 同 [`Self::put_file_full_url`]，边读文件边回报已发送字节数。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn put_file_full_url_with_progress(
        &self,
        url: &str,
        path: &std::path::Path,
        content_len: u64,
        headers: &HashMap<String, String>,
        on_sent: Option<SentBytesCallback>,
    ) -> Result<HashMap<String, String>> {
        self.ensure_tls_ready()?;
        let client = self.client_for_url(url)?;
        let file = tokio::fs::File::open(path)
            .await
            .map_err(|e| FlareError::general_error(format!("open upload file failed: {e}")))?;
        let mut req = client.put(url.to_string()).body(match on_sent.clone() {
            Some(on_sent) => counted_file_body(file, on_sent),
            None => reqwest::Body::from(file),
        });
        for (key, value) in headers {
            if !key.eq_ignore_ascii_case(reqwest::header::CONTENT_LENGTH.as_str()) {
                req = req.header(key, value);
            }
        }
        req = req.header(reqwest::header::CONTENT_LENGTH, content_len.to_string());
        let resp = req.send().await.map_err(|e| {
            self.note_send_failure(&e);
            FlareError::system(format!("http put file failed: {e}"))
        })?;
        let status = resp.status();
        if !status.is_success() {
            return Err(FlareError::general_error(format!(
                "http put file status not success: {status}"
            )));
        }
        if let Some(on_sent) = on_sent.as_ref() {
            on_sent(content_len);
        }
        let mut out = HashMap::new();
        for (key, value) in resp.headers() {
            if let Ok(text) = value.to_str() {
                out.insert(key.as_str().to_string(), text.to_string());
            }
        }
        Ok(out)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn delete<T>(&self, path: &str, query: Option<&HashMap<String, String>>) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request(
            reqwest::Method::DELETE,
            RequestTarget::path(path),
            query,
            None::<&()>,
        )
        .await
    }

    /// DELETE 带额外请求头（如 `Idempotency-Key`）。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn delete_with_headers<T>(
        &self,
        path: &str,
        query: Option<&HashMap<String, String>>,
        headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request_with_headers(
            reqwest::Method::DELETE,
            RequestTarget::path(path),
            query,
            None::<&()>,
            headers,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn delete_full_url<T>(
        &self,
        url: &str,
        query: Option<&HashMap<String, String>>,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request(
            reqwest::Method::DELETE,
            RequestTarget::url(url),
            query,
            None::<&()>,
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn delete_with_body<B, T>(&self, path: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.request(
            reqwest::Method::DELETE,
            RequestTarget::path(path),
            None,
            Some(body),
        )
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn delete_with_body_headers<B, T>(
        &self,
        path: &str,
        body: &B,
        headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.request_with_headers(
            reqwest::Method::DELETE,
            RequestTarget::path(path),
            None,
            Some(body),
            headers,
        )
        .await
    }

    /// GET 任意 http(s) URL，返回原始字节（不附加鉴权头，适用于 S3/RustFS 等预签名直链）。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn get_bytes_direct_url(&self, url: &str) -> Result<Vec<u8>> {
        let resp = self
            .client_for_url(url)?
            .get(url)
            .send()
            .await
            .map_err(|e| {
                self.note_send_failure(&e);
                FlareError::system(format!("http get bytes failed: {e}"))
            })?;
        let status = resp.status();
        if !status.is_success() {
            return Err(FlareError::general_error(format!(
                "http get bytes status not success: {status}"
            )));
        }
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| FlareError::system(format!("http read body failed: {e}")))
    }

    /// GET 任意 http(s) URL，返回成功响应体流（不附加鉴权头，适用于预签名直链）；调用方负责消费 `bytes_stream`。
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn get_response_direct_url(&self, url: &str) -> Result<reqwest::Response> {
        let resp = self
            .client_for_url(url)?
            .get(url)
            .send()
            .await
            .map_err(|e| {
                self.note_send_failure(&e);
                FlareError::system(format!("http get stream failed: {e}"))
            })?;
        let status = resp.status();
        if !status.is_success() {
            return Err(FlareError::general_error(format!(
                "http get stream status not success: {status}"
            )));
        }
        Ok(resp)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn send_bytes(&self, path: &str, data: &[u8]) -> Result<Vec<u8>> {
        let resolved = self.resolve(&RequestTarget::path(path), &[], None).await?;
        let resp = self
            .client_for(&resolved.spki_pins)?
            .post(resolved.url)
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| {
                self.note_send_failure(&e);
                FlareError::system(format!("http send_bytes failed: {e}"))
            })?;
        let status = resp.status();
        if !status.is_success() {
            return Err(FlareError::general_error(format!(
                "http send_bytes status={status}"
            )));
        }
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| FlareError::system(format!("http read bytes failed: {e}")))
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn send_bytes(&self, path: &str, data: &[u8]) -> Result<Vec<u8>> {
        let headers = self.context_headers().await;
        let url = self
            .resolve(&RequestTarget::path(path), &[], None)
            .await?
            .url;
        wasm_http::fetch_bytes(
            "POST",
            url,
            None,
            Some(data.to_vec()),
            Some("application/octet-stream"),
            headers,
            None,
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn get<T>(&self, path: &str, query: Option<&HashMap<String, String>>) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request_wasm_json("GET", RequestTarget::path(path), query, None, None)
            .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn get_with_headers<T>(
        &self,
        path: &str,
        query: Option<&HashMap<String, String>>,
        extra_headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request_wasm_json(
            "GET",
            RequestTarget::path(path),
            query,
            None,
            Some(extra_headers),
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn get_full_url<T>(
        &self,
        url: &str,
        query: Option<&HashMap<String, String>>,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        wasm_http::fetch_json("GET", url.to_string(), query, None, HashMap::new(), None).await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn post<B, T>(&self, path: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let body_json = serde_json::to_string(body)
            .map_err(|e| FlareError::system(format!("encode http json body failed: {e}")))?;
        self.request_wasm_json(
            "POST",
            RequestTarget::path(path),
            None,
            Some(&body_json),
            None,
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn post_with_headers<B, T>(
        &self,
        path: &str,
        body: &B,
        extra_headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let body_json = serde_json::to_string(body)
            .map_err(|e| FlareError::system(format!("encode http json body failed: {e}")))?;
        self.request_wasm_json(
            "POST",
            RequestTarget::path(path),
            None,
            Some(&body_json),
            Some(extra_headers),
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn post_full_url<B, T>(&self, url: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let body_json = serde_json::to_string(body)
            .map_err(|e| FlareError::system(format!("encode http json body failed: {e}")))?;
        wasm_http::fetch_json(
            "POST",
            url.to_string(),
            None,
            Some(&body_json),
            HashMap::new(),
            None,
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn put<B, T>(&self, path: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let body_json = serde_json::to_string(body)
            .map_err(|e| FlareError::system(format!("encode http json body failed: {e}")))?;
        self.request_wasm_json(
            "PUT",
            RequestTarget::path(path),
            None,
            Some(&body_json),
            None,
        )
        .await
    }

    /// PUT 带额外请求头（如 `Idempotency-Key`）。
    #[cfg(target_arch = "wasm32")]
    pub async fn put_with_headers<B, T>(
        &self,
        path: &str,
        body: &B,
        extra_headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let body_json = serde_json::to_string(body)
            .map_err(|e| FlareError::system(format!("encode http json body failed: {e}")))?;
        self.request_wasm_json(
            "PUT",
            RequestTarget::path(path),
            None,
            Some(&body_json),
            Some(extra_headers),
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn put_full_url<B, T>(&self, url: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let body_json = serde_json::to_string(body)
            .map_err(|e| FlareError::system(format!("encode http json body failed: {e}")))?;
        wasm_http::fetch_json(
            "PUT",
            url.to_string(),
            None,
            Some(&body_json),
            HashMap::new(),
            None,
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn put_bytes_full_url(
        &self,
        url: &str,
        data: &[u8],
        extra_headers: &HashMap<String, String>,
    ) -> Result<HashMap<String, String>> {
        self.put_bytes_full_url_with_progress(url, data, extra_headers, None)
            .await
    }

    /// 同 [`Self::put_bytes_full_url`]；带回调时走 XHR（`fetch` 拿不到上传进度）。
    #[cfg(target_arch = "wasm32")]
    pub async fn put_bytes_full_url_with_progress(
        &self,
        url: &str,
        data: &[u8],
        extra_headers: &HashMap<String, String>,
        on_sent: Option<SentBytesCallback>,
    ) -> Result<HashMap<String, String>> {
        if let Some(on_sent) = on_sent {
            return wasm_http::put_bytes_with_progress(
                url.to_string(),
                data.to_vec(),
                extra_headers.clone(),
                on_sent,
            )
            .await;
        }
        let (_, headers) = wasm_http::fetch_bytes_with_headers(
            "PUT",
            url.to_string(),
            None,
            Some(data.to_vec()),
            None,
            HashMap::new(),
            Some(extra_headers),
        )
        .await?;
        Ok(headers)
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn delete<T>(&self, path: &str, query: Option<&HashMap<String, String>>) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request_wasm_json("DELETE", RequestTarget::path(path), query, None, None)
            .await
    }

    /// DELETE 带额外请求头（如 `Idempotency-Key`）。
    #[cfg(target_arch = "wasm32")]
    pub async fn delete_with_headers<T>(
        &self,
        path: &str,
        query: Option<&HashMap<String, String>>,
        extra_headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        self.request_wasm_json(
            "DELETE",
            RequestTarget::path(path),
            query,
            None,
            Some(extra_headers),
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn delete_full_url<T>(
        &self,
        url: &str,
        query: Option<&HashMap<String, String>>,
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        wasm_http::fetch_json("DELETE", url.to_string(), query, None, HashMap::new(), None).await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn delete_with_body<B, T>(&self, path: &str, body: &B) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let body_json = serde_json::to_string(body)
            .map_err(|e| FlareError::system(format!("encode http json body failed: {e}")))?;
        self.request_wasm_json(
            "DELETE",
            RequestTarget::path(path),
            None,
            Some(&body_json),
            None,
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn delete_with_body_headers<B, T>(
        &self,
        path: &str,
        body: &B,
        extra_headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        let body_json = serde_json::to_string(body)
            .map_err(|e| FlareError::system(format!("encode http json body failed: {e}")))?;
        self.request_wasm_json(
            "DELETE",
            RequestTarget::path(path),
            None,
            Some(&body_json),
            Some(extra_headers),
        )
        .await
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn get_bytes_direct_url(&self, url: &str) -> Result<Vec<u8>> {
        wasm_http::fetch_bytes(
            "GET",
            url.to_string(),
            None,
            None,
            None,
            HashMap::new(),
            None,
        )
        .await
    }

    /// 浏览器端的请求循环：与原生同一张重试表（§3.9）。fetch 区分不了「没发出去」与「发出去后
    /// 断了」，只有浏览器报告离线时才确定没发出去；其余网络错误按结果未知处理。
    #[cfg(target_arch = "wasm32")]
    async fn request_wasm_json<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        target: RequestTarget,
        query: Option<&HashMap<String, String>>,
        body: Option<&str>,
        extra_headers: Option<&HashMap<String, String>>,
    ) -> Result<T> {
        let client = self.clone();
        let method = method.to_owned();
        let query = query.cloned();
        let body = body.map(|value| value.as_bytes().to_vec());
        let extra = extra_headers.cloned().unwrap_or_default();
        let (tx, rx) = futures::channel::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = client
                .request_wasm_loop(&method, &target, query.as_ref(), body, &extra)
                .await;
            let _ = tx.send(result);
        });
        let bytes = rx
            .await
            .map_err(|_| FlareError::system("http renewal task dropped"))??;
        serde_json::from_slice(&bytes)
            .map_err(|error| FlareError::system(format!("parse http json failed: {error}")))
    }

    #[cfg(target_arch = "wasm32")]
    async fn request_wasm_loop(
        &self,
        method: &str,
        target: &RequestTarget,
        query: Option<&HashMap<String, String>>,
        body: Option<Vec<u8>>,
        extra: &HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        use super::router::{
            HttpFailure, HttpRetry, classify_status, has_idempotency_key, method_safety,
            network_uncertain, retry_decision,
        };
        let safety = method_safety(method, has_idempotency_key(extra));
        let content_type = body.as_ref().map(|_| "application/json");
        let mut excluded: Vec<String> = Vec::new();
        let mut pinned: Option<String> = None;
        let mut retried = false;
        let mut renewed = false;
        loop {
            let resolved = self.resolve(target, &excluded, pinned.as_deref()).await?;
            let headers = self.context_headers().await;
            let sent = self.core_bearer_in_headers(&headers, extra);
            let mut request_headers = extra.clone();
            if let Some(ap) = resolved.access_point.as_deref() {
                request_headers
                    .insert(super::router::ACCESS_POINT_HEADER.to_owned(), ap.to_owned());
            }
            let started = crate::shared::util::now_millis();
            let outcome = wasm_http::fetch_bytes(
                method,
                resolved.url.clone(),
                query,
                body.clone(),
                content_type,
                headers,
                Some(&request_headers),
            )
            .await;
            let error = match outcome {
                Ok(bytes) => {
                    resolved.record_success(started);
                    return Ok(bytes);
                }
                Err(error) => error,
            };
            if !renewed && self.renew_after_core_expiry(&error, sent.as_deref()).await {
                renewed = true;
                continue;
            }
            let failure = match error.as_localized().and_then(|error| error.params) {
                // 有 HTTP 状态：服务端给了响应。
                Some(params) if params.contains_key("httpStatus") => {
                    let status = params
                        .get("httpStatus")
                        .and_then(|status| status.parse::<u16>().ok())
                        .unwrap_or(0);
                    resolved.observe_status(status);
                    classify_status(
                        status,
                        params.get("retryAfter").map(String::as_str),
                        params.get("machineReason").map(String::as_str),
                    )
                }
                _ if error.code() == Some(crate::shared::error::ErrorCode::NetworkUnreachable) => {
                    Some(HttpFailure::ConnectPhase)
                }
                _ => Some(HttpFailure::Uncertain),
            };
            let Some(failure) = failure else {
                resolved.record_response();
                return Err(error);
            };
            let error = match failure {
                HttpFailure::Overloaded { retry_after_ms } => {
                    super::router::overloaded_error(error, retry_after_ms)
                }
                _ => error,
            };
            resolved.record_failure(failure);
            if retried {
                return Err(error);
            }
            retried = true;
            match retry_decision(safety, failure) {
                HttpRetry::OtherAccessPoint => {
                    if let Some(ap) = resolved.access_point {
                        excluded.push(ap.to_string());
                    }
                }
                HttpRetry::SameAccessPointAfter { delay_ms } => {
                    pinned = resolved.access_point.map(|ap| ap.to_string());
                    crate::shared::util::delay(std::time::Duration::from_millis(delay_ms)).await;
                }
                HttpRetry::AfterBackoff { delay_ms } => {
                    crate::shared::util::delay(std::time::Duration::from_millis(delay_ms)).await;
                }
                HttpRetry::Uncertain => return Err(network_uncertain(method, &resolved.url)),
                HttpRetry::GiveUp => return Err(error),
            }
        }
    }

    /// Only an exact typed expiry reason from a Core request permits one retry.
    async fn renew_after_core_expiry(&self, error: &FlareError, sent: Option<&str>) -> bool {
        let Some(sent) = sent else {
            return false;
        };
        let expired = error
            .as_localized()
            .and_then(|error| error.params)
            .is_some_and(|params| {
                params.get("httpStatus").map(String::as_str) == Some("401")
                    && params.get("machineReason").map(String::as_str) == Some("TOKEN_EXPIRED")
            });
        if !expired {
            return false;
        }
        match &self.context {
            Some(context) => context.renew_core_bearer(sent).await.is_some(),
            None => false,
        }
    }

    fn core_bearer_in_headers(
        &self,
        headers: &HashMap<String, String>,
        extra: &HashMap<String, String>,
    ) -> Option<String> {
        if !self.core_bearer
            || extra
                .keys()
                .any(|key| key.eq_ignore_ascii_case("authorization"))
        {
            return None;
        }
        headers
            .get("Authorization")?
            .strip_prefix("Bearer ")
            .filter(|token| !token.trim().is_empty())
            .map(str::to_owned)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn request<B, T>(
        &self,
        method: reqwest::Method,
        target: RequestTarget,
        query: Option<&HashMap<String, String>>,
        body: Option<&B>,
    ) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        self.request_with_headers(method, target, query, body, &HashMap::new())
            .await
    }

    /// 请求循环：按服务取接入点、发请求、按 §3.9 决定是否重试（每个请求最多一次；401 续期另算一次），
    /// 结果回写健康簿。
    #[cfg(not(target_arch = "wasm32"))]
    async fn request_with_headers<B, T>(
        &self,
        method: reqwest::Method,
        target: RequestTarget,
        query: Option<&HashMap<String, String>>,
        body: Option<&B>,
        headers: &HashMap<String, String>,
    ) -> Result<T>
    where
        B: serde::Serialize + ?Sized,
        T: serde::de::DeserializeOwned,
    {
        use super::router::{
            HttpFailure, HttpRetry, classify_status, has_idempotency_key, method_safety,
            network_uncertain, retry_decision,
        };
        self.ensure_tls_ready()?;
        let safety = method_safety(method.as_str(), has_idempotency_key(headers));
        let mut excluded: Vec<String> = Vec::new();
        let mut pinned: Option<String> = None;
        let mut retried = false;
        let mut renewed = false;
        loop {
            let resolved = self.resolve(&target, &excluded, pinned.as_deref()).await?;
            let context_headers = self.context_headers().await;
            let sent = self.core_bearer_in_headers(&context_headers, headers);
            let started = crate::shared::util::now_millis();
            let sent_result = self
                .send_native(
                    method.clone(),
                    &resolved,
                    query,
                    body,
                    &context_headers,
                    headers,
                )
                .await;
            let (failure, error) = match sent_result {
                // 接入点的钉扎本身无效：这个接入点建不起受信连接，按 TLS 失败换下一个。
                Err(error) => (HttpFailure::Tls, error),
                Ok(Ok(resp)) if resp.status().is_success() => {
                    resolved.record_success(started);
                    return resp
                        .json()
                        .await
                        .map_err(|e| FlareError::system(format!("parse http json failed: {e}")));
                }
                Ok(Ok(resp)) => {
                    let status = resp.status().as_u16();
                    resolved.observe_status(status);
                    let retry_after = resp
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let text = resp.text().await.unwrap_or_default();
                    let error = http_error_from_response_status(status, &text);
                    if !renewed && self.renew_after_core_expiry(&error, sent.as_deref()).await {
                        renewed = true;
                        continue;
                    }
                    let machine_reason = error
                        .as_localized()
                        .and_then(|error| error.params)
                        .and_then(|params| params.get("machineReason").cloned());
                    match classify_status(status, retry_after.as_deref(), machine_reason.as_deref())
                    {
                        Some(failure @ HttpFailure::Overloaded { retry_after_ms }) => (
                            failure,
                            super::router::overloaded_error(error, retry_after_ms),
                        ),
                        Some(failure) => (failure, error),
                        None => {
                            resolved.record_response();
                            return Err(error);
                        }
                    }
                }
                Ok(Err(error)) => {
                    self.note_send_failure(&error);
                    // 证书或钉扎不符同样发生在连接阶段，单独认出来，健康簿记作 TLS 失败。
                    let failure = if super::router::is_tls_failure(&error) {
                        HttpFailure::Tls
                    } else if error.is_connect() {
                        HttpFailure::ConnectPhase
                    } else {
                        HttpFailure::Uncertain
                    };
                    (failure, send_failure(error))
                }
            };
            resolved.record_failure(failure);
            if retried {
                return Err(error);
            }
            retried = true;
            match retry_decision(safety, failure) {
                HttpRetry::OtherAccessPoint => {
                    if let Some(ap) = resolved.access_point {
                        excluded.push(ap.to_string());
                    }
                }
                HttpRetry::SameAccessPointAfter { delay_ms } => {
                    pinned = resolved.access_point.map(|ap| ap.to_string());
                    crate::shared::util::delay(Duration::from_millis(delay_ms)).await;
                }
                HttpRetry::AfterBackoff { delay_ms } => {
                    crate::shared::util::delay(Duration::from_millis(delay_ms)).await;
                }
                HttpRetry::Uncertain => {
                    return Err(network_uncertain(method.as_str(), &resolved.url));
                }
                HttpRetry::GiveUp => return Err(error),
            }
        }
    }

    /// 外层错误：这个接入点的钉扎无效，没有发请求；内层是 reqwest 的发送结果。
    #[cfg(not(target_arch = "wasm32"))]
    async fn send_native<B>(
        &self,
        method: reqwest::Method,
        resolved: &ResolvedRequest,
        query: Option<&HashMap<String, String>>,
        body: Option<&B>,
        context_headers: &HashMap<String, String>,
        headers: &HashMap<String, String>,
    ) -> Result<std::result::Result<reqwest::Response, reqwest::Error>>
    where
        B: serde::Serialize + ?Sized,
    {
        let client = self.client_for(&resolved.spki_pins)?;
        let mut req = client.request(method, &resolved.url);
        for (key, value) in context_headers {
            req = req.header(key, value);
        }
        if let Some(q) = query {
            req = req.query(q);
        }
        for (key, value) in headers {
            req = req.header(key, value);
        }
        if let Some(ap) = resolved.access_point.as_deref() {
            req = req.header(super::router::ACCESS_POINT_HEADER, ap);
        }
        if let Some(b) = body {
            req = req.json(b);
        }
        Ok(req.send().await)
    }

    /// 本次请求的完整 URL：固定入口直接拼；路由客户端向选路器取接入点。
    async fn resolve(
        &self,
        target: &RequestTarget,
        excluded: &[String],
        pinned: Option<&str>,
    ) -> Result<ResolvedRequest> {
        match target {
            RequestTarget::Url(url) => Ok(ResolvedRequest {
                url: url.clone(),
                access_point: None,
                router: None,
                #[cfg(not(target_arch = "wasm32"))]
                spki_pins: self
                    .router
                    .as_ref()
                    .map(|router| router.spki_pins_for_url(url))
                    .unwrap_or_default(),
                #[cfg(target_arch = "wasm32")]
                spki_pins: Vec::new(),
            }),
            RequestTarget::Path(path) => match &self.router {
                None => Ok(ResolvedRequest {
                    url: join_url(&self.base_url, path),
                    access_point: None,
                    router: None,
                    spki_pins: Vec::new(),
                }),
                Some(router) => {
                    let route = router.route(excluded, pinned).await?;
                    Ok(ResolvedRequest {
                        url: join_url(&route.base, path),
                        access_point: Some(route.access_point_id),
                        router: Some(router.clone()),
                        spki_pins: route.spki_pins,
                    })
                }
            },
        }
    }
}

/// 请求目标：服务内路径（按服务路由）或完整 URL（预签名直链等，不路由）。
#[derive(Debug, Clone)]
enum RequestTarget {
    Path(String),
    Url(String),
}

impl RequestTarget {
    fn path(path: &str) -> Self {
        if path.starts_with("http://") || path.starts_with("https://") {
            return Self::Url(path.to_owned());
        }
        Self::Path(path.to_owned())
    }

    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    fn url(url: &str) -> Self {
        Self::Url(url.to_owned())
    }
}

/// 解析好的一次请求：URL、所经接入点（回写健康簿用）与该接入点的 SPKI 钉扎。
struct ResolvedRequest {
    url: String,
    access_point: Option<flare_access::ApId>,
    router: Option<super::router::HttpRouter>,
    /// 浏览器里 TLS 由浏览器负责，钉扎只在原生端生效。
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    spki_pins: Vec<String>,
}

impl ResolvedRequest {
    fn record_success(&self, started_ms: u64) {
        if let (Some(router), Some(ap)) = (&self.router, &self.access_point) {
            router.record_success(
                ap,
                crate::shared::util::now_millis().saturating_sub(started_ms),
            );
        }
    }

    fn record_failure(&self, failure: super::router::HttpFailure) {
        if let (Some(router), Some(ap)) = (&self.router, &self.access_point) {
            router.record_failure(ap, failure);
        }
    }

    fn record_response(&self) {
        if let (Some(router), Some(ap)) = (&self.router, &self.access_point) {
            router.record_response(ap);
        }
    }

    /// 服务端给了响应：版本被门禁拦下时告诉更新中心。
    fn observe_status(&self, status: u16) {
        if status == super::router::UPGRADE_REQUIRED_STATUS
            && let Some(router) = &self.router
        {
            router.upgrade_required();
        }
    }
}

fn join_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}
