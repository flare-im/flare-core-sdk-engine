//! Web/WASM presence facade — gateway REST + event-backed cache.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use super::presence_wire::BatchGetUserPresenceHttpRequest;
use crate::client::api::session_guard::SessionGuard;
use crate::infrastructure::transport::http::{HttpApiResponse, HttpClient, unwrap_api_response};
use crate::kernel::event::{EventBus, MessageEvent, SdkEvent};
use crate::shared::error::{ErrorCode, FlareError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionQualityDto {
    pub rtt_ms: i64,
    pub packet_loss_rate: f64,
    pub last_measured_at: i64,
    pub network_type: String,
    pub signal_strength: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DevicePresenceDto {
    pub device_id: String,
    pub platform: String,
    pub model: String,
    pub os_version: String,
    pub last_active_time_ms: i64,
    pub priority: i32,
    pub token_version: i64,
    pub connection_quality: Option<ConnectionQualityDto>,
    pub conversation_id: String,
    pub gateway_id: String,
    pub server_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserPresenceDto {
    pub user_id: String,
    pub is_online: bool,
    pub status: String,
    pub last_seen_ms: i64,
    pub devices: Vec<DevicePresenceDto>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchGetUserPresenceHttpResponse {
    presences: HashMap<String, UserPresenceDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LogoutPresenceHttpRequest {
    conversation_id: String,
}

/// `POST /api/v1/presence/logout` 的 `data`：网关回 `{"success": bool}`（Online 登出的结果），
/// 不是空体。按 `()` 解码会在每次登出时报「error decoding response body」。
#[derive(Debug, Deserialize)]
struct LogoutPresenceHttpResponse {
    success: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListUserDevicesHttpResponse {
    devices: Vec<DevicePresenceDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct KickDeviceHttpRequest {
    reason: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KickDeviceHttpResponse {
    success: bool,
}

#[derive(Clone)]
pub struct PresenceApi {
    session_guard: SessionGuard,
    http: HttpClient,
    current_user_id: Arc<RwLock<String>>,
    device_id: String,
    cache: Arc<RwLock<HashMap<String, UserPresenceDto>>>,
    cache_owner_user_id: Arc<RwLock<Option<String>>>,
    subscribed_user_ids: Arc<Mutex<HashSet<String>>>,
}

impl PresenceApi {
    pub(crate) fn for_session(mut self, watch: crate::client::im_client::SessionEndWatch) -> Self {
        self.session_guard.bind(watch);
        self
    }

    pub(crate) fn push_http(&self) -> HttpClient {
        self.http.clone()
    }
    pub(crate) fn push_device_id(&self) -> String {
        self.device_id.clone()
    }
    /// `http` is the Core API client (IM bearer, the deployment's TLS trust).
    pub fn new(
        http: HttpClient,
        current_user_id: Arc<RwLock<String>>,
        device_id: impl Into<String>,
        default_tenant_id: impl Into<String>,
        bus: EventBus,
    ) -> Self {
        let http = http.with_core_bearer();
        let cache = Arc::new(RwLock::new(HashMap::new()));
        let cache_owner_user_id = Arc::new(RwLock::new(None));
        let cache_for_task = cache.clone();
        let cache_owner_for_task = cache_owner_user_id.clone();
        let current_user_id_for_task = current_user_id.clone();
        let mut rx = bus.subscribe();
        crate::shared::util::background::spawn_background(async move {
            loop {
                let ev = match rx.recv().await {
                    Ok(event) => event,
                    Err(_) => break,
                };
                if let SdkEvent::Message(MessageEvent::PresenceChanged {
                    conversation_id,
                    event,
                }) = ev
                {
                    let owner_user_id = current_user_id_for_task.read().await.trim().to_string();
                    if owner_user_id.is_empty() {
                        continue;
                    }
                    {
                        let mut owner = cache_owner_for_task.write().await;
                        if owner.as_deref() != Some(owner_user_id.as_str()) {
                            cache_for_task.write().await.clear();
                            *owner = Some(owner_user_id);
                        }
                    }
                    let user_id = event.user_id.clone();
                    let status = event.status.clone();
                    let is_online = !status.eq_ignore_ascii_case("offline")
                        && !status.eq_ignore_ascii_case("invisible");
                    let dto = UserPresenceDto {
                        user_id: user_id.clone(),
                        is_online,
                        status,
                        last_seen_ms: chrono::Utc::now().timestamp_millis(),
                        devices: vec![DevicePresenceDto {
                            device_id: String::new(),
                            platform: String::new(),
                            model: String::new(),
                            os_version: String::new(),
                            last_active_time_ms: chrono::Utc::now().timestamp_millis(),
                            priority: 0,
                            token_version: 0,
                            connection_quality: None,
                            conversation_id,
                            gateway_id: String::new(),
                            server_id: String::new(),
                        }],
                    };
                    cache_for_task.write().await.insert(user_id, dto);
                }
            }
        });

        let _ = crate::shared::util::normalize_tenant_id(default_tenant_id.into());
        Self {
            session_guard: SessionGuard::new(current_user_id.clone(), "presence"),
            http,
            current_user_id,
            device_id: device_id.into().trim().to_string(),
            cache,
            cache_owner_user_id,
            subscribed_user_ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn offline(user_id: &str) -> UserPresenceDto {
        UserPresenceDto {
            user_id: user_id.to_string(),
            is_online: false,
            status: "offline".to_string(),
            last_seen_ms: 0,
            devices: Vec::new(),
        }
    }

    async fn ensure_session_cache(&self) -> Result<String> {
        let user_id = self
            .session_guard
            .capture_user()
            .await?
            .ok_or_else(|| FlareError::localized(ErrorCode::NotConnected, "未连接"))?;
        let should_clear = {
            let mut owner = self.cache_owner_user_id.write().await;
            if owner.as_deref() == Some(user_id.as_str()) {
                false
            } else {
                *owner = Some(user_id.clone());
                true
            }
        };
        if should_clear {
            self.cache.write().await.clear();
        }
        Ok(user_id)
    }

    async fn fetch_user_presence_unbound(&self, user_id: &str) -> Result<UserPresenceDto> {
        let path = format!("/api/v1/presence/users/{user_id}");
        let body: HttpApiResponse<UserPresenceDto> = self.http.get(&path, None).await?;
        unwrap_api_response(body, "get user presence")
    }

    async fn fetch_user_presence(&self, user_id: &str) -> Result<UserPresenceDto> {
        let api = self.clone();
        let user_id = user_id.to_string();
        self.session_guard
            .run_with_user(move |session_user_id| async move {
                let session_user_id = session_user_id
                    .ok_or_else(|| FlareError::localized(ErrorCode::NotConnected, "未连接"))?;
                let dto = api.fetch_user_presence_unbound(&user_id).await?;
                api.session_guard
                    .ensure_unchanged(Some(&session_user_id))
                    .await?;
                api.cache.write().await.insert(user_id, dto.clone());
                Ok(dto)
            })
            .await
    }

    pub async fn get_user_presence(&self, user_id: &str) -> Result<UserPresenceDto> {
        let uid = user_id.trim();
        if uid.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.presence.user_id_required",
            ));
        }
        self.ensure_session_cache().await?;
        if let Some(cached) = self.cache.read().await.get(uid).cloned() {
            return Ok(cached);
        }
        match self.fetch_user_presence(uid).await {
            Ok(dto) => Ok(dto),
            Err(err) if err.code() == Some(ErrorCode::NotConnected) => Err(err),
            Err(_) => Ok(Self::offline(uid)),
        }
    }

    pub async fn batch_get_user_presence(
        &self,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserPresenceDto>> {
        let ids: Vec<String> = user_ids
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        self.ensure_session_cache().await?;

        let mut out = HashMap::new();
        let mut missing = Vec::new();
        {
            let cache = self.cache.read().await;
            for id in &ids {
                if let Some(dto) = cache.get(id) {
                    out.insert(id.clone(), dto.clone());
                } else {
                    missing.push(id.clone());
                }
            }
        }

        if !missing.is_empty() {
            let req = BatchGetUserPresenceHttpRequest {
                user_ids: missing.clone(),
            };
            let api = self.clone();
            let fetch_result = self
                .session_guard
                .run_with_user(move |session_user_id| async move {
                    let session_user_id = session_user_id
                        .ok_or_else(|| FlareError::localized(ErrorCode::NotConnected, "未连接"))?;
                    let body = api
                        .http
                        .post::<_, HttpApiResponse<BatchGetUserPresenceHttpResponse>>(
                            "/api/v1/presence/users/batch",
                            &req,
                        )
                        .await?;
                    let data = unwrap_api_response(body, "batch get user presence")?;
                    api.session_guard
                        .ensure_unchanged(Some(&session_user_id))
                        .await?;
                    Ok(data)
                })
                .await;
            match fetch_result {
                Ok(body) => {
                    let mut cache = self.cache.write().await;
                    for (user_id, dto) in body.presences {
                        cache.insert(user_id.clone(), dto.clone());
                        out.insert(user_id, dto);
                    }
                }
                Err(err) if err.code() == Some(ErrorCode::NotConnected) => return Err(err),
                Err(_) => {
                    for id in missing {
                        out.entry(id.clone()).or_insert_with(|| Self::offline(&id));
                    }
                }
            }
        }

        for id in ids {
            out.entry(id.clone()).or_insert_with(|| Self::offline(&id));
        }
        Ok(out)
    }

    pub async fn list_current_user_devices(&self) -> Result<Vec<DevicePresenceDto>> {
        let api = self.clone();
        self.session_guard
            .run_with_user(move |session_user_id| async move {
                let user_id = session_user_id
                    .ok_or_else(|| FlareError::localized(ErrorCode::NotConnected, "未连接"))?;
                let path = format!("/api/v1/presence/users/{user_id}/devices");
                let body: HttpApiResponse<ListUserDevicesHttpResponse> =
                    api.http.get(&path, None).await?;
                let data = unwrap_api_response(body, "list user devices")?;
                Ok(data.devices)
            })
            .await
    }

    pub async fn get_device(&self, device_id: &str) -> Result<DevicePresenceDto> {
        let device_id = device_id.trim();
        if device_id.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.presence.device_id_required",
            ));
        }
        let path = format!("/api/v1/presence/devices/{device_id}");
        let body: HttpApiResponse<DevicePresenceDto> = self.http.get(&path, None).await?;
        unwrap_api_response(body, "get device")
    }

    pub async fn kick_device(&self, device_id: &str, reason: &str) -> Result<bool> {
        let device_id = device_id.trim();
        if device_id.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "sdk.presence.device_id_required",
            ));
        }
        let req = KickDeviceHttpRequest {
            reason: reason.trim().to_string(),
        };
        let path = format!("/api/v1/presence/devices/{device_id}/kick");
        let body: HttpApiResponse<KickDeviceHttpResponse> = self.http.post(&path, &req).await?;
        Ok(unwrap_api_response(body, "kick device")?.success)
    }

    pub async fn logout_current_device_presence(&self) -> Result<()> {
        let user_id = self.current_user_id.read().await.trim().to_string();
        if user_id.is_empty() {
            return Ok(());
        }
        self.logout_user_device_presence(&user_id).await
    }

    pub(crate) async fn logout_user_device_presence(&self, user_id: &str) -> Result<()> {
        let user_id = user_id.trim().to_string();
        if user_id.is_empty() {
            return Ok(());
        }

        let device_id = self.device_id.trim();
        if device_id.is_empty() {
            return Ok(());
        }

        let presence = self.fetch_user_presence_unbound(&user_id).await?;
        let Some(device) = current_device_presence(presence.devices, device_id) else {
            self.cache.write().await.clear();
            return Ok(());
        };

        let req = LogoutPresenceHttpRequest {
            conversation_id: device.conversation_id,
        };
        let body: HttpApiResponse<LogoutPresenceHttpResponse> =
            self.http.post("/api/v1/presence/logout", &req).await?;
        let logout = unwrap_api_response(body, "logout presence")?;
        if !logout.success {
            return Err(FlareError::localized(
                ErrorCode::GeneralError,
                "logout presence failed: the online service did not end the session",
            ));
        }
        self.cache.write().await.clear();
        Ok(())
    }

    pub async fn subscribe_user_presence(&self, user_ids: Vec<String>) -> Result<()> {
        self.ensure_session_cache().await?;
        let mut subscribed = self.subscribed_user_ids.lock().await;
        for user_id in user_ids {
            let uid = user_id.trim();
            if !uid.is_empty() {
                subscribed.insert(uid.to_string());
            }
        }
        Ok(())
    }
}

fn current_device_presence(
    devices: Vec<DevicePresenceDto>,
    device_id: &str,
) -> Option<DevicePresenceDto> {
    let device_id = device_id.trim();
    if device_id.is_empty() {
        return None;
    }
    devices.into_iter().find(|device| {
        device.device_id.trim() == device_id && !device.conversation_id.trim().is_empty()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_presence_defaults_to_offline_status() {
        let dto = PresenceApi::offline("alice");
        assert_eq!(dto.user_id, "alice");
        assert!(!dto.is_online);
        assert_eq!(dto.status, "offline");
        assert!(dto.devices.is_empty());
    }

    #[test]
    fn user_presence_dto_deserializes_gateway_http_payload() {
        let json = r#"{
            "userId": "alice",
            "isOnline": true,
            "status": "online",
            "lastSeenMs": 42,
            "devices": [{
                "deviceId": "d1",
                "platform": "web",
                "model": "Mac",
                "osVersion": "14",
                "lastActiveTimeMs": 99,
                "priority": 2,
                "tokenVersion": 7,
                "connectionQuality": {
                    "rttMs": 12,
                    "packetLossRate": 0.25,
                    "lastMeasuredAt": 42,
                    "networkType": "wifi",
                    "signalStrength": 4
                },
                "conversationId": "c1",
                "gatewayId": "gw1",
                "serverId": "srv1"
            }]
        }"#;
        let dto: UserPresenceDto = serde_json::from_str(json).expect("deserialize presence dto");
        assert_eq!(dto.user_id, "alice");
        assert!(dto.is_online);
        assert_eq!(dto.devices[0].conversation_id, "c1");
        assert_eq!(
            dto.devices[0]
                .connection_quality
                .as_ref()
                .map(|q| q.network_type.as_str()),
            Some("wifi")
        );
    }

    fn device(
        device_id: &str,
        conversation_id: &str,
        last_active_time_ms: i64,
    ) -> DevicePresenceDto {
        DevicePresenceDto {
            device_id: device_id.to_string(),
            platform: "web".to_string(),
            model: "Browser".to_string(),
            os_version: "macOS".to_string(),
            last_active_time_ms,
            priority: 2,
            token_version: 1,
            connection_quality: None,
            conversation_id: conversation_id.to_string(),
            gateway_id: "gateway".to_string(),
            server_id: "server".to_string(),
        }
    }

    #[test]
    fn current_device_presence_selects_exact_device_id_not_recent_device() {
        let selected = current_device_presence(
            vec![
                device("device-b", "conn-b", 300),
                device("device-a", "conn-a", 100),
            ],
            "device-a",
        )
        .expect("current device");

        assert_eq!(selected.device_id, "device-a");
        assert_eq!(selected.conversation_id, "conn-a");
    }

    /// 本机桩：依次接受 `responses.len()` 个连接，每个连接读一个请求并回对应的响应；
    /// 返回每个请求的「方法 路径」行与请求体。
    #[cfg(not(target_arch = "wasm32"))]
    async fn gateway_stub(
        responses: Vec<(u16, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<Vec<(String, String)>>) {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let mut seen = Vec::new();
            for (status, body) in responses {
                let (socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(socket);
                let mut request_line = String::new();
                reader.read_line(&mut request_line).await.unwrap();
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    let line = line.trim_end();
                    if line.is_empty() {
                        break;
                    }
                    if let Some((key, value)) = line.split_once(':')
                        && key.trim().eq_ignore_ascii_case("content-length")
                    {
                        content_length = value.trim().parse().unwrap();
                    }
                }
                let mut request_body = vec![0u8; content_length];
                reader.read_exact(&mut request_body).await.unwrap();
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                reader
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .unwrap();
                let line = request_line.trim_end();
                let line = line.strip_suffix(" HTTP/1.1").unwrap_or(line).to_string();
                seen.push((line, String::from_utf8(request_body).unwrap()));
            }
            seen
        });
        (base, handle)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn presence_api(base: &str, user_id: &str, device_id: &str) -> PresenceApi {
        PresenceApi::new(
            HttpClient::new(base),
            Arc::new(RwLock::new(user_id.to_string())),
            device_id,
            "0",
            EventBus::new(),
        )
    }

    /// 在线状态查询里的当前设备（登出按它的 conversationId 走）。
    #[cfg(not(target_arch = "wasm32"))]
    const PRESENCE_WITH_CURRENT_DEVICE: &str = r#"{"code":0,"data":{
        "userId":"alice","isOnline":true,"status":"online","lastSeenMs":1,
        "devices":[{"deviceId":"dev-a","platform":"pc","model":"m","osVersion":"1",
            "lastActiveTimeMs":1,"priority":1,"tokenVersion":1,"connectionQuality":null,
            "conversationId":"conn-a","gatewayId":"gw","serverId":"srv"}]}}"#;

    /// 登出响应的契约：`flare-api-gateway` 的 `logout_presence` 返回
    /// `ApiResponse::success(LogoutPresenceHttpResponse { success })`，`ApiResponse` 省略空字段，
    /// 线上就是下面这一行。以前 SDK 按 `data: ()` 解码，每次都报
    /// 「parse http json failed: error decoding response body」再退回直接断开传输。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn presence_logout_accepts_the_gateway_response() {
        let logout_ok = r#"{"code":0,"data":{"success":true}}"#;
        let (base, stub) =
            gateway_stub(vec![(200, PRESENCE_WITH_CURRENT_DEVICE), (200, logout_ok)]).await;
        let api = presence_api(&base, "alice", "dev-a");

        api.logout_user_device_presence("alice")
            .await
            .expect("gateway logout response must decode");

        let seen = stub.await.unwrap();
        assert_eq!(seen[0].0, "GET /api/v1/presence/users/alice");
        assert_eq!(seen[1].0, "POST /api/v1/presence/logout");
        let body: serde_json::Value = serde_json::from_str(&seen[1].1).unwrap();
        assert_eq!(body, serde_json::json!({"conversationId": "conn-a"}));
    }

    /// Online 没结束会话（`success: false`）要报错，调用方据此退回断开传输。
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn presence_logout_not_ended_is_an_error() {
        let logout_rejected = r#"{"code":0,"data":{"success":false}}"#;
        let (base, stub) = gateway_stub(vec![
            (200, PRESENCE_WITH_CURRENT_DEVICE),
            (200, logout_rejected),
        ])
        .await;
        let api = presence_api(&base, "alice", "dev-a");

        let err = api
            .logout_user_device_presence("alice")
            .await
            .expect_err("success=false is not a logout");
        assert!(err.to_string().contains("logout presence failed"), "{err}");
        assert!(!err.to_string().contains("decoding"), "{err}");
        stub.await.unwrap();
    }

    #[test]
    fn current_device_presence_requires_active_conversation_id() {
        let selected = current_device_presence(
            vec![
                device("device-a", "", 300),
                device("device-b", "conn-b", 100),
            ],
            "device-a",
        );

        assert!(selected.is_none());
    }
}
