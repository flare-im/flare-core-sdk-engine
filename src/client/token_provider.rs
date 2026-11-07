//! 接入 token 的网关签发/刷新（SDK 托管形态）。
//!
//! 对应 `flare-api-gateway` 的 `POST /api/v1/auth/tokens` 与 `POST /api/v1/auth/tokens/refresh`
//! （见 flare-core-server/docs/AUTH-TOKEN-ISSUANCE.zh-CN.md）。客户端不持有任何签名密钥。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::infrastructure::transport::http::HttpClient;
use crate::shared::error::{ErrorCode, FlareError, Result};

/// 宿主托管令牌时的连接令牌续期回调。
///
/// 核心自己只会向 SDK 托管的签发端点换令牌（[`GatewayTokenProvider`]，要配
/// `auth`）。令牌由宿主递进来时（例如社交网关连同 IM 连接令牌一起签发），
/// 核心没有渠道换新：连接一断，重连就一直拿着那枚过期令牌去撞网关，网关回通用的鉴权失败，
/// 核心只记日志、不发任何事件，宿主也就无从知道该续期了。实测 web 端因此两个多小时重连
/// 三百多次、始终连不上。
///
/// 装上这个回调后，重连被网关按鉴权拒绝时核心会向宿主要一枚新的，换到了就立刻用它重连。
/// 宿主负责单飞与防重放：刷新令牌通常是一次性的，同一枚被换两次会被服务端当成泄露。
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait ConnectTokenRefresher: Send + Sync {
    /// 返回当前可用的连接令牌（刷新成功后是新的那枚）；会话已失效、换不到时返回 `None`。
    ///
    /// 核心只在返回值与手上那枚不同时才当作「换到了」，所以刷新没成功时原样返回旧的也可以。
    async fn refresh_connect_token(&self) -> Option<String>;
}

/// The same host refresher renews the Core HTTP bearer after Core reports it expired.
pub(crate) struct ConnectTokenRenewal(pub(crate) std::sync::Arc<dyn ConnectTokenRefresher>);

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl crate::infrastructure::transport::http::CoreBearerRenewal for ConnectTokenRenewal {
    async fn renew(&self) -> Option<String> {
        self.0.refresh_connect_token().await
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IssuedAccessToken {
    pub token: String,
    pub expires_at: u64,
    pub user_id: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub device_id: Option<String>,
    /// 长效刷新令牌：接入令牌过期后凭它换新，无需重登（7x24）。旧网关不下发时为 `None`。
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub refresh_expires_at: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    code: i32,
    data: Option<T>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IssueBody<'a> {
    user_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    device_id: Option<&'a str>,
}

/// 向 Core API 签发/换新连接令牌（SDK 托管形态）。
///
/// 请求经按服务路由的 Core API 客户端发出（换接入点与重试照 §3.9），基址来自接入配置；这里只有路径：
/// 签发是配置的路径（默认 `/api/v1/auth/tokens`），换新是它后面加 `/refresh`。
#[derive(Clone)]
pub struct GatewayTokenProvider {
    issue_path: String,
    tenant_id: Option<String>,
    device_id: Option<String>,
    http: HttpClient,
}

impl std::fmt::Debug for GatewayTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayTokenProvider")
            .field("issue_path", &self.issue_path)
            .finish()
    }
}

impl GatewayTokenProvider {
    /// `http` 是 Core API 的路由客户端，不该带会话鉴权头：签发与换新各自给出凭据。
    pub fn new(
        http: HttpClient,
        issue_path: impl Into<String>,
        tenant_id: Option<String>,
        device_id: Option<String>,
    ) -> Self {
        Self {
            issue_path: issue_path.into(),
            tenant_id,
            device_id,
            http,
        }
    }

    pub fn issue_path(&self) -> &str {
        &self.issue_path
    }

    pub fn refresh_path(&self) -> String {
        format!("{}/refresh", self.issue_path)
    }

    pub async fn issue(&self, user_id: &str) -> Result<IssuedAccessToken> {
        let user_id = user_id.trim();
        if user_id.is_empty() {
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "user_id is required",
            ));
        }
        let body = IssueBody {
            user_id,
            tenant_id: self.tenant_id.as_deref(),
            device_id: self.device_id.as_deref(),
        };
        let envelope: Envelope<IssuedAccessToken> =
            self.http
                .post(&self.issue_path, &body)
                .await
                .map_err(|err| token_endpoint_error("issue", &self.issue_path, err))?;
        unwrap_envelope("issue", envelope)
    }

    pub async fn refresh(&self, current_token: &str) -> Result<IssuedAccessToken> {
        let mut headers = HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            format!("Bearer {}", current_token.trim()),
        );
        let refresh_path = self.refresh_path();
        let envelope: Envelope<IssuedAccessToken> = self
            .http
            .post_with_headers(&refresh_path, &serde_json::json!({}), &headers)
            .await
            .map_err(|err| token_endpoint_error("refresh", &refresh_path, err))?;
        unwrap_envelope("refresh", envelope)
    }
}

fn token_endpoint_error(action: &str, path: &str, err: FlareError) -> FlareError {
    // 网关 401/403 是鉴权问题（联调开关没开 / 凭据不对 / 旧 token 超宽限）；网关答了别的是服务不可用；
    // 没有答复（连不上、结果未知）是网络问题——链路按网络失败退避，`connect` 报 NETWORK_UNREACHABLE。
    let status = err
        .as_localized()
        .and_then(|error| error.params)
        .and_then(|params| {
            params
                .get("httpStatus")
                .and_then(|status| status.parse::<u16>().ok())
        });
    let code = if matches!(err.code(), Some(ErrorCode::AuthenticationFailed))
        || matches!(status, Some(401 | 403))
    {
        ErrorCode::AuthenticationFailed
    } else if status.is_some() {
        ErrorCode::ServiceUnavailable
    } else {
        ErrorCode::NetworkUnreachable
    };
    FlareError::localized(
        code,
        format!("token {action} via the Core API failed ({path}): {err}"),
    )
}

fn unwrap_envelope(
    action: &str,
    envelope: Envelope<IssuedAccessToken>,
) -> Result<IssuedAccessToken> {
    if envelope.code != 0 {
        return Err(FlareError::localized(
            ErrorCode::AuthenticationFailed,
            format!(
                "token {action} rejected by gateway: {} {}",
                envelope.reason.unwrap_or_default(),
                envelope.message.unwrap_or_default()
            ),
        ));
    }
    let issued = envelope.data.ok_or_else(|| {
        FlareError::localized(
            ErrorCode::ServiceUnavailable,
            format!("token {action}: gateway returned no data"),
        )
    })?;
    if issued.token.trim().is_empty() {
        return Err(FlareError::localized(
            ErrorCode::ServiceUnavailable,
            format!("token {action}: gateway returned an empty token"),
        ));
    }
    Ok(issued)
}

/// 从 JWT 的 payload 里读 `exp`（秒）。不校验签名——只用于安排刷新时机。
pub fn jwt_exp_secs(token: &str) -> Option<u64> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value.get("exp")?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issued_token_parses_refresh_token_camelcase() {
        let raw = r#"{"token":"a.b.c","expiresAt":123,"userId":"u","refreshToken":"r.r.r","refreshExpiresAt":456}"#;
        let issued: IssuedAccessToken = serde_json::from_str(raw).unwrap();
        assert_eq!(issued.refresh_token.as_deref(), Some("r.r.r"));
        assert_eq!(issued.refresh_expires_at, Some(456));
    }

    #[test]
    fn issued_token_without_refresh_is_backward_compatible() {
        let raw = r#"{"token":"a.b.c","expiresAt":123,"userId":"u"}"#;
        let issued: IssuedAccessToken = serde_json::from_str(raw).unwrap();
        assert_eq!(issued.refresh_token, None);
    }

    #[test]
    fn refresh_follows_the_issue_path() {
        let p = GatewayTokenProvider::new(
            HttpClient::new(""),
            crate::client::config::SdkAuthConfig::DEFAULT_TOKEN_PATH,
            None,
            None,
        );
        assert_eq!(p.issue_path(), "/api/v1/auth/tokens");
        assert_eq!(p.refresh_path(), "/api/v1/auth/tokens/refresh");
    }

    #[test]
    fn jwt_exp_is_read_from_payload() {
        let token =
            crate::shared::util::generate_core_token(&crate::shared::util::CoreTokenConfig {
                secret: "a-strong-shared-secret-with-more-than-32-bytes!".into(),
                issuer: "flare-im-core".into(),
                user_id: "u".into(),
                ttl_secs: 3600,
                device_id: None,
                tenant_id: None,
            })
            .unwrap();
        let exp = jwt_exp_secs(&token).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(exp >= now + 3500 && exp <= now + 3600);
        assert_eq!(jwt_exp_secs("not-a-jwt"), None);
    }

    #[test]
    fn envelope_errors_map_to_authentication_failed() {
        let err = unwrap_envelope(
            "issue",
            Envelope {
                code: 401,
                data: None,
                reason: Some("UNAUTHORIZED".into()),
                message: None,
            },
        )
        .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::AuthenticationFailed));
    }
}
