//! 连接与接入线路的宿主 API：`connection.get_status`、`connection.reconnect_now`、`access.*`。
//!
//! 全部只是转告链路状态机或读取接入运行时：何时重连、换哪条线路由状态机决定，宿主不做判断。

use std::sync::Arc;

use crate::kernel::event::ConnectionStatus;
use crate::runtime::link::{
    AccessPointDto, AccessRuntime, AccessStatusDto, RecoveryImportDto, RefreshReportDto,
};
use crate::shared::error::{ErrorCode, FlareError, Result};

use super::IMClient;

/// `access.pin` 的返回值。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PinnedAccessPoint {
    pub access_point_id: String,
    /// 固定到期（Unix 毫秒）；App 重启也会解除。
    pub expires_at_ms: i64,
}

impl IMClient {
    pub(crate) async fn access_runtime(&self) -> Option<Arc<AccessRuntime>> {
        self.inner.read().await.access.clone()
    }

    /// 当前连接状态（`connection://status` 的最新值）。没有会话时为 `idle`。
    pub async fn connection_status(&self) -> ConnectionStatus {
        let g = self.inner.read().await;
        match g.engine.as_ref() {
            Some(engine) => engine.connection_status(),
            None => ConnectionStatus::idle(
                g.access
                    .as_ref()
                    .is_some_and(|access| access.development_mode()),
            ),
        }
    }

    /// 横幅「立即重试」：重置退避，立即一轮。在线、正在连接或已终止时不打断。
    pub async fn reconnect_now(&self) -> Result<()> {
        let link = {
            let g = self.inner.read().await;
            g.engine.as_ref().map(|engine| engine.link().clone())
        };
        let link = link.ok_or_else(|| {
            FlareError::localized(ErrorCode::NotConnected, "no session to reconnect")
        })?;
        link.reconnect_now().await;
        Ok(())
    }

    /// 按服务路由的 HTTP 客户端，给业务 SDK（社交 SDK 等）用：基址跟随 IM 当前接入点，失败按
    /// 重试表换点并回写健康簿；非幂等写请求用 [`HttpClient::post_idempotent`] 带幂等键，同一次
    /// 用户动作的重试复用同一个键。鉴权头取共享的 HTTP 上下文（与媒体、能力同一份）。
    pub async fn routed_http_client(
        &self,
        service: flare_access::ServiceClass,
    ) -> Result<crate::infrastructure::transport::http::HttpClient> {
        use crate::client::lifecycle::merge_sdk_config_onto;
        use crate::infrastructure::transport::http::HttpClient;
        use crate::infrastructure::transport::http::router::HttpRouter;

        let access = self.require_access().await?;
        let (context, config) = {
            let g = self.inner.read().await;
            let base = g.configured_config.clone().unwrap_or_default();
            (
                g.http_request_context.clone(),
                merge_sdk_config_onto(base, g.sdk_config.as_ref()),
            )
        };
        let context = context.ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "the HTTP context is not initialised; call init first",
            )
        })?;
        let router = HttpRouter::new(access, service);
        #[cfg(not(target_arch = "wasm32"))]
        let http = HttpClient::routed(router, context, config.core_tls_config());
        #[cfg(target_arch = "wasm32")]
        let http = {
            let _ = &config;
            HttpClient::routed(router, context)
        };
        Ok(http)
    }

    async fn require_access(&self) -> Result<Arc<AccessRuntime>> {
        self.access_runtime().await.ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "access is not initialised; call init with sdkConfig.access first",
            )
        })
    }

    /// 接入状态：清单 serial、来源、是否过期、开发模式、当前 Region/Cell/接入点/协议、手动固定。
    pub async fn access_status(&self) -> Result<AccessStatusDto> {
        Ok(self.require_access().await?.status())
    }

    /// 线路诊断页数据：归属 Cell 的全部接入点与它们在当前网络下的 RTT、熔断状态、是否当前、是否固定。
    pub async fn list_access_points(&self) -> Result<Vec<AccessPointDto>> {
        Ok(self.require_access().await?.list_access_points())
    }

    /// 手动固定接入点（24 小时或 App 重启后解除）。当前不在它上面时立即切过去（不重登）。
    pub async fn pin_access_point(&self, access_point_id: &str) -> Result<PinnedAccessPoint> {
        let access = self.require_access().await?;
        // 目录可能还在后台装配：等它完成，否则用户点选会被当成未知接入点。
        access.kernel().await.map_err(|error| error.to_error())?;
        let pinned = access.pin(access_point_id)?;
        let link = {
            let g = self.inner.read().await;
            g.engine.as_ref().map(|engine| engine.link().clone())
        };
        if let Some(link) = link {
            link.repin().await;
        }
        Ok(PinnedAccessPoint {
            access_point_id: pinned.ap_id.to_string(),
            expires_at_ms: pinned.expires_at_ms,
        })
    }

    /// 解除手动固定，回到自动选路（不立即切换，下一个自然边界由恢复切回决定）。
    pub async fn unpin_access_point(&self) -> Result<()> {
        self.require_access().await?.unpin();
        Ok(())
    }

    /// 立即刷新接入清单（诊断页使用）。恢复期间这一轮试遍所有引导层。
    pub async fn refresh_access(&self) -> Result<RefreshReportDto> {
        self.require_access().await?.refresh().await
    }

    /// 导入恢复链接或二维码内容（`flare-recover://…`）。验签之后与网络下发的清单走同一条采纳路径；
    /// 被拒是返回值里的 `rejected`，不是错误。
    pub async fn import_access_recovery(&self, link: &str) -> Result<RecoveryImportDto> {
        self.require_access().await?.import_recovery(link).await
    }

    /// 诊断 JSON：接入状态、各接入点健康、网络指纹（本地哈希）与告警。不含令牌、用户与消息内容。
    pub async fn export_access_diagnostics(&self) -> Result<serde_json::Value> {
        let mut diagnostics = self.require_access().await?.export_diagnostics();
        if let Some(object) = diagnostics.as_object_mut() {
            object.insert(
                "connection".to_owned(),
                serde_json::to_value(self.connection_status().await).unwrap_or_default(),
            );
        }
        Ok(diagnostics)
    }
}
