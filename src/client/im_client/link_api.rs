//! 宿主提供直连地址，连接状态机处理建连和重连。

use std::sync::Arc;

use crate::runtime::link::ProtocolMode;

use crate::client::{CoreEndpoints, EndpointUpdate};
use crate::kernel::event::ConnectionStatus;
use crate::runtime::link::ConnectionRuntime;
use crate::shared::error::{ErrorCode, FlareError, Result};

use super::IMClient;

impl IMClient {
    /// Replace the complete host-selected address group for this Core deployment.
    pub async fn replace_endpoints(&self, endpoints: CoreEndpoints) -> Result<EndpointUpdate> {
        let g = self.inner.read().await;
        let runtime = g
            .connection
            .as_ref()
            .ok_or_else(Self::uninitialised_connection)?;
        let endpoints = runtime.validate_replacement(endpoints)?;
        if let Some(link) = g.engine.as_ref().map(|engine| engine.link())
            && Arc::ptr_eq(runtime, link.connection())
        {
            return link.replace_endpoints(endpoints).await;
        }
        runtime.replace_endpoints(endpoints)
    }

    pub(crate) async fn connection_runtime(&self) -> Option<Arc<ConnectionRuntime>> {
        self.inner.read().await.connection.clone()
    }

    /// 当前连接状态（`connection://status` 的最新值）。没有会话时为 `idle`。
    pub async fn connection_status(&self) -> ConnectionStatus {
        let g = self.inner.read().await;
        let access = g.connection.as_ref();
        match g.engine.as_ref().filter(|engine| {
            access.is_none_or(|runtime| {
                runtime.endpoints.is_none() || Arc::ptr_eq(runtime, engine.link().connection())
            })
        }) {
            Some(engine) => engine.connection_status(),
            None => {
                let mut status =
                    ConnectionStatus::idle(access.is_some_and(|access| access.development_mode()));
                status.endpoint_revision = access.and_then(|access| access.endpoint_revision());
                status
            }
        }
    }

    /// Most recent outbound session traffic. Zero means no packet has been sent.
    pub async fn last_outbound_activity_at_ms(&self) -> u64 {
        self.inner
            .read()
            .await
            .engine
            .as_ref()
            .map_or(0, |engine| engine.sender().last_activity_ms())
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

    /// Applies a host-selected IM transport policy without replacing the session.
    pub async fn set_protocol_mode(&self, mode: &str) -> Result<()> {
        let mode = match mode {
            "race" => ProtocolMode::Race,
            "ws" => ProtocolMode::WebSocket,
            "quic" if cfg!(all(feature = "quic", not(target_arch = "wasm32"))) => {
                ProtocolMode::Quic
            }
            "quic" => {
                return Err(FlareError::localized(
                    ErrorCode::OperationNotSupported,
                    "QUIC is unavailable in this build",
                ));
            }
            _ => {
                return Err(FlareError::localized(
                    ErrorCode::InvalidParameter,
                    "protocol mode must be ws, quic, or race",
                ));
            }
        };
        let link = {
            let g = self.inner.read().await;
            let access = g
                .connection
                .as_ref()
                .ok_or_else(Self::uninitialised_connection)?;
            if mode == ProtocolMode::Quic
                && access
                    .endpoints
                    .as_ref()
                    .is_some_and(|store| store.snapshot().endpoints.quic_url.is_none())
            {
                return Err(FlareError::localized(
                    ErrorCode::ConfigurationError,
                    "QUIC requires sdkConfig.endpoints.quicUrl",
                ));
            }
            let changed = access.set_protocol_mode(mode);
            g.engine
                .as_ref()
                .map(|engine| engine.link())
                .filter(|link| {
                    if Arc::ptr_eq(access, link.connection()) {
                        changed
                    } else {
                        link.connection().set_protocol_mode(mode)
                    }
                })
                .cloned()
        };
        if let Some(link) = link {
            link.protocol_mode_changed().await;
        }
        Ok(())
    }

    /// An anonymous HTTP client bound to the current Core media address.
    pub async fn media_http_client(
        &self,
    ) -> Result<crate::infrastructure::transport::http::HttpClient> {
        self.http_client(crate::infrastructure::transport::http::router::CoreHttpService::Media)
            .await
    }

    pub(crate) async fn http_client(
        &self,
        service: crate::infrastructure::transport::http::router::CoreHttpService,
    ) -> Result<crate::infrastructure::transport::http::HttpClient> {
        use crate::client::lifecycle::merge_sdk_config_onto;
        use crate::infrastructure::transport::http::HttpClient;
        use crate::infrastructure::transport::http::router::HttpRouter;

        let access = self.require_connection().await?;
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
        Ok(
            if service == crate::infrastructure::transport::http::router::CoreHttpService::CoreApi {
                http.with_core_bearer()
            } else {
                http.anonymous_clone()
            },
        )
    }

    fn uninitialised_connection() -> FlareError {
        FlareError::localized(
            ErrorCode::ConfigurationError,
            "connection is not initialised; call init with sdkConfig.endpoints",
        )
    }

    async fn require_connection(&self) -> Result<Arc<ConnectionRuntime>> {
        self.connection_runtime()
            .await
            .ok_or_else(Self::uninitialised_connection)
    }
}
