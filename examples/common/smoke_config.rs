//! Explicit test endpoints/storage. Never redirect HOME or reuse the evaluator's SDK database.
//!
//! 端点经开发模式接入配置传入（`access.developmentEndpoints`），SDK 没有其他地址字段。
use flare_im_core_sdk::client::lifecycle::SdkConfigOverlay;
use flare_im_core_sdk::prelude::{AccessConfig, DevelopmentEndpoints};
use std::{io, path::Path};

pub fn overlay() -> io::Result<Option<SdkConfigOverlay>> {
    from_values(
        std::env::var("FLARE_SMOKE_DATA_ROOT").ok().as_deref(),
        std::env::var("FLARE_SMOKE_WS_URL").ok().as_deref(),
        std::env::var("FLARE_SMOKE_HTTP_URL").ok().as_deref(),
    )
}

fn from_values(
    root: Option<&str>,
    ws: Option<&str>,
    http: Option<&str>,
) -> io::Result<Option<SdkConfigOverlay>> {
    if root.is_none() && ws.is_none() && http.is_none() {
        return Ok(None);
    }
    if root.is_some_and(|root| root.trim().is_empty() || !Path::new(root).is_absolute()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "FLARE_SMOKE_DATA_ROOT must be an absolute directory",
        ));
    }
    for (value, schemes) in [(ws, ["ws", "wss"]), (http, ["http", "https"])] {
        if let Some(value) = value {
            let url = url::Url::parse(value)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid smoke URL"))?;
            if !schemes.contains(&url.scheme())
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid smoke URL scheme/host/credentials",
                ));
            }
        }
    }
    let access = (ws.is_some() || http.is_some()).then(|| {
        AccessConfig::development(DevelopmentEndpoints {
            ws_url: ws.map(str::to_owned),
            core_api_url: http.map(str::to_owned),
            media_url: http.map(str::to_owned),
            ..Default::default()
        })
    });
    Ok(Some(SdkConfigOverlay {
        data_url: root.map(str::to_owned),
        access,
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_absolute_storage_override_preserves_network_and_identity_defaults() {
        assert!(from_values(None, None, None).unwrap().is_none());
        for invalid in ["", " ", "relative", "../data"] {
            assert!(from_values(Some(invalid), None, None).is_err());
        }
        let configured = from_values(Some("/tmp/smoke-data"), None, None)
            .unwrap()
            .unwrap();
        assert_eq!(configured.data_url.as_deref(), Some("/tmp/smoke-data"));
        assert!(configured.access.is_none());
        assert!(configured.tenant_id.is_none());
    }

    #[test]
    fn network_override_is_explicit_and_does_not_accept_embedded_secrets() {
        let configured = from_values(
            None,
            Some("ws://127.0.0.1:8080"),
            Some("http://127.0.0.1:50050"),
        )
        .unwrap()
        .unwrap();
        let endpoints = configured
            .access
            .as_ref()
            .and_then(|access| access.development_endpoints.as_ref())
            .expect("development endpoints");
        assert_eq!(endpoints.ws_url.as_deref(), Some("ws://127.0.0.1:8080"));
        assert_eq!(
            endpoints.core_api_url.as_deref(),
            Some("http://127.0.0.1:50050")
        );
        assert!(configured.data_url.is_none());
        for invalid in [
            "",
            "https://localhost",
            "ws://user:secret@localhost",
            "ws://",
        ] {
            assert!(from_values(None, Some(invalid), None).is_err());
        }
    }
}
