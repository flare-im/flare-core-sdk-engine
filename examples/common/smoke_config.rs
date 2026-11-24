//! Explicit test endpoints/storage. Never redirect HOME or reuse the evaluator's SDK database.
use flare_im_core_sdk::client::lifecycle::SdkConfigOverlay;
use flare_im_core_sdk::prelude::CoreEndpoints;
use std::{io, path::Path};

pub fn overlay() -> io::Result<Option<SdkConfigOverlay>> {
    from_values(
        std::env::var("FLARE_SMOKE_DATA_ROOT").ok().as_deref(),
        std::env::var("FLARE_SMOKE_WS_URL")
            .or_else(|_| std::env::var("FLARE_IM_SERVER_URL"))
            .ok()
            .as_deref(),
        std::env::var("FLARE_SMOKE_HTTP_URL").ok().as_deref(),
    )
}

fn from_values(
    root: Option<&str>,
    ws: Option<&str>,
    http: Option<&str>,
) -> io::Result<Option<SdkConfigOverlay>> {
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
    Ok(Some(SdkConfigOverlay {
        data_url: root.map(str::to_owned),
        endpoints: Some(CoreEndpoints {
            base_url: http.unwrap_or("http://127.0.0.1:41050").into(),
            websocket_url: ws.unwrap_or("ws://localhost:60051").into(),
            quic_url: None,
            media_base_url: None,
        }),
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_absolute_storage_override_preserves_network_and_identity_defaults() {
        let defaults = from_values(None, None, None).unwrap().unwrap();
        for invalid in ["", " ", "relative", "../data"] {
            assert!(from_values(Some(invalid), None, None).is_err());
        }
        let configured = from_values(Some("/tmp/smoke-data"), None, None)
            .unwrap()
            .unwrap();
        assert_eq!(configured.data_url.as_deref(), Some("/tmp/smoke-data"));
        assert_eq!(configured.endpoints, defaults.endpoints);
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
        let endpoints = configured.endpoints.expect("development endpoints");
        assert_eq!(endpoints.websocket_url, "ws://127.0.0.1:8080");
        assert_eq!(endpoints.base_url, "http://127.0.0.1:50050");
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
