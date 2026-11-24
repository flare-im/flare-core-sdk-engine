use flare_im_core_sdk::prelude::CoreEndpoints;

pub const LOCAL_CORE_HTTP: &str = "http://127.0.0.1:41050";

pub fn for_websocket(websocket_url: String) -> CoreEndpoints {
    CoreEndpoints {
        base_url: std::env::var("FLARE_SMOKE_HTTP_URL").unwrap_or_else(|_| LOCAL_CORE_HTTP.into()),
        websocket_url,
        quic_url: None,
        media_base_url: None,
    }
}
