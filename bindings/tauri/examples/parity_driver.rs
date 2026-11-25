//! Drives the Tauri binding's command path without a window, for cross-binding parity.
//!
//! Reads `operation\tbody` lines like `tests/storage_parity/native.c`: `sdk.init` takes
//! `{environment, sdkConfig}`, `sdk.prepare` takes the user ID, and every other line is
//! an API ID with its JSON request. Each reply is one line: the value the Tauri command
//! resolves with, or `{"__error": ...}` holding the value it rejects with. The calls are
//! the ones in `commands::lifecycle` and `generated::invoke`, minus `AppHandle` and the
//! webview event bridge.

use flare_im_core_sdk::client::SdkConfigOverlay;
use flare_im_core_sdk::serde_json::{self, Value, json};
use flare_im_core_sdk_bindings_runtime::{
    binding_response_to_value, invoke_api_id_json, sdk_error_value,
};
use flare_im_core_sdk_tauri::{SdkState, with_tauri_app_info};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct InitArgs {
    environment: Option<String>,
    sdk_config: Option<SdkConfigOverlay>,
}

async fn handle(state: &SdkState, operation: &str, body: &str) -> Result<Value, Value> {
    let fail = |error: &flare_im_core_sdk::FlareError| sdk_error_value(error, Some(operation));
    match operation {
        "sdk.init" => {
            let args: InitArgs = serde_json::from_str(body).map_err(
                |error| json!({"code": "invalidParameter", "message": error.to_string()}),
            )?;
            let config = with_tauri_app_info(args.sdk_config, env!("CARGO_PKG_VERSION").into());
            state
                .set_config(args.environment, Some(config))
                .await
                .map_err(|e| fail(&e))?;
            Ok(Value::Null)
        }
        "sdk.prepare" => {
            state
                .client()
                .prepare(body, flare_im_core_sdk_sqlite_im::sqlite())
                .await
                .map_err(|e| fail(&e))?;
            Ok(Value::Null)
        }
        api_id => invoke_api_id_json(state, api_id, body)
            .await
            .map(binding_response_to_value)
            .map_err(|e| fail(&e)),
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let state = SdkState::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Some((operation, body)) = line.split_once('\t') else {
            std::process::exit(1);
        };
        let reply = match handle(&state, operation, body).await {
            Ok(value) => value,
            Err(error) => json!({ "__error": error }),
        };
        println!("{reply}");
    }
}
