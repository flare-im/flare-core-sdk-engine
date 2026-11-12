//! Tauri binding for Flare IM Core SDK.
//!
//! This crate is an IPC adapter over the shared binding runtime. Dedicated
//! commands are limited to lifecycle paths that need `AppHandle` or host state;
//! all normal SDK operations go through generated `sdk_invoke_json`.

pub mod commands;
pub mod convert;
pub mod generated;
pub mod host_files;
pub mod state;

pub mod model {
    use flare_im_core_sdk::client::SdkConfigOverlay;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SdkInitArgs {
        #[serde(default)]
        pub environment: Option<String>,
        #[serde(default, rename = "sdkConfig")]
        pub sdk_config: Option<SdkConfigOverlay>,
    }
}

pub const BINDING_CONTRACT_VERSION: &str =
    flare_im_core_sdk_bindings_runtime::BINDING_CONTRACT_VERSION;

pub type SdkConfigOptions = flare_im_core_sdk::client::SdkConfigOverlay;

pub use commands::*;
pub use host_files::{StagedGeneratedFile, sdk_cleanup_generated_file, sdk_stage_generated_file};
pub use model::SdkInitArgs;
pub use state::SdkState;

/// Generated IM commands plus handwritten platform file adaptation.
pub fn im_invoke_handler() -> impl Fn(tauri::ipc::Invoke) -> bool + Send + 'static {
    let generated = generated::handler::im_invoke_handler();
    let files: fn(tauri::ipc::Invoke) -> bool = tauri::generate_handler![
        host_files::sdk_stage_generated_file,
        host_files::sdk_cleanup_generated_file,
    ];
    move |invoke| match invoke.message.command() {
        "sdk_stage_generated_file" | "sdk_cleanup_generated_file" => files(invoke),
        _ => generated(invoke),
    }
}
