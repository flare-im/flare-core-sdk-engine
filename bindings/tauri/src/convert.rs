//! Core SDK event to Tauri IPC payload conversion.
//!
//! The shared binding runtime owns canonical event payloads and channels
//! (`im://*`, `connection://status`, `access://*`). Tauri only adapts the shared
//! tuple into `Emitter::emit`.

use flare_im_core_sdk::event::SdkEvent;
use flare_im_core_sdk::serde_json::Value;

pub fn sdk_event_to_tauri(ev: &SdkEvent) -> Option<(String, Value)> {
    flare_im_core_sdk_bindings_runtime::sdk_event_channel_payload(ev)
        .map(|(channel, payload)| (channel.to_string(), payload))
}
