//! 生命周期：透传 [IMClient]；事件仅做 SdkEvent → emit，无查库或合并。

use tauri::{Emitter, State};
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::state::SdkState;
use flare_im_core_sdk::client::{LoginDbKind, SdkConfigOverlay};
use flare_im_core_sdk::event::{SdkEvent, SharedEventReceiver};
use flare_im_core_sdk_bindings_runtime::{SessionTaskSlot, platform_event_bridge_resync_marker};

#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_ffi_contract_version()
-> std::result::Result<serde_json::Value, super::CommandError> {
    Ok(serde_json::json!({
        "version": crate::BINDING_CONTRACT_VERSION
    }))
}

/// 透传 [IMClient::init]；`sdkConfig` / `dataUrl` 由前端与 core-sdk 约定。
///
/// 应用版本与渠道由这里补齐（见 [`with_tauri_app_info`]）。`app` 由 Tauri 运行时注入。
#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_init(
    state: State<'_, SdkState>,
    app: tauri::AppHandle,
    environment: Option<String>,
    sdk_config: Option<SdkConfigOverlay>,
) -> std::result::Result<(), super::CommandError> {
    let sdk_config = with_tauri_app_info(sdk_config, app.package_info().version.to_string());
    state
        .set_config(environment, Some(sdk_config))
        .await
        .map_err(super::map_sdk_err)
}

/// Tauri 宿主的版本号由运行时自带（`tauri.conf.json` 的 `version`），零依赖就能拿到，渠道恒为
/// `tauri`；CONNECT 时随设备信息上报。只补前端没给的项，前端显式传入的值优先。构建号 Tauri
/// 没有独立来源，需要时由前端经 `sdkConfig.appBuild` 传入。
fn with_tauri_app_info(
    sdk_config: Option<SdkConfigOverlay>,
    package_version: String,
) -> SdkConfigOverlay {
    fn is_blank(value: Option<&str>) -> bool {
        value.map(str::trim).is_none_or(str::is_empty)
    }
    let mut overlay = sdk_config.unwrap_or_default();
    if is_blank(overlay.app_version.as_deref()) && !package_version.trim().is_empty() {
        overlay.app_version = Some(package_version);
    }
    if is_blank(overlay.distribution.as_deref()) {
        overlay.distribution = Some("tauri".to_string());
    }
    overlay
}

#[cfg(test)]
mod app_info_tests {
    use super::with_tauri_app_info;
    use flare_im_core_sdk::client::SdkConfigOverlay;

    #[test]
    fn package_version_fills_only_what_the_frontend_left_out() {
        let filled = with_tauri_app_info(None, "2.4.0".to_string());
        assert_eq!(filled.app_version.as_deref(), Some("2.4.0"));
        assert_eq!(filled.distribution.as_deref(), Some("tauri"));
        assert_eq!(filled.app_build, None);

        let explicit = with_tauri_app_info(
            Some(SdkConfigOverlay {
                app_version: Some("2.4.1-hotfix".to_string()),
                app_build: Some(241),
                distribution: Some("direct".to_string()),
                ..Default::default()
            }),
            "2.4.0".to_string(),
        );
        assert_eq!(explicit.app_version.as_deref(), Some("2.4.1-hotfix"));
        assert_eq!(explicit.app_build, Some(241));
        assert_eq!(explicit.distribution.as_deref(), Some("direct"));
    }
}

/// 透传 [IMClient::login]。`app` 由 Tauri 运行时注入（勿从前端 JSON 传参）。
#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_login(
    state: State<'_, SdkState>,
    app: tauri::AppHandle,
    user_id: String,
    token: Option<String>,
) -> std::result::Result<(), super::CommandError> {
    let client = state.client();
    let event_bridge = state.event_bridge();
    event_bridge.clear();
    let event_bridge_for_login = event_bridge.clone();
    let login_result = client
        .login(
            &user_id,
            token.as_deref(),
            LoginDbKind::Sqlite,
            move |bus, _msg_store| {
                let rx = bus.subscribe_shared_raw();
                let app = app.clone();
                spawn_event_bridge(app, rx, event_bridge_for_login.clone());
            },
        )
        .await;
    let apis = match login_result {
        Ok(apis) => apis,
        Err(err) => {
            event_bridge.clear();
            return Err(super::map_sdk_err(err));
        }
    };
    state.install_session(apis).await;

    info!(user_id = %user_id, "sdk_login ok");
    Ok(())
}

/// 透传 [IMClient::prepare]：开库 + 建引擎（不连网），把开库 / 迁移移出登录关键路径。
///
/// 与 [`sdk_connect`] 配合实现「初始化前置、登录只做网络」。
#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_prepare(
    state: State<'_, SdkState>,
    app: tauri::AppHandle,
    user_id: String,
) -> std::result::Result<(), super::CommandError> {
    let client = state.client();
    let event_bridge = state.event_bridge();
    event_bridge.clear();
    client
        .prepare(&user_id, LoginDbKind::Sqlite)
        .await
        .map_err(super::map_sdk_err)?;
    // 预热后订阅事件总线 → webview（等价 login 闭包在 connect 前所做）。
    let bus = client.bus().await.map_err(super::map_sdk_err)?;
    let rx = bus.subscribe_shared_raw();
    spawn_event_bridge(app, rx, event_bridge);
    info!(user_id = %user_id, "sdk_prepare ok");
    Ok(())
}

/// 透传 [IMClient::connect]：连接已预热引擎 + 首次同步（登录的网络半段）。
#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_connect(
    state: State<'_, SdkState>,
    user_id: String,
    token: Option<String>,
) -> std::result::Result<(), super::CommandError> {
    let client = state.client();
    let apis = client
        .connect(&user_id, token.as_deref())
        .await
        .map_err(super::map_sdk_err)?;
    state.install_session(apis).await;
    info!(user_id = %user_id, "sdk_connect ok");
    Ok(())
}

/// EventBus → `im://*`，独立任务避免阻塞登录路径。
fn spawn_event_bridge(app: tauri::AppHandle, rx: SharedEventReceiver, bridge: SessionTaskSlot) {
    let (cancel_tx, cancel_rx) = oneshot::channel();
    bridge.replace(move || {
        let _ = cancel_tx.send(());
    });
    tauri::async_runtime::spawn(async move {
        forward_event_rx_to_webview(app, rx, cancel_rx).await;
    });
}

async fn forward_event_rx_to_webview(
    app: tauri::AppHandle,
    mut rx: SharedEventReceiver,
    mut cancel_rx: oneshot::Receiver<()>,
) {
    let mut missed_events = 0u64;
    loop {
        tokio::select! {
            _ = &mut cancel_rx => break,
            result = rx.recv() => {
                let ev = match result {
                    Ok(e) => e,
                    Err(_) => break,
                };
                if missed_events > 0 {
                    let marker = platform_event_bridge_resync_marker(missed_events);
                    if emit_event_to_webview(&app, &marker) {
                        missed_events = 0;
                    } else {
                        missed_events = missed_events.saturating_add(1);
                        warn!(
                            missed_events,
                            "failed to emit resync marker to Tauri webview"
                        );
                        continue;
                    }
                }

                if !emit_event_to_webview(&app, ev.event()) {
                    missed_events = missed_events.saturating_add(1);
                    warn!(missed_events, "failed to emit SDK event to Tauri webview");
                }
            }
        }
    }
}

fn emit_event_to_webview<R: tauri::Runtime>(app: &tauri::AppHandle<R>, ev: &SdkEvent) -> bool {
    let Some((name, payload)) = crate::convert::sdk_event_to_tauri(ev) else {
        return true;
    };
    app.emit(&name, payload).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_event_bridge_resync_marker_maps_to_tauri_event() {
        let event = platform_event_bridge_resync_marker(3);
        let (channel, payload) =
            crate::convert::sdk_event_to_tauri(&event).expect("resync marker should convert");

        assert_eq!(channel, "im://resync_needed");
        assert_eq!(
            payload.get("scope").and_then(|v| v.as_str()),
            Some("platform_event_bridge")
        );
        assert_eq!(
            payload.get("reason").and_then(|v| v.as_str()),
            Some("platform_event_bridge_lagged")
        );
        assert_eq!(
            payload.get("droppedEvents").and_then(|v| v.as_u64()),
            Some(3)
        );
    }
}

#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_logout(
    state: State<'_, SdkState>,
) -> std::result::Result<(), super::CommandError> {
    state.logout().await.map_err(super::map_sdk_err)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_update_access_token(
    state: State<'_, SdkState>,
    access_token: String,
    tenant_id: Option<String>,
) -> std::result::Result<(), super::CommandError> {
    state
        .client()
        .update_access_token(access_token, tenant_id.as_deref())
        .await
        .map_err(super::map_sdk_err)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_is_connected(
    state: State<'_, SdkState>,
) -> std::result::Result<bool, super::CommandError> {
    Ok(state.client().is_connected().await)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_current_user_id(
    state: State<'_, SdkState>,
) -> std::result::Result<Option<String>, super::CommandError> {
    Ok(state.client().current_user_id().await)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_engine_state(
    state: State<'_, SdkState>,
) -> std::result::Result<String, super::CommandError> {
    let c = state.client();
    Ok(format!("{:?}", c.state()))
}

#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_sync_conversation(
    state: State<'_, SdkState>,
    conversation_id: String,
) -> std::result::Result<(), super::CommandError> {
    state
        .client()
        .sync_conversation(&conversation_id)
        .await
        .map_err(super::map_sdk_err)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_sync_messages(
    state: State<'_, SdkState>,
    conversation_id: String,
    last_seq: u64,
    limit: i32,
) -> std::result::Result<(), super::CommandError> {
    state
        .client()
        .sync_messages(&conversation_id, last_seq, limit)
        .await
        .map_err(super::map_sdk_err)
}

#[tauri::command(rename_all = "camelCase")]
pub async fn sdk_set_conversation_input_state(
    state: State<'_, SdkState>,
    conversation_id: String,
    is_typing: bool,
) -> std::result::Result<(), super::CommandError> {
    state
        .client()
        .set_conversation_input_state(&conversation_id, is_typing)
        .await
        .map_err(super::map_sdk_err)
}
