use anyhow::Result;
use std::{fs, path::Path};

use crate::{core_root, emit_errors, fail};

pub(crate) fn verify_multidevice_state_gate(root: &Path) -> Result<()> {
    let mut errors = Vec::new();
    let monorepo_root = root.parent().unwrap_or_else(|| Path::new(".."));
    let sdk_root = core_root(root);

    require_contains_all(
        &mut errors,
        &root.join("docs/multidevice-state-sync.md"),
        "multi-device state sync doc",
        &[
            "conversation.markRead",
            "conversation.updateDraft",
            "ReadStatesSyncTask",
            "ConversationUserSettingsSync",
            "UpdateConversationUserSettings",
            "propagation errors visible",
            "Device Presence Logout",
            "config.effective_device_id()",
            "`device_id` exactly matches",
            "Device Management",
            "list_current_user_devices",
            "kick_device",
            "Typing State",
            "TypingStatePacket",
            "Multidevice Conformance Matrix",
            "examples/multidevice_conformance.json",
            "message_fanout",
            "read_state_roaming",
            "draft_roaming",
            "device_kick",
            "typing_device_attribution",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join("examples/multidevice_conformance.json"),
        "multi-device Web/Flutter conformance manifest",
        &[
            "flare.im.examples.multidevice.conformance.v1",
            "\"clients\"",
            "\"web\"",
            "\"flutter\"",
            "\"message_fanout\"",
            "\"read_state_roaming\"",
            "\"draft_roaming\"",
            "\"device_kick\"",
            "\"typing_device_attribution\"",
            "\"webEntrypoints\"",
            "\"flutterEntrypoints\"",
            "\"observables\"",
            "\"conversation_seq\"",
            "\"ConversationUserSettingsSync.draft\"",
            "\"TypingStatePacket.device_id\"",
        ],
    );
    require_contains_all(
        &mut errors,
        // 这份一致性清单测试已从 web app 下沉进 TS SDK 包（web 桥下沉 adapters/web
        // 那轮改造的一部分）。门禁的路径没跟着改，于是一直报「文件缺失」——
        // 而整个 xtask verify 从来没进过 CI，所以没人看见。
        &root.join(
            "packages/flare-core-typescript-sdk/test/multidevice_conformance_manifest.test.ts",
        ),
        "web multi-device conformance manifest test",
        &[
            "multidevice_conformance.json",
            "message_fanout",
            "read_state_roaming",
            "draft_roaming",
            "device_kick",
            "typing_device_attribution",
            "webEntrypoints",
            "flutterEntrypoints",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "../flare-core-examples/apps/flare-core-flutter-app/test/multidevice_conformance_manifest_test.dart",
        ),
        "flutter multi-device conformance manifest test",
        &[
            "multidevice_conformance.json",
            "message_fanout",
            "read_state_roaming",
            "draft_roaming",
            "device_kick",
            "typing_device_attribution",
            "webEntrypoints",
            "flutterEntrypoints",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root.join("flare-protocol/proto/sync.proto"),
        "multi-device state sync proto",
        &[
            "ConversationUserSettingsSync conversation_user_settings = 24",
            "message ConversationUserSettingsSync",
            "optional string draft = 5",
            "message ConversationUserSettingsSyncRes",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root
            .join("flare-core-server/flare-sync-orchestrator/src/application/handlers/sync_orchestration_handler.rs"),
        "sync orchestrator conversation settings handler",
        &[
            "SyncPayload::ConversationUserSettings",
            "conversation_user_settings_sync",
            "UpdateConversationUserSettingsRequest",
            "draft: req.draft",
            "settings: resp.settings",
            "draft: req.draft.unwrap_or_default()",
            "assert_eq!(settings.draft, \"draft text\")",
            "SyncResPayload::ConversationUserSettings",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root
            .join("flare-core-server/flare-signaling/gateway/src/domain/service/sync_service.rs"),
        "signaling sync gateway settings forwarding",
        &["SyncPayload::ConversationUserSettings"],
    );

    require_contains_all(
        &mut errors,
        &sdk_root.join("src/domain/conversation/read.rs"),
        "conversation read domain service",
        &[
            "ConversationReadService",
            "plan_mark_read",
            "effective_max_seq",
            "mark_read_zero_keeps_current_read_position",
            "mark_read_non_zero_is_clamped_by_effective_max_seq",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/application/adapters/sync_protocol_adapter.rs"),
        "state sync adapter",
        &[
            "pub async fn send_read_ack",
            "push_local_read_states",
            "push_local_read_states_before_summary_sync",
            "build_read_ack",
            "ConversationUserSettingsSync",
            "request_conversation_user_settings",
            "draft: patch.draft",
            "SyncResPayload::ConversationUserSettings",
            "conversation user settings sync response missing settings",
            "conversation.draft = non_empty(&settings.draft)",
            "draft: conversation.draft.clone()",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/application/usecases/conversation/command.rs"),
        "conversation settings command strict sync",
        &[
            "push_conversation_user_settings(conversation_id, base, patch)",
            ".await?;",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/application/sync_task/read_states.rs"),
        "read states sync task",
        &[
            "ReadStatesSyncTask",
            "\"read_states\"",
            "SyncMode::Background",
            "push_local_read_states",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/application/sync_task/conversation_settings.rs"),
        "conversation settings sync task",
        &[
            "ConversationSettingsSyncTask",
            "\"conversation_user_settings\"",
            "settings_dirty",
            "push_conversation_user_settings_from_local",
            ".await?;",
        ],
    );
    forbid_contains_any(
        &mut errors,
        &sdk_root.join("src/application/sync_task/conversation_settings.rs"),
        "conversation settings sync task",
        &[".await\n                    .is_ok()"],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/client/api/conversation.rs"),
        "conversation state client API",
        &[
            "pub async fn mark_read",
            "ConversationEvent::UnreadCountChanged",
            "pub async fn update_draft",
            "publish_updated",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/application/usecases/message/mutation.rs"),
        "message mutation multi-device attribution",
        &[
            "read_ack_packet",
            "AckPayload::Read(ReadAck",
            "read ack device_id must not be empty",
            "read_ack_packet_includes_current_device_id",
            "typing_realtime_control_packet",
            "TypingStatePacket",
            "device_id: Some(device_id.to_string())",
            "typing_realtime_control_packet_includes_current_device_id",
        ],
    );
    forbid_contains_any(
        &mut errors,
        &sdk_root.join("src/domain/message/transport.rs"),
        "message mutation transport action",
        &["ReadReceipt {"],
    );
    forbid_contains_any(
        &mut errors,
        &sdk_root.join("src/application/usecases/message/transport_mapper.rs"),
        "message mutation transport mapper",
        &["EventReadReceipt", "ReadReceiptEvent"],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/client/builder.rs"),
        "client builder device identity wiring",
        &["self.config.effective_device_id()"],
    );
    // 原生端与 Web 的在线状态自 745e53f 起是同一份实现（presence/http.rs，原 web.rs；
    // native.rs 已删除），所有目标都经 Core API 网关。锚点随之只查这一份，并要求模块声明
    // 不按目标分流，否则某一端换回另一份实现时这里查不到。
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/client/api/mod.rs"),
        "presence module wiring",
        &["#[path = \"presence/http.rs\"]\nmod presence;"],
    );
    forbid_contains_any(
        &mut errors,
        &sdk_root.join("src/client/api/mod.rs"),
        "presence module wiring",
        &["presence/native.rs", "presence/web.rs"],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/client/api/presence/http.rs"),
        "presence exact device logout",
        &[
            "device_id: String",
            "current_device_presence(presence.devices, device_id)",
            "device.device_id.trim() == device_id",
            "current_device_presence_selects_exact_device_id_not_recent_device",
            "pub async fn list_current_user_devices",
            "pub async fn get_device",
            "pub async fn kick_device",
            "ConnectionQualityDto",
        ],
    );
    forbid_contains_any(
        &mut errors,
        &sdk_root.join("src/client/api/presence/http.rs"),
        "presence exact device logout",
        &[
            "max_by_key(|device| device.last_active_time_ms)",
            "最近活跃",
        ],
    );
    // 同平台新设备登录把本设备顶下线（「已在别处登录」）：上报一次 KickedOff(DEVICE_CONFLICT)、
    // 会话终止、不再自动重连，由本机回环两台同平台设备的集成测试守住。
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/infrastructure/transport/socket/socket_handler.rs"),
        "device conflict kick reported as KickedOff",
        &[
            "fn report_session_ending_kick",
            "flare_core::common::kick::is_device_conflict_reason(reason)",
            "device_conflict_disconnect_is_reported_once_as_kicked_off",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/client/im_client.rs"),
        "device conflict kick never reconnects",
        &["flare_core::common::kick::ends_session(reason)"],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("tests/device_conflict_kick.rs"),
        "device conflict kick loopback test",
        &[
            "a_device_conflict_kick_ends_the_session_without_reconnecting",
            "assert_eq!(owner_events.kicked_off(), vec![DEVICE_CONFLICT]);",
            "a kicked session must not reconnect",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join("sdk-spec/modules/conversations.json"),
        "conversation state SDK spec",
        &[
            "\"operation\": \"conversation.mark_read\"",
            "\"operation\": \"conversation.update_draft\"",
            "\"request\": \"UpdateConversationDraftRequest\"",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join("sdk-spec/models/conversations.json"),
        "conversation draft SDK model",
        &[
            "\"name\": \"UpdateConversationDraftRequest\"",
            "\"wireName\": \"conversationId\"",
            "\"wireName\": \"draft\"",
            "\"draft\"",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join("packages/flare-core-typescript-sdk/src/api/modules/conversations.ts"),
        "typescript conversation draft API type",
        &[
            "UpdateConversationDraftRequest } from '../../model';",
            "updateConversationDraft(request: UpdateConversationDraftRequest): Promise<void>",
        ],
    );
    forbid_contains_any(
        &mut errors,
        &root.join("packages/flare-core-typescript-sdk/src/api/types.ts"),
        "typescript conversation draft API type",
        &["export type UpdateConversationDraftRequest = FlareJsonObject"],
    );
    require_contains_all(
        &mut errors,
        &root.join("packages/flare-core-flutter-sdk/lib/src/adapter/codec/wire_codec.dart"),
        "flutter typed conversation draft codec",
        &[
            "Map<String, Object?> updateConversationDraftRequestToMap(",
            "UpdateConversationDraftRequest request",
            "'conversationId': request.conversationId",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join("packages/flare-core-flutter-sdk/lib/src/adapter/default_flare_im_client.dart"),
        "flutter typed conversation draft adapter",
        &[
            "Future<void> updateConversationDraft(UpdateConversationDraftRequest request)",
            "updateConversationDraftRequestToMap(request)",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join("packages/flare-core-flutter-sdk/lib/src/adapter/default_flare_im_client.dart"),
        "flutter conversation mark unread unit adapter",
        &[
            "Future<void> markConversationUnread(Map<String, Object?> request)",
            "_bridge.invoke<void>(NativeCallMap.conversationMarkUnread, request)",
        ],
    );
    forbid_contains_any(
        &mut errors,
        &root.join("packages/flare-core-flutter-sdk/lib/src/adapter/default_flare_im_client.dart"),
        "flutter conversation mark unread unit adapter",
        &["Future<Map<String, Object?>> markConversationUnread"],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module/DefaultConversationsApi.kt",
        ),
        "android typed conversation draft adapter",
        &[
            "override suspend fun updateConversationDraft(request: UpdateConversationDraftRequest): Unit",
            "updateConversationDraftRequestToMap(request)",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module/DefaultConversationsApi.kt",
        ),
        "android conversation mark unread unit adapter",
        &[
            "override suspend fun markConversationUnread(request: Map<String, Any?>): Unit",
            "invokeUnit(bridge, NativeCallMap.CONVERSATION_MARK_UNREAD, request)",
        ],
    );
    forbid_contains_any(
        &mut errors,
        &root.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module/DefaultConversationsApi.kt",
        ),
        "android conversation mark unread unit adapter",
        &["markConversationUnread(request: Map<String, Any?>): Map<String, Any?>"],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Module/DefaultConversationsApi.swift",
        ),
        "apple typed conversation draft adapter",
        &[
            "public func updateConversationDraft(_ request: UpdateConversationDraftRequest)",
            "updateConversationDraftRequestToMap(request)",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Module/DefaultConversationsApi.swift",
        ),
        "apple conversation mark unread void adapter",
        &[
            "public func markConversationUnread(_ request: [String: AnySendable]) async throws -> Void",
            "invokeVoid(bridge, descriptor: NativeCallMap.conversationMarkUnread",
        ],
    );
    forbid_contains_any(
        &mut errors,
        &root.join(
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Module/DefaultConversationsApi.swift",
        ),
        "apple conversation mark unread void adapter",
        &["markConversationUnread(_ request: [String: AnySendable]) async throws -> [String: AnySendable]"],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/module/DefaultConversationsApi.ets",
        ),
        "arkts typed conversation draft adapter",
        &[
            "async updateConversationDraft(request: UpdateConversationDraftRequest)",
            "updateConversationDraftRequestToMap(request)",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-harmony-cangjie-sdk/src/adapter/module/DefaultConversationsApi.cj",
        ),
        "cangjie typed conversation draft adapter",
        &[
            "public func updateConversationDraft(request: UpdateConversationDraftRequest): Unit",
            "updateConversationDraftRequestToJson(request)",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root
            .join("flare-core-server/flare-api-gateway/src/interface/http/presence_handler.rs"),
        "api gateway presence device management",
        &[
            "pub async fn list_user_devices",
            "pub async fn get_device",
            "pub async fn kick_device",
            "ConnectionQualityHttp",
            "ListUserDevicesRequest",
            "KickDeviceRequest",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root.join("flare-core-server/flare-api-gateway/src/interface/http/router.rs"),
        "api gateway presence device routes",
        &[
            "/users/{user_id}/devices",
            "/devices/{device_id}",
            "/devices/{device_id}/kick",
        ],
    );
    require_contains_all(
        &mut errors,
        // manager.rs 早已拆成 manager/ 目录（mod / ops / lifecycle / trait_impl / tests）。
        // 扇出这几个标记落在 trait_impl.rs 里。用户索引按（租户，用户）即 DeviceOwner 建立。
        &monorepo_root.join("flare-transport/src/server/connection/manager/trait_impl.rs"),
        "transport active multi-device fanout",
        &[
            "async fn send_to_user",
            "ConnectionManager::get_user_connections(self, owner)",
            "self.connection_handles_for_ids",
            ".for_each_concurrent(",
            "self.fanout_concurrency",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/infrastructure/persistence/sqlite/conversation_repo.rs"),
        "sqlite conversation state persistence",
        &[
            "async fn update_unread",
            "async fn update_draft",
            "last_read_seq",
            "draft",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("storage/indexeddb/src/provider.rs"),
        "indexeddb conversation state persistence",
        &[
            "async fn update_unread",
            "async fn update_draft",
            "last_read_seq",
            "draft",
        ],
    );

    emit_errors("multi-device state gate", errors)
}

fn require_contains_all(errors: &mut Vec<String>, path: &Path, label: &str, needles: &[&str]) {
    let Ok(text) = fs::read_to_string(path) else {
        fail(
            errors,
            format!("{label} missing or unreadable: {}", path.display()),
        );
        return;
    };

    for needle in needles {
        if !text.contains(needle) {
            fail(
                errors,
                format!("{label} missing `{needle}` in {}", path.display()),
            );
        }
    }
}

fn forbid_contains_any(errors: &mut Vec<String>, path: &Path, label: &str, needles: &[&str]) {
    let Ok(text) = fs::read_to_string(path) else {
        fail(
            errors,
            format!("{label} missing or unreadable: {}", path.display()),
        );
        return;
    };

    for needle in needles {
        if text.contains(needle) {
            fail(
                errors,
                format!(
                    "{label} contains forbidden `{needle}` in {}",
                    path.display()
                ),
            );
        }
    }
}
