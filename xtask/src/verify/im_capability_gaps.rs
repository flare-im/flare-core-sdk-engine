use anyhow::Result;
use std::{fs, path::Path};

use crate::{emit_errors, fail};

pub(crate) fn verify_e2ee_contract_gate(root: &Path) -> Result<()> {
    let mut errors = Vec::new();
    let monorepo_root = root.parent().unwrap_or_else(|| Path::new(".."));
    let sdk_root = monorepo_root.join("flare-core-sdk-engine");
    let server_root = monorepo_root.join("flare-core-server");

    require_contains_all(
        &mut errors,
        &root.join("docs/e2ee-contract.md"),
        "E2EE contract doc",
        &[
            "conversation_encryption",
            "E2EE_PLACEHOLDER_REASON",
            "E2eeKeyManager",
            "ContentCodec",
            "Getui",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/extension/encryption.rs"),
        "SDK E2EE core",
        &[
            "pub trait E2eeKeyManager",
            "VolatileE2eeKeyManager",
            "E2EE session establishment requires a concrete crypto provider",
            "upsert_session",
            "KeyManagedConversationEncryptionPolicyResolver",
            "session_for_conversation",
            "pub enum EncryptionTier",
            "E2e",
            "ContentEncryptionInterceptor",
            "E2EE_PLACEHOLDER_REASON",
            "PlaceholderContent",
            "encrypted_content_envelope_from_bytes",
            "E2EE session is not established for conversation",
            "E2EE content codec returned empty ciphertext",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/client/builder.rs"),
        "SDK builder E2EE wiring",
        &[
            "pub fn conversation_encryption",
            "ContentEncryptionInterceptor::new",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/lib.rs"),
        "SDK E2EE exports",
        &[
            "E2eeKeyManager",
            "VolatileE2eeKeyManager",
            "KeyManagedConversationEncryptionPolicyResolver",
            "ConversationEncryptionPolicy",
            "encrypted_content_envelope_from_bytes",
        ],
    );
    require_contains_all(
        &mut errors,
        &server_root.join("flare-push/worker/src/infrastructure/getui_push.rs"),
        "push E2EE privacy",
        &[
            "const E2EE_PLACEHOLDER_REASON",
            "message_has_e2ee_placeholder",
            "e2ee_message_ignores_plain_offline_push_info",
        ],
    );
    // 这一条原先和上面挤在 getui_push.rs 里，只查 `ContentVisibility::Hidden`。
    // 可见性判定后来被抽到 push_display.rs，getui 那边就再也搜不到那个字符串，
    // 门禁一直报红——但**实现是对的**，不是隐私缺陷：
    // message_requires_generic_push_display 同时覆盖 Hidden / Redacted / Purged
    // 以及 E2EE 占位符。所以这里跟着搬家，并且把三个状态**都**点名——
    // 原来只查 Hidden，漏掉任何一个都发现不了。
    require_contains_all(
        &mut errors,
        &server_root.join("flare-push/worker/src/infrastructure/push_display.rs"),
        "push content visibility",
        &[
            "fn message_requires_generic_push_display",
            "ContentVisibility::Hidden",
            "ContentVisibility::Redacted",
            "ContentVisibility::Purged",
            "message_has_e2ee_placeholder(message)",
        ],
    );

    emit_errors("e2ee contract gate", errors)
}

pub(crate) fn verify_channel_capability_gate(root: &Path) -> Result<()> {
    let mut errors = Vec::new();
    let monorepo_root = root.parent().unwrap_or_else(|| Path::new(".."));
    let sdk_root = monorepo_root.join("flare-core-sdk-engine");
    let server_root = monorepo_root.join("flare-core-server");

    require_contains_all(
        &mut errors,
        &root.join("docs/channel-capability.md"),
        "channel capability doc",
        &[
            "ConversationType::Channel",
            "ConversationType::Broadcast",
            "subscriber model",
            "large_conversation",
            "notify+pull",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/model/conversation.rs"),
        "SDK conversation type policy",
        &[
            "ConversationType::Channel",
            "ConversationType::Broadcast",
            "wire_name: \"channel\"",
            "wire_name: \"broadcast\"",
            "cid_prefix: Some(\"7\")",
            "cid_prefix: Some(\"8\")",
            "subscriber_model: true",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join("sdk-spec/models/conversations.json"),
        "sdk-spec conversation enum",
        &["\"channel\"", "\"broadcast\""],
    );
    require_contains_all(
        &mut errors,
        &root.join("packages/flare-core-typescript-sdk/src/model/conversation_type.ts"),
        "TypeScript conversation enum",
        &["Channel = \"channel\"", "Broadcast = \"broadcast\""],
    );
    let conversation_service =
        server_root.join("flare-conversation/src/domain/service/conversation_domain_service.rs");
    require_contains_all(
        &mut errors,
        &conversation_service,
        "server channel conversation creation",
        &[
            "required_route_id",
            "generate_channel_conversation_id",
            "generate_broadcast_conversation_id",
            "channel conversation requires channel_id",
            "broadcast conversation requires broadcast_id",
        ],
    );
    forbid_contains_any(
        &mut errors,
        &conversation_service,
        "server channel conversation creation",
        &[
            "Unspecified session type, using UUID for conversation_id (backward compatibility)",
            ".unwrap_or_else(|| Uuid::new_v4().to_string());\n                    generate_channel_conversation_id",
            ".unwrap_or_else(|| Uuid::new_v4().to_string());\n                    generate_broadcast_conversation_id",
        ],
    );
    require_contains_all(
        &mut errors,
        &server_root.join("flare-orchestrator/src/domain/service/message_fanout_service.rs"),
        "large conversation fanout",
        &["large_conversation", "push_only_message_ping", "Vec::new()"],
    );

    emit_errors("channel capability gate", errors)
}

pub(crate) fn verify_media_processing_gate(root: &Path) -> Result<()> {
    let mut errors = Vec::new();
    let monorepo_root = root.parent().unwrap_or_else(|| Path::new(".."));
    let sdk_root = monorepo_root.join("flare-core-sdk-engine");
    let server_root = monorepo_root.join("flare-core-server");

    require_contains_all(
        &mut errors,
        &root.join("docs/media-processing.md"),
        "media processing doc",
        &[
            "MediaProcessorPort",
            "prepare_upload",
            "thumbnail",
            "transcode",
            "blurhash",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/platform/ports/media.rs"),
        "SDK media processing port",
        &[
            "pub trait MediaProcessorPort",
            "async fn inspect",
            "async fn prepare_upload",
            "pub struct ProcessedMedia",
            "pub payload: Option<Vec<u8>>",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/platform/adapters/media/upload_only.rs"),
        "SDK upload service processing chain",
        &[
            "UploadOnlyMediaService::with_processor",
            "prepare_upload(",
            "upload_only_service_runs_processor_before_uploader",
        ],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/platform/runtime/mod.rs"),
        "SDK media processor runtime wiring",
        &["pub media_processor", "pub fn with_media_processor"],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/client/builder.rs"),
        "SDK media processor builder wiring",
        &["media_processor", "UploadOnlyMediaService::with_processor"],
    );
    require_contains_all(
        &mut errors,
        &server_root.join("flare-media/src/infrastructure/media_processor.rs"),
        "server image media processor",
        &["compress_image", "generate_thumbnail", "thumbnail"],
    );
    require_contains_all(
        &mut errors,
        &server_root.join("flare-media/src/application/handlers/command_handler.rs"),
        "server media processing command handler",
        &[
            "process_image_compress",
            "process_image_thumbnail",
            "process_video_transcode",
            "process_video_thumbnail",
            "process_video_compress",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join("sdk-spec/models/message_content_elems.json"),
        "media content spec",
        &[
            "ImageContentPayload",
            "\"thumbnail\"",
            "\"blurhash\"",
            "VideoContentPayload",
            "\"cover\"",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root.join("flare-protocol/proto/message_content.proto"),
        "media proto blurhash contract",
        &["string blurhash = 10"],
    );
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/content/message_elem.rs"),
        "SDK image info blurhash mapping",
        &[
            "pub blurhash: String",
            "blurhash: i.blurhash.clone()",
            "blurhash: e.blurhash.clone()",
        ],
    );

    emit_errors("media processing gate", errors)
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
                format!("{label} must not contain `{needle}` in {}", path.display()),
            );
        }
    }
}
