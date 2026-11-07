use anyhow::Result;
use std::{fs, path::Path};

use crate::{core_root, emit_errors, fail};

pub(crate) fn verify_network_reconnect_gate(root: &Path) -> Result<()> {
    let mut errors = Vec::new();
    let sdk_root = core_root(root);

    require_contains_all(
        &mut errors,
        &root.join("docs/client-network-reconnect.md"),
        "network change doc",
        &[
            "client.connection.notifyNetworkChange",
            "connection.notify_network_change",
            "fingerprintHint",
            "lifecycle.setAppState",
            "link supervisor",
        ],
    );

    // The link supervisor owns reconnecting; a network change only updates the fingerprint and
    // tells the supervisor.
    require_contains_all(
        &mut errors,
        &sdk_root.join("src/client/im_client/session_lifecycle.rs"),
        "network change runtime",
        &[
            "pub async fn notify_network_change",
            "access.set_network(",
            "link.network_changed(event.available, changed)",
        ],
    );

    require_contains_all(
        &mut errors,
        &sdk_root.join("bindings/contract/direct_invoke.json"),
        "network change direct invoke contract",
        &[
            "\"route\": \"connection.notify_network_change\"",
            "NetworkChangeEvent",
            "fingerprint_hint: dispatch_support::optional_string(request, \\\"fingerprintHint\\\")",
            "client.notify_network_change(event).await",
        ],
    );

    require_contains_all(
        &mut errors,
        &root.join("sdk-spec/modules/connection.json"),
        "network change SDK API contract",
        &[
            "\"name\": \"notifyNetworkChange\"",
            "\"operation\": \"connection.notify_network_change\"",
            "\"request\": \"NetworkChangeRequest\"",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join("sdk-spec/models/connection.json"),
        "network change SDK model contract",
        &[
            "\"name\": \"NetworkChangeRequest\"",
            "\"name\": \"NetworkInterfaceKind\"",
            "\"wifi\"",
            "\"cellular\"",
            "\"ethernet\"",
            "\"name\": \"available\"",
            "\"name\": \"interface\"",
            "\"type\": \"NetworkInterfaceKind\"",
            "\"name\": \"fingerprintHint\"",
        ],
    );
    require_absent(
        &mut errors,
        &root.join("sdk-spec/models/connection.json"),
        "network change SDK model contract",
        &["NetworkChangeResponse", "\"reconnected\""],
    );

    require_contains_all(
        &mut errors,
        &root.join("packages/flare-core-flutter-sdk/lib/src/adapter/codec/wire_codec.dart"),
        "Flutter network change codec",
        &[
            "Map<String, Object?> networkChangeRequestToMap(NetworkChangeRequest request)",
            "'interface': request.interface!.name",
            "'fingerprintHint': request.fingerprintHint!",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-apple-sdk/Sources/FlareCoreAppleSDK/Adapter/Codec/WireCodec.swift",
        ),
        "Apple network change codec",
        &[
            "func networkChangeRequestToMap(_ request: NetworkChangeRequest)",
            "map[\"interface\"] = wrapSendable(interface.rawValue)",
            "map[\"fingerprintHint\"]",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/model/command/NetworkChangeRequest.kt",
        ),
        "Android network change model",
        &["val `interface`: NetworkInterfaceKind? = null"],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/codec/WireCodec.kt",
        ),
        "Android network change codec",
        &[
            "fun networkInterfaceKindWireValue(value: NetworkInterfaceKind): String",
            "request.`interface`?.let { put(\"interface\", networkInterfaceKindWireValue(it)) }",
            "request.fingerprintHint?.let { put(\"fingerprintHint\", it) }",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-android-sdk/src/main/kotlin/com/flare/im/adapter/module/DefaultConnectionApi.kt",
        ),
        "Android network change adapter",
        &["override suspend fun notifyNetworkChange(request: NetworkChangeRequest): Unit"],
    );
    require_contains_all(
        &mut errors,
        &root
            .join("packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/codec/WireCodec.ets"),
        "ArkTS network change codec",
        &[
            "export function networkChangeRequestToMap(request: NetworkChangeRequest)",
            "interface: request.interface",
            "fingerprintHint: request.fingerprintHint",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-harmony-arkts-sdk/src/main/ets/adapter/module/DefaultConnectionApi.ets",
        ),
        "ArkTS network change adapter",
        &["async notifyNetworkChange(request: NetworkChangeRequest): Promise<void>"],
    );
    require_contains_all(
        &mut errors,
        &root.join("packages/flare-core-harmony-cangjie-sdk/src/adapter/codec/WireCodec.cj"),
        "Cangjie network change codec",
        &[
            "public func networkInterfaceKindWireValue(value: NetworkInterfaceKind): String",
            "jsonPutString(json, \"interface\", networkInterfaceKindWireValue(value))",
            "jsonPutString(json, \"fingerprintHint\", value)",
        ],
    );
    require_contains_all(
        &mut errors,
        &root.join(
            "packages/flare-core-harmony-cangjie-sdk/src/adapter/module/DefaultConnectionApi.cj",
        ),
        "Cangjie network change adapter",
        &["public func notifyNetworkChange(request: NetworkChangeRequest): Unit"],
    );
    // Browser runtimes report reachability from the platform lifecycle installed at init.
    require_contains_all(
        &mut errors,
        &root.join("packages/flare-core-typescript-sdk/src/lifecycle/platformLifecycle.ts"),
        "TypeScript platform lifecycle",
        &[
            "connection.notifyNetworkChange",
            "fingerprintHint",
            "\"online\"",
            "\"offline\"",
            "lifecycle.setAppState",
        ],
    );

    require_contains_all(
        &mut errors,
        &sdk_root.join("src/client/im_client/tests.rs"),
        "network change regression test",
        &["network_change_without_session_only_updates_the_fingerprint"],
    );

    emit_errors("network reconnect gate", errors)
}

fn require_absent(errors: &mut Vec<String>, path: &Path, label: &str, needles: &[&str]) {
    let Ok(text) = fs::read_to_string(path) else {
        fail(
            errors,
            format!("{label} missing or unreadable: {}", path.display()),
        );
        return;
    };
    for needle in needles {
        if text.contains(needle) {
            fail(errors, format!("{label} still contains removed `{needle}`"));
        }
    }
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
