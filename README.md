# Flare IM Core SDK

English · **[中文](README.zh-CN.md)**

> ## ⚠ Clone this next to its sibling repos

This crate depends on `flare-core`, `flare-proto` and `flare-grpc-proto` by **path**
(`../flare-transport`, …). Cloning this repository on its own does not build — the failure
happens in `cargo metadata`, before it ever reaches your code, so the error names an
internal workspace member and says nothing about the real cause.

```bash
mkdir flare && cd flare
git clone https://github.com/flare-im/flare-transport.git
git clone https://github.com/flare-im/flare-protocol.git
git clone https://github.com/flare-im/flare-protocol-grpc.git
git clone https://github.com/flare-im/flare-core-sdk-engine.git
cd flare-im-core-sdk && cargo check
```

> ## ℹ This is communication infrastructure, not a turnkey IM product
>
> Up front, so you don't discover it only after cloning and failing to log in:
> **the open-source part ships no account system** (no sign-up/login, friend
> relationships, group roles/approval/muting, or moments/feed).
>
> What it does ship is a complete, pluggable authentication contract — both
> paths live on the open-source side:
>
> - **`CoreJwtTokenValidator`** — validates JWTs locally. Hand-sign a token and
>   you can run a demo / POC **without any user system at all**.
> - **`HttpHookTokenValidator`** — POSTs the token to your own endpoint. **This
>   is the entry point for wiring in your own user system.**
>
> Business rules work the same way: `flare-core-server/crates/flare-im-hooks`
> exposes 9 extension points (PreSend / PostSend / Delivery / Recall /
> MessageRead / MessageReaction / ConversationLifecycle / ConversationMember /
> GetConversationParticipants).
>
> To go to production you implement your own user system and plug it in via the
> contracts above — the same "bring your own identity" model as Sendbird /
> Twilio Conversations, except Flare can be self-hosted and its protocol and
> core are auditable.
>
> For the exact boundary, see [GOVERNANCE.md](.github/GOVERNANCE.md).


[![Rust](https://img.shields.io/badge/rust-1.94+-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

`flare-im-core-sdk` is Flare IM's unified client core. The Rust core owns the
SDK lifecycle, local message/conversation projection, the sync-task entry
points, the event system, media records, capability packs, and extension
events; the C, Tauri, and Wasm bindings only translate the
ABI / IPC / language boundary.

## Core boundaries

| Layer | Responsibility |
| --- | --- |
| `flare-core` | Transport, connection, framing, negotiation, heartbeat, and other base communication primitives |
| `flare-proto` | The single wire contract — `DataPacket`, `Message`, `SyncRes`, `CapabilityPacket`, etc. |
| `flare-im-core-sdk` | Client IM behavior, offline local state, projection, the outbound queue, event routing, extension-capability entry points |
| bindings | Boundary adapters for C/Tauri/Wasm — they never re-implement core semantics |

## Current production modules

`src/lib.rs` re-exports only the current public contract. SDK behavior is placed
by boundary across `client`, `application`, `domain`, `core`,
`infrastructure`, `platform`, and `extension`; the old prototype route facade
is no longer the production contract.

| Module | Responsibility |
| --- | --- |
| `client/` | `IMClient` lifecycle, typed APIs, builder, login session, the cross-platform SDK entry |
| `application/` | Use-case orchestration, message/sync/presence/capability adapters, projection updates |
| `domain/` | Business invariants — messages, conversations, sync cursors, pending sends |
| `core/` | Event bus, dispatcher, reliable queue, sync orchestrator |
| `infrastructure/` | protobuf codec, packet sender, socket/http transport, memory/sqlite persistence |
| `platform/` | Media, transport, and host-capability ports |
| `extension/` | Capability registry, plugin UI and pages, middleware |

## Installation

```toml
[dependencies]
# `LoginDbKind::Sqlite` 由 lifecycle-sqlite 提供；不开这个 feature，下面的样例编不过。
flare-im-core-sdk = { version = "1.2", features = ["lifecycle-sqlite"] }
tokio = { version = "1", features = ["full"] }
```

Build prerequisite: `protoc`. The dependency chain compiles `.proto` files at build time.

```bash
# Debian / Ubuntu
sudo apt-get install -y protobuf-compiler
# macOS
brew install protobuf
```

## Quick start

> Client apps must not mint tokens: set `SdkConfig::auth.token_endpoint` (builder `auth_token_endpoint`) so the SDK issues and refreshes via the gateway's `/api/v1/auth/tokens`, or pass a token issued by your backend. `generate_core_token` stays only for server-side tooling and tests (see flare-core-server/docs/AUTH-TOKEN-ISSUANCE.md).

```rust
use flare_im_core_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let client = IMClient::new();
    client.init(Some("example".into()), None).await?;

    let token = IMClient::generate_core_token(CoreTokenConfig {
        secret: "insecure-secret".to_string(),
        issuer: "flare-im-core".to_string(),
        user_id: "user_a".to_string(),
        ttl_secs: 3600,
        device_id: None,
        tenant_id: Some("0".to_string()),
    })?;
    let apis = client
        .login("user_a", Some(&token), LoginDbKind::Sqlite, |_, _| {})
        .await?;

    let conversation = apis
        .conversation_api
        .get_one("peer_b", &flare_im_core_sdk::model::conversation::ConversationType::Single)
        .await?;
    let message = apis
        .message_build_api
        .create_text(&conversation.conversation_id, "hello", false, &[])
        .await?;
    let ack = apis.message_api.send_no_oss(message).await?;
    println!("sent: {:?}", ack);
    Ok(())
}
```

## Binding runtime

Bindings all enter the current typed SDK through the `BindingRequest` of the
`bindings/shared` crate. This shared runtime only wires up routes that have been
explicitly migrated to vNext; the old prototype JSON routes are not revived for
compatibility.

```rust
let response = flare_im_core_sdk_bindings_runtime::invoke_json(
    &client,
    r#"{"route":"sdk.state","params":{}}"#,
).await;
```

Contract sources live in `bindings/contract/*.json`; after editing them, run
`rtk cargo xtask codegen` to regenerate the tables.

Read messages from JSON through `IMMessage::from_json_str` /
`IMMessage::from_json_value`, and message content on its own through
`Elem::from_json_value`, rather than with a `serde_json` call of your own. They
read exactly as `serde_json` does; being compiled once in this crate, they keep a
single copy of the (recursive) message reader in builds that link several crates,
such as the browser bundle.

## Event system

Events uniformly use `SdkEvent`:

- `EventBus::subscribe()` provides a typed Rust event stream.
- Message-mutation events come from the `event.proto` oneof payload;
  typing/presence do not consume `conversation_seq` and travel over the DATA
  realtime channel.
- Plugin realtime packets (`DataPacket.capability`, `im.realtime.deliver`)
  surface as `SdkEvent::Plugin(PluginEvent::Realtime)`: expired packets and
  payloads that are not JSON objects are dropped. The tenant's
  `flare.plugin.catalog` / `changed` notice invalidates the plugin catalog and
  surfaces as `PluginEvent::CatalogChanged` after a random 0-3 s delay, with
  notices within 3 s merged. Bindings emit them as `plugin.realtime` (7001,
  `im://plugin_realtime`) and `plugin.catalog_changed` (7002,
  `im://plugin_catalog_changed`).
- `bindings/contract/events.json` maintains cross-platform event ids, C codes,
  and `im://*` names.
- Custom business events go through `ExtensionEvent` or `MessageEvent::Custom`;
  their payload bytes are opaque to the core.

## Outbox and sync

- On send, the message is first written to a bounded pending outbox; while
  offline it is only enqueued, and it is sent immediately only once `Ready` and
  the transport is connected.
- Both `SendAck` accepted and error converge the local message state and remove
  the pending entry.
- When a realtime downstream message reveals a `conversation_seq` gap, the gap
  is recorded and back-filled via a single-conversation sync request.
- Conversation-list pagination uses the server's string cursor; the local
  adapter only parses the numeric watermark when needed.
- `SyncRes` / `EventEnvelope` fields are currently authoritative on
  `max_conversation_seq`, `next_cursor`, and the oneof payload.

### Operations outside the outbox while disconnected

Only message sends go through the outbox. Recall, edit, delete, reactions,
pin/mark, read receipts (`mark_read`, `mark_read_and_burn`), sync requests
and custom data are sent directly: they are not queued and
not retried. While the long connection is down (including while the reconnect
watcher is still backing off) they fail at once instead of reconnecting; the
watcher is the only reconnect path. Branch on the error code, not the message:

| Code | Meaning | Suggested app handling |
| --- | --- | --- |
| `NOT_CONNECTED` (C ABI `FLARE_ERR_NOT_CONNECTED`) | No usable connection: disconnected, reconnecting, or the server ended the session (`DEVICE_CONFLICT` / `TENANT_UNAVAILABLE`, reported as `KickedOff`) | Show "not connected"; retry after the state returns to `Ready` |
| `OPERATION_TIMEOUT` (C ABI `FLARE_ERR_OPERATION_TIMEOUT`) | The send did not complete in time | Show a timeout; the server may still have applied it, so refresh before retrying |
| `CONNECTION_FAILED` (sync requests: `GENERAL_ERROR`; the C ABI currently reports both as `FLARE_ERR_INTERNAL`) | The write failed, e.g. the connection dropped mid-send | Treat as a transient failure; retry after `Ready` |

Typing indicators are best effort: while disconnected they are dropped and the
call still returns `Ok`.

## Bindings

| Binding | Directory | Verification |
| --- | --- | --- |
| C ABI | `bindings/c` | `cargo check -p flare-im-core-sdk-ffi --all-targets` |
| Tauri | `bindings/tauri` | `cargo check -p flare-im-core-sdk-tauri` |
| Archived UniFFI placeholder | `bindings/uniffi` | Excluded from the workspace and production verification. Native clients use C ABI. |
| Wasm | `bindings/wasm` | `cargo check -p flare-im-core-sdk-wasm --target wasm32-unknown-unknown` |

## Development verification

```bash
rtk cargo fmt --all -- --check
rtk cargo check --workspace --all-features --all-targets
rtk cargo test --workspace --all-features
rtk cargo check -p flare-im-core-sdk-wasm --target wasm32-unknown-unknown --all-features
rtk cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Server integration testing

The local full-stack server is started by
`../flare-core-server/scripts/start_server.sh` and can be checked with
`../flare-core-server/scripts/check_services.sh`. The current live-gateway E2E needs
to be rebuilt on top of `SocketTransport` / `PacketSender` / the typed API; do
not restore the old `memory://local` route-facade tests.

```bash
rtk bash ../flare-core-server/scripts/check_services.sh
```

## License

Apache License 2.0, same as the underlying `flare-core` — see the license file in
its repository.

---

## Next steps

| What you want | Where to go |
|---|---|
| **Run it in five minutes** | [QUICKSTART](https://github.com/flare-im/flare-core-server/blob/main/QUICKSTART.md) — start the server, hand-sign a token, get a call through, **no self-built user system required** |
| Wire in your own user system | Implement `TokenValidator` (`CoreJwtTokenValidator` for local verification / `HttpHookTokenValidator` to call your endpoint) |
| Add your own business rules | The 9 `flare-im-hooks` extension points: PreSend / PostSend / Delivery / Recall / MessageRead / MessageReaction / ConversationLifecycle / ConversationMember / GetConversationParticipants |
| Build a UI | [`@flare-im/vue-ui`](https://www.npmjs.com/package/@flare-im/vue-ui) — 107 components, one contract consistent across four platforms |
| Report a security issue | [SECURITY.md](.github/SECURITY.md) — **please do not open a public issue** |

## When you need an account system and social features

The open-source part is **communication infrastructure**. If what you need is a
ready-made account system, friend relationships, group governance (roles /
join approval / muting), or a moments/feed, those live in the commercial
modules — building this layer yourself usually takes months and is all
communication-unrelated, repetitive work.

Enterprise scenarios additionally cover SSO / org directory / audit export /
data residency / SLA support.

Inquiries: `flare1522@163.com`

> For the boundary and its immutable guarantees, see
> [GOVERNANCE](https://github.com/flare-im/flare-core-server/blob/main/GOVERNANCE.md).
> In short: **what is already open-sourced will not be taken back, and the auth
> and hooks contracts stay open forever — they will never be crippled to force
> payment.**
