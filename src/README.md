# Flare IM Core SDK Source Layout

`src` 按长期生产架构分层。开发阶段不保留兼容 facade；新代码直接使用下面的 canonical path。

## Top-Level Layers

```text
client/             public SDK facade, lifecycle, builder, connected APIs
application/        command/query/usecase orchestration, sync tasks, projections
domain/             business-neutral IM domain models, invariants, repository traits
core/               engine, dispatcher, event bus, FSM, reliable delivery, sync
platform/           platform ports, runtime assembly, media/storage adapters
infrastructure/     concrete protocol, socket/http clients, persistence implementations
model/              serializable SDK models, content decoding/building, RichDoc v2
extension/          business extension registration, capability plugins, middleware
shared/             error contracts, typed IDs, config helpers, utility functions
```

## Core Runtime Layer

```text
core/
  engine.rs         SDK engine composition and connection lifecycle
  dispatcher.rs     downlink packet routing into core/application events
  event/            SDK event bus and public event contracts
  fsm/              connection/message/sync state machines
  reliable_queue/   durable outbound message delivery actor
  sync/             sync manager, orchestration, progress and task contracts
```

`core` owns long-lived runtime state and failure recovery mechanisms. `application`
may enqueue work into core runtime systems, but platform-specific storage,
networking, media, and runtime decisions still enter through `platform` and
`infrastructure`.

## Platform Layer

```text
platform/
  ports/            stable contracts consumed by core/application/client
  adapters/         concrete or host-bridge adapters for media/storage
  runtime/          PlatformKind, RuntimeConfig, RuntimeComponents, assemblers
```

Media and storage differences for Web, React Native, uni-app, Android, iOS, and Flutter belong here. Business code should consume `MediaServicePort`, `StoreProvider`, and `RuntimeComponents`; it should not branch on platform names.

Adapter profiles use one shared platform taxonomy:

- `AdapterPlatform`: coarse host family for Web/RN/uni-app/native/mobile runtimes.
- `MediaAdapterProfile`: media input sources plus upload/cache provisioning mode.
- `StorageAdapterProfile`: preferred storage backend plus store provisioning mode.

Do not create separate media/storage platform enums. Platform identity is shared;
media and storage only describe different capabilities for that same host family.

## Application Layer

```text
application/
  commands/         state-changing command objects
  queries/          read/query request objects
  usecases/         business-neutral orchestration for messages, conversations, sync
  sync_task/        built-in IM sync tasks registered by the client builder
  notification/     inbound notification pipeline and handler registry
  projections/      local read model projection and display materialization
  services/         application-level dedupe, convergence, and message building
  lifecycle/        local lifecycle operations that are not transport lifecycle
  callbacks/        progress and host callback contracts
  adapters/         protocol adapters used by application orchestration
```

`application` may depend on domain repositories and `platform::ports`, but not on concrete platform adapters or infrastructure clients.

## Extension Layer

```text
extension/
  mod.rs            external SDK extension registry
  capability/       optional capability/plugin model, e.g. RTC/call integration
  middleware/       platform-neutral message/event middleware pipeline
```

Business SDKs such as `flare-im-sdk` should install sync tasks, middleware,
and capability plugins through `extension`. Core IM behavior remains in
`domain`, `application`, and `core`.

## Model Layer

```text
model/
  message.rs        canonical SDK message model
  message_elem.rs   typed message content elements
  content_builder.rs / message_builder.rs
                    strongly typed content and message builders
  rich_doc_v2/      RichDoc v2 normalization, validation, derived fields
```

RichDoc v2 lives under `model` because it is part of the message content model,
not a standalone SDK subsystem.

## Client API Layer

```text
client/
  api/
    message.rs
    conversation.rs
    media.rs
    capability.rs
    message_build.rs
    presence/
      native.rs     gRPC-backed native presence facade
      web.rs        Web/WASM unsupported/host-adapter facade
  profile_center.rs user-facing profile center contract
```

Each public facade keeps one canonical API name. Platform variants live under the facade directory instead of introducing platform-specific public method names.

## Shared Layer

```text
shared/
  error.rs          FlareError, ErrorCode, Result
  types.rs          strongly typed shared IDs
  config/           cross-cutting configuration module
  util/             paths, IDs, dates, token helpers, SQLite opening helpers
```

Use `crate::shared::error`, `crate::shared::types`, `crate::shared::util`, and `crate::shared::config` directly.

## Boundary Rules

- `domain` must not depend on `infrastructure` or platform adapters.
- `application` orchestrates use cases and depends on domain repositories plus platform ports only.
- `client::builder` is the composition root for `IMClient`; it wires runtime components into application use cases and facades.
- `infrastructure` implements concrete persistence, protocol, HTTP, and socket details.
- `platform::adapters` is for platform IO differences, not business policy.
- Stable core semantics use typed fields and enums, not `metadata`.
- Event callbacks must be removable by dropping `Subscription`; never add callback lists that cannot be compacted.
- Hot-path dedupe keys should be strongly typed keys, not formatted strings.

## Domain Conversation IDs

CID generation and validation live in `domain/conversation/id.rs`.
