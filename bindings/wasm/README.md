# Flare IM Core SDK WASM Binding

This package is the browser L1 binding consumed by
`flare-core-typescript-sdk/web`.

It exports:

- `createWasmRuntime()`
- `FlareImWasmRuntime.invoke(operation, requestJson)`

Build:

```bash
cd flare-core-sdk-engine/bindings/wasm
npm install
npm run build
```

The generated package is written to `bindings/wasm/pkg`.

The default `production-runtime` feature uses the real `IMClient`, browser
WebSocket transport, shared contract dispatch, and the Web storage host bridge.
The `local-smoke-runtime` feature remains available for isolated contract smoke
tests; it must not become a second implementation of IM behavior.

The Web storage bridge is currently stage-1 durability: Rust keeps the hot path
in memory and asks the JavaScript IndexedDB host to load/save snapshots. This is
good enough for browser integration, but it is not yet a full IndexedDB
repository with SQLite-equivalent transaction semantics.

The thin wasm adapter itself must also compile with `--no-default-features`;
`cargo check -p flare-im-core-sdk-wasm --target wasm32-unknown-unknown --no-default-features` verifies that boundary.

## Binding Design Rule

`bindings/wasm` must stay a thin platform adapter. Its responsibilities are:

- initialize wasm-bindgen exports
- translate `(operation, requestJson)` into a shared core runtime call
- map core errors into stable JS errors
- expose browser-specific lifecycle/dispose hooks

Conversation, message, sync, delivery, read-state, media, presence, and capability
behavior should live in shared SDK crates, not in the wasm adapter. If behavior
is missing in the browser runtime, add a shared facade/port first, then route
this binding to that facade. The in-memory runtime in this package is only a
temporary smoke layer until the shared core facade is wasm-ready.
