# Flare IM Core SDK Tauri Binding

`bindings/tauri` is a thin IPC adapter over the shared binding runtime. It does
not own IM behavior, event mapping, storage, sync, or transport semantics.

## Contract

Canonical API, event, and error contracts live in:

```text
../contract/apis.json
../contract/events.json
../contract/errors.json
../contract/client_config.json
```

Run `rtk cargo xtask core-codegen` after changing contract JSON.

## Shape

```text
bindings/tauri/
  src/
    lib.rs          sdk_contract_json / sdk_init / sdk_invoke_json commands
    generated/      generated contract metadata; not the behavior source
```

The active Tauri surface creates an `IMClient::new()` shell with `sdk_init`,
then forwards canonical API IDs and raw request JSON through
`flare_im_core_sdk_bindings_runtime::invoke_api_id_json`.

## Usage

```rust
tauri::Builder::default()
    .manage(flare_im_core_sdk_tauri::SdkState::default())
    .invoke_handler(flare_im_core_sdk_tauri::im_invoke_handler())
    .run(tauri::generate_context!())?;
```

```ts
await invoke("sdk_init", {
  args: {
    config: {
      environment: "development",
      sdkConfig: {
        access: {
          trustMode: "development",
          developmentEndpoints: {
            wsUrl: "ws://localhost:60051",
            coreApiUrl: "http://localhost:50050"
          }
        }
      }
    }
  }
});

const response = await invoke("sdk_invoke_json", {
  apiId: "connection.get_state",
  requestJson: "{}"
});
```

## Rules

- Do not implement IM business rules in this crate.
- Do not restore the old command tree or `SdkEvent -> im://*` converter without
  first wiring it to the current `SdkEvent` and `events.json` contract.
- Plugin realtime packets and plugin catalog changes reach the app as the
  typed `im://plugin_realtime` and `im://plugin_catalog_changed` events.
