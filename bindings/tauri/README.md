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
        endpoints: {
          baseUrl: "http://localhost:50050",
          websocketUrl: "ws://localhost:60051"
        }
      }
    }
  }
});

const response = await invoke("sdk_invoke_json", {
  apiId: "connection.get_status",
  requestJson: "{}"
});
```

## Rules

- Do not implement IM business rules in this crate.
- Do not restore the old command tree or `SdkEvent -> im://*` converter without
  first wiring it to the current `SdkEvent` and `events.json` contract.
- Plugin realtime packets and plugin catalog changes reach the app as the
  typed `im://plugin_realtime` and `im://plugin_catalog_changed` events.

## Generated file saves

The handwritten Tauri host adapter exposes `sdk_stage_generated_file({ bytesBase64 })`
and `sdk_cleanup_generated_file({ token })`. Staging returns `{ token, path, sizeBytes }`:
an App-owned, randomly named file under the private `generated-files` cache directory.
Neither command accepts a JavaScript destination or deletion path. Writes and cleanup
run off the UI thread; files are limited to 8 MiB and 16 outstanding tokens per App.
Entries older than 15 minutes are reclaimed on a subsequent staging request. Registered
files are also removed when their App state is dropped; abrupt process termination can
leave private cache files, so this is not a persistent transfer journal.

For generated PNGs, keep rendering in the UI kit and use the reusable TypeScript adapter:

```ts
import { createTauriGeneratedFileSaver } from '@flare-im/sdk/tauri';

// Supply the current SDK's existing typed save operation, for example
// request => sdk.media.saveToDevice(request).
const saveFile = createTauriGeneratedFileSaver(saveToDevice);
await saveFile(pngBlob, 'flare-code.png', { signal });
```

The adapter stages bytes, passes the controlled `sourcePath` to the existing media save
operation, validates its actual destination path, and cleans the token in `finally`.
That existing native operation owns the configured download directory, safe naming,
collision handling, atomic destination write and saved-file record. Native failures
are propagated without falling back to a browser anchor. Cleanup failure changes neither
a validated successful save nor the original save/cancellation failure. It emits only a
fixed SDK warning, without generated content, paths, tokens or the native error text.

Bind `signal` to the original presentation/session. Cancellation prevents a later stage
or media save from starting and still releases a returned token. A media operation that
has already started may finish; the caller must suppress stale UI completion rather than
automatically retrying it. Browser download behavior belongs in the browser adapter and
does not establish that a desktop file was saved.
