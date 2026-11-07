// GENERATED. Do not edit by hand.
import type { ConnectTokenRefresher, NativeBridge, NativeCallDescriptor } from '../contract/bridge_contract';
import { wireDecodeResponse, wireEncodeRequest } from '../adapter/codec/wireCodec';
import { assertBindingContractVersion } from './contractVersion';
import { FlareSdkException } from './flareSdkException';

/** Expected wasm-bindgen surface from `flare-core-sdk-engine/bindings/wasm`. */
export interface FlareWasmRuntime {
  invoke(operation: string, requestJson: string): Promise<unknown> | unknown;
  flareBindingContractVersion(): string;
  dispose?(): Promise<void> | void;
  /** Cancel local operations without replacing the authenticated runtime. */
  cancelPendingInvocations?(): boolean;
  /** Installs (or, with undefined, removes) the page's connect token refresher. */
  setConnectTokenRefresher?(refresher: ConnectTokenRefresher | undefined): Promise<void> | void;
}

export type WasmBridgeOptions = {
  /** URL or package subpath for the wasm module. Defaults to `/flare_im_core_sdk.wasm`. */
  wasmUrl?: string;
  /** Pre-instantiated runtime (tests / bundler integration). */
  runtime?: FlareWasmRuntime;
  loader?: (wasmUrl: string) => Promise<FlareWasmRuntime>;
};

const DEFAULT_WASM_URL = '/flare_im_core_sdk.wasm';

type WasmBindgenModule = {
  default?: (input?: unknown) => Promise<unknown> | unknown;
  flareBindingContractVersion?: () => string;
  createWasmRuntime?: () => FlareWasmRuntime;
  FlareImWasmRuntime?: new () => FlareWasmRuntime;
};

type RuntimeShape = Omit<FlareWasmRuntime, 'flareBindingContractVersion'> & {
  flareBindingContractVersion?: () => string;
};

function hasRuntimeShape(value: unknown): value is RuntimeShape {
  return value !== null && typeof value === 'object' && typeof (value as RuntimeShape).invoke === 'function';
}

function withContractVersion(runtime: RuntimeShape, fromModule?: () => string): FlareWasmRuntime {
  const contractVersion = runtime.flareBindingContractVersion ?? fromModule;
  return {
    invoke: runtime.invoke.bind(runtime),
    ...(runtime.dispose ? { dispose: runtime.dispose.bind(runtime) } : {}),
    ...(runtime.cancelPendingInvocations ? { cancelPendingInvocations: runtime.cancelPendingInvocations.bind(runtime) } : {}),
    ...(runtime.setConnectTokenRefresher ? { setConnectTokenRefresher: runtime.setConnectTokenRefresher.bind(runtime) } : {}),
    flareBindingContractVersion: () => {
      if (!contractVersion) {
        return '';
      }
      return contractVersion();
    },
  };
}

async function defaultWasmLoader(wasmUrl: string): Promise<FlareWasmRuntime> {
  const isWasmAssetUrl = wasmUrl.endsWith('.wasm');
  const specifiers = [
    ...(isWasmAssetUrl ? [] : [wasmUrl]),
    'flare-core-sdk-engine/bindings/wasm/pkg/flare_im_core_sdk',
    '../../../../../flare-core-sdk-engine/bindings/wasm/pkg/flare_im_core_sdk',
    '../../../../flare-core-sdk-engine/bindings/wasm/pkg/flare_im_core_sdk',
  ];
  let lastError: unknown;
  for (const specifier of specifiers) {
    try {
      const mod = (await import(/* @vite-ignore */ specifier)) as WasmBindgenModule;
      if (typeof mod.default === 'function') {
        const initialized = await mod.default();
        if (hasRuntimeShape(initialized)) {
          return withContractVersion(initialized, mod.flareBindingContractVersion);
        }
      }
      if (typeof mod.createWasmRuntime === 'function') {
        return withContractVersion(mod.createWasmRuntime(), mod.flareBindingContractVersion);
      }
      if (typeof mod.FlareImWasmRuntime === 'function') {
        return withContractVersion(new mod.FlareImWasmRuntime(), mod.flareBindingContractVersion);
      }
    } catch (error) {
      lastError = error;
    }
  }
  throw new FlareSdkException(
    'wasm.not_available',
    'Unable to load flare-im-core-sdk wasm runtime. Build bindings/wasm first or pass WasmBridgeOptions.runtime.',
    undefined,
    {
      wasmUrl,
      hint: 'Run `npm install && npm run build` in flare-core-sdk-engine/bindings/wasm, or pass WasmBridgeOptions.runtime.',
      lastError: `${lastError ?? 'unknown'}`,
    },
  );
}

/** Web L1 bridge over `flare-core-sdk-engine/bindings/wasm` (no hand-written IM logic). */
export class WasmNativeBridge implements NativeBridge {
  private runtime: FlareWasmRuntime | null;
  private contractVersionChecked = false;

  constructor(private readonly options: WasmBridgeOptions = {}) {
    this.runtime = options.runtime ?? null;
  }

  private async ensureRuntime(): Promise<FlareWasmRuntime> {
    if (this.runtime) {
      return this.runtime;
    }
    const loader = this.options.loader ?? defaultWasmLoader;
    this.runtime = await loader(this.options.wasmUrl ?? DEFAULT_WASM_URL);
    return this.runtime;
  }

  private async ensureContractVersion(): Promise<void> {
    if (this.contractVersionChecked) {
      return;
    }
    const runtime = await this.ensureRuntime();
    assertBindingContractVersion(runtime.flareBindingContractVersion(), 'wasm');
    this.contractVersionChecked = true;
  }

  async invoke<T>(descriptor: NativeCallDescriptor, request?: unknown): Promise<T> {
    await this.ensureContractVersion();
    const runtime = await this.ensureRuntime();
    const payload =
      request === undefined
        ? '{}'
        : typeof request === 'string'
          ? request
          : JSON.stringify(wireEncodeRequest(request));
    try {
      const result = await runtime.invoke(descriptor.operation, payload);
      return wireDecodeResponse(result) as T;
    } catch (error) {
      const structured = FlareSdkException.fromBindingError(error, descriptor.operation);
      if (structured) {
        throw structured;
      }
      throw new FlareSdkException(
        'wasm.invoke_failed',
        error instanceof Error ? error.message : `${error}`,
        descriptor.operation,
        { cApi: descriptor.cApi, transport: descriptor.transport },
      );
    }
  }

  async setConnectTokenRefresher(refresher: ConnectTokenRefresher | null): Promise<void> {
    const runtime = await this.ensureRuntime();
    if (!runtime.setConnectTokenRefresher) {
      throw new FlareSdkException(
        'capabilityUnavailable',
        'this wasm runtime has no connect token refresher hook',
        'sdk.set_connect_token_refresher',
      );
    }
    await runtime.setConnectTokenRefresher(refresher ?? undefined);
  }
}
