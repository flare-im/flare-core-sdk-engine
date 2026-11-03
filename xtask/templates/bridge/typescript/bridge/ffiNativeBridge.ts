// GENERATED. Do not edit by hand.
import { NativeCallMap, type NativeBridge, type NativeCallDescriptor } from '../contract/bridge_contract';
import { wireDecodeResponse, wireEncodeRequest } from '../adapter/codec/wireCodec';
import { assertBindingContractVersion } from './contractVersion';
import { FlareSdkException } from './flareSdkException';

/** Host-provided React Native / uni-app native module surface. */
export interface FlareNativeModuleHost {
  invoke(operation: string, requestJson: string, descriptorJson?: string): Promise<string>;
}

declare global {
  // eslint-disable-next-line no-var
  var __FLARE_IM_CORE_NATIVE__: FlareNativeModuleHost | undefined;
}

function resolveNativeHost(): FlareNativeModuleHost {
  const host = globalThis.__FLARE_IM_CORE_NATIVE__;
  if (host?.invoke) {
    return host;
  }
  const rn = (globalThis as { nativeModules?: Record<string, { invoke?: FlareNativeModuleHost['invoke'] }> })
    .nativeModules?.FlareImCoreSdk;
  if (rn?.invoke) {
    return { invoke: rn.invoke.bind(rn) };
  }
  throw new FlareSdkException(
    'native.module_missing',
    'Native module host not registered. Link flare-core-sdk-engine/bindings/c through a TurboModule and set globalThis.__FLARE_IM_CORE_NATIVE__.',
  );
}

/** React Native / uni-app App L1 bridge over `flare-core-sdk-engine/bindings/c`. */
export class FfiNativeBridge implements NativeBridge {
  private contractVersionCheck: Promise<void> | null = null;

  constructor(private readonly host: FlareNativeModuleHost = resolveNativeHost()) {}

  private async ensureContractVersion(): Promise<void> {
    if (!this.contractVersionCheck) {
      const descriptorJson = JSON.stringify(NativeCallMap.diagnosticsFfiContractVersion);
      this.contractVersionCheck = this.host
        .invoke('diagnostics.ffi_contract_version', '{}', descriptorJson)
        .then((raw) => {
          const value = raw && raw !== 'null' ? wireDecodeResponse(JSON.parse(raw)) : undefined;
          assertBindingContractVersion(value, 'ffi');
        })
        .catch((error) => {
          this.contractVersionCheck = null;
          throw error;
        });
    }
    await this.contractVersionCheck;
  }

  async invoke<T>(descriptor: NativeCallDescriptor, request?: unknown): Promise<T> {
    await this.ensureContractVersion();
    const payload =
      request === undefined
        ? '{}'
        : typeof request === 'string'
          ? request
          : JSON.stringify(wireEncodeRequest(request));
    try {
      const descriptorJson = JSON.stringify(descriptor);
      const raw = await this.host.invoke(descriptor.operation, payload, descriptorJson);
      if (!raw || raw === 'null') {
        return undefined as T;
      }
      return wireDecodeResponse(JSON.parse(raw)) as T;
    } catch (error) {
      const structured = FlareSdkException.fromBindingError(error, descriptor.operation);
      if (structured) {
        throw structured;
      }
      throw new FlareSdkException(
        'native.invoke_failed',
        error instanceof Error ? error.message : `${error}`,
        descriptor.operation,
        { cApi: descriptor.cApi },
      );
    }
  }
}
