// GENERATED. Do not edit by hand.
import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import type { NativeBridge, NativeCallDescriptor } from '../../contract/bridge_contract';
import { wireDecodeResponse, wireEncodeRequest } from '../../adapter/codec/wireCodec';
import { assertBindingContractVersion } from '../../bridge/contractVersion';
import {
  WEB_EVENT_CHANNELS,
  eventTypeForWebChannel,
  nativeEventFromCode,
} from '../../adapter/module/DefaultEventsApi';
import { FlareSdkException } from '../../bridge/flareSdkException';

/** Maps contract operation ids to dedicated Tauri commands from `flare-core-sdk-engine/bindings/tauri`. */
export const TAURI_COMMAND_BY_OPERATION: Record<string, string> = {
  'sdk.init': 'sdk_init',
  'sdk.login': 'sdk_login',
  'sdk.prepare': 'sdk_prepare',
  'sdk.connect': 'sdk_connect',
  'sdk.logout': 'sdk_logout',
};

type EventEmitter = { emit(event: unknown): void };

function mapImPayload(channel: string, payload: unknown): unknown {
  const eventType = eventTypeForWebChannel(channel);
  if (eventType === undefined) {
    return wireDecodeResponse(payload);
  }
  return nativeEventFromCode(eventType, payload);
}

/** Tauri L1 bridge over `flare-core-sdk-engine/bindings/tauri` commands. */
export class TauriNativeBridge implements NativeBridge {
  private eventsApi: EventEmitter | null = null;
  private unlisteners: UnlistenFn[] = [];
  private eventsStarted = false;
  private eventsStarting: Promise<void> | null = null;
  private contractVersionCheck: Promise<void> | null = null;

  attachEventEmitter(api: EventEmitter): void {
    this.eventsApi = api;
  }

  async invoke<T>(descriptor: NativeCallDescriptor, request?: unknown): Promise<T> {
    const operation = descriptor.operation;
    try {
      if (operation === 'sdk.create') {
        return { handle: 1 } as T;
      }
      if (operation === 'event.subscribe' || operation === 'event.subscribe_batch') {
        await this.ensureEventListeners();
        return { id: 1 } as T;
      }
      if (operation === 'event.unsubscribe_all') {
        await this.stopEventListeners();
        return undefined as T;
      }
      if (operation === 'event.unsubscribe') {
        return undefined as T;
      }
      if (operation === 'sdk.dispose' || operation === 'sdk.hard_reset') {
        await this.stopEventListeners();
        return undefined as T;
      }

      await this.ensureContractVersion();

      const dedicated = TAURI_COMMAND_BY_OPERATION[operation];
      if (dedicated === 'sdk_init') {
        const encoded = wireEncodeRequest(request ?? {}) as Record<string, unknown>;
        await invoke(dedicated, {
          environment: encoded.environment ?? 'development',
          sdkConfig: encoded.sdkConfig ?? encoded,
        });
        return undefined as T;
      }
      if (dedicated === 'sdk_login' || dedicated === 'sdk_prepare' || dedicated === 'sdk_connect') {
        await invoke(dedicated, wireEncodeRequest(request ?? {}) as Record<string, unknown>);
        return undefined as T;
      }
      if (dedicated === 'sdk_logout') {
        await invoke(dedicated);
        return undefined as T;
      }

      const raw = await invoke<unknown>('sdk_invoke_json', {
        apiId: operation,
        requestJson: JSON.stringify(wireEncodeRequest(request ?? {})),
      });
      if (raw === null || raw === undefined) {
        return undefined as T;
      }
      return wireDecodeResponse(raw) as T;
    } catch (error) {
      const structured = FlareSdkException.fromBindingError(error, operation);
      if (structured) {
        throw structured;
      }
      throw new FlareSdkException(
        'tauri.invoke_failed',
        error instanceof Error ? error.message : `${error}`,
        operation,
        { transport: 'tauri-command', cApi: descriptor.cApi },
      );
    }
  }

  private async ensureEventListeners(): Promise<void> {
    if (this.eventsStarted) {
      return;
    }
    if (this.eventsStarting) {
      await this.eventsStarting;
      return;
    }
    this.eventsStarting = this.startEventListeners();
    try {
      await this.eventsStarting;
    } finally {
      this.eventsStarting = null;
    }
  }

  private async ensureContractVersion(): Promise<void> {
    if (!this.contractVersionCheck) {
      this.contractVersionCheck = invoke<unknown>('sdk_ffi_contract_version')
        .then((raw) => {
          assertBindingContractVersion(raw, 'tauri-command');
        })
        .catch((error) => {
          this.contractVersionCheck = null;
          throw error;
        });
    }
    await this.contractVersionCheck;
  }

  private async startEventListeners(): Promise<void> {
    const registered: UnlistenFn[] = [];
    for (const channel of WEB_EVENT_CHANNELS) {
      try {
        const unlisten = await listen(channel, (event: { payload: unknown }) => {
          const mapped = mapImPayload(channel, event.payload);
          this.eventsApi?.emit(mapped);
        });
        registered.push(unlisten);
      } catch (error) {
        await Promise.allSettled(registered.map((fn) => fn()));
        throw error;
      }
    }
    this.unlisteners = registered;
    this.eventsStarted = true;
  }

  private async stopEventListeners(): Promise<void> {
    if (this.eventsStarting) {
      await this.eventsStarting.catch(() => undefined);
    }
    const pending = this.unlisteners.splice(0);
    await Promise.all(pending.map((fn) => fn()));
    this.eventsStarted = false;
  }
}
