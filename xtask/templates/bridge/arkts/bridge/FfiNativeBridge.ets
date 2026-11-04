// GENERATED. Do not edit by hand.
import { NativeBridge, NativeCallDescriptor } from '../contract/BridgeContract';
import { SdkOperations } from '../contract/SdkContract';
import { assertFfiContractVersion } from './FfiContractVersionGuard';
import { FlareSdkException } from './FlareSdkException';
import { FlareAppInfo } from './FlareAppInfo';
import { wireDecodeResponse, wireEncodeRequest } from '../adapter/codec/WireCodec';
import flareCoreNative from 'libflare_core_harmony_arkts_sdk.so';

type NativeModule = {
  create(): number;
  release(handle: number): void;
  hardReset(): void;
  connectionState(handle: number): number;
  isConnected(handle: number): boolean;
  sessionActive(handle: number): boolean;
  sdkVersion(): string;
  ffiContractVersion(): string;
  messageDispatchJson(handle: number, op: string, requestJson: string): Promise<string>;
  capabilityDispatchJson(handle: number, op: string, requestJson: string): Promise<string>;
  sdkInvokeJson(handle: number, apiId: string, requestJson: string): Promise<string>;
  messageBuildJson(handle: number, requestJson: string): Promise<string>;
  sdkInit(handle: number, requestJson: string): Promise<string>;
  sdkUninit(handle: number): Promise<string>;
  sdkLogin(handle: number, userId: string, token: string, storeConfigJson: string): Promise<string>;
  sdkLogout(handle: number): Promise<string>;
  sdkUpdateAccessToken(handle: number, accessToken: string, tenantId: string): Promise<string>;
  sdkCurrentUserId(handle: number): Promise<string>;
  sdkDisconnect(handle: number): Promise<string>;
  sdkDataRoot(handle: number): Promise<string>;
  subscribeEvents(handle: number): number;
  subscribeEventsBatch(handle: number): number;
  unsubscribeEvents(subscription: number): void;
  unsubscribeAllEvents(): void;
};

const nativeModule = flareCoreNative as NativeModule;

/**
 * Thin ArkTS bridge over the Flare Core C ABI / NAPI layer.
 */
export class FfiNativeBridge implements NativeBridge {
  private handle: number = 0;
  private released: boolean = false;
  private contractVersionChecked: boolean = false;
  /** `sdk.init` 时补进配置的应用版本信息（宿主在请求里显式给的值优先）。 */
  private readonly appInfo: FlareAppInfo;

  /** `appInfo` 默认从本应用的 bundle 信息自动采集；需要覆盖（如渠道）时显式传入。 */
  constructor(appInfo?: FlareAppInfo) {
    this.appInfo = appInfo ?? FlareAppInfo.fromBundle();
  }

  async invoke<T>(descriptor: NativeCallDescriptor, request?: Object): Promise<T> {
    if (descriptor.operation !== SdkOperations.diagnosticsFfiContractVersion) {
      this.ensureContractVersion();
    }
    let value: Object | boolean | number | string | null;
    switch (descriptor.operation) {
      case 'sdk.create':
        value = this.createHandle();
        break;
      case 'sdk.dispose':
        this.releaseHandle();
        value = null;
        break;
      case 'sdk.hard_reset':
        nativeModule.hardReset();
        this.handle = 0;
        this.released = true;
        value = null;
        break;
      case 'sdk.is_connected':
        value = nativeModule.isConnected(this.requireHandle());
        break;
      case 'sdk.session_active':
        value = nativeModule.sessionActive(this.requireHandle());
        break;
      case 'connection.get_state':
        value = this.connectionState(nativeModule.connectionState(this.requireHandle()));
        break;
      case 'diagnostics.sdk_version':
        value = { version: nativeModule.sdkVersion() };
        break;
      case 'diagnostics.ffi_contract_version':
        value = { version: nativeModule.ffiContractVersion() };
        break;
      case 'event.subscribe':
        value = { subscription: nativeModule.subscribeEvents(this.requireHandle()) };
        break;
      case 'event.subscribe_batch':
        value = { subscription: nativeModule.subscribeEventsBatch(this.requireHandle()) };
        break;
      case 'event.unsubscribe':
        nativeModule.unsubscribeEvents(this.numberField(request, 'subscription'));
        value = null;
        break;
      case 'event.unsubscribe_all':
        nativeModule.unsubscribeAllEvents();
        value = null;
        break;
      default:
        value = await this.invokeAsync(descriptor, request);
    }
    return value as T;
  }

  private ensureContractVersion(): void {
    if (this.contractVersionChecked) {
      return;
    }
    assertFfiContractVersion(nativeModule.ffiContractVersion());
    this.contractVersionChecked = true;
  }

  private createHandle(): Record<string, Object> {
    if (this.handle === 0 || this.released) {
      this.handle = nativeModule.create();
      this.released = false;
    }
    if (this.handle === 0) {
      throw new FlareSdkException('native_create_failed', 'flare_sdk_create returned an invalid handle.', 'sdk.create');
    }
    return { handle: this.handle };
  }

  private releaseHandle(): void {
    if (this.handle !== 0 && !this.released) {
      nativeModule.release(this.handle);
    }
    this.handle = 0;
    this.released = true;
  }

  private requireHandle(): number {
    if (this.handle === 0 || this.released) {
      this.createHandle();
    }
    return this.handle;
  }

  private async invokeAsync(descriptor: NativeCallDescriptor, request?: Object): Promise<Object | null> {
    let resultJson: string;
    if (descriptor.transport === 'message-dispatch-json' && descriptor.cApi === 'flare_message_dispatch_json') {
      resultJson = await nativeModule.messageDispatchJson(this.requireHandle(), descriptor.dispatchOp ?? '', this.encodeJson(request));
    } else if (descriptor.transport === 'capability-dispatch-json' && descriptor.cApi === 'flare_capability_dispatch_json') {
      resultJson = await nativeModule.capabilityDispatchJson(this.requireHandle(), descriptor.dispatchOp ?? '', this.encodeJson(request));
    } else if (descriptor.transport === 'contract-invoke-json' && descriptor.cApi === 'flare_sdk_invoke_json') {
      resultJson = await nativeModule.sdkInvokeJson(this.requireHandle(), descriptor.operation, this.encodeJson(request));
    } else if (descriptor.transport === 'dispatch-json' && descriptor.cApi === 'flare_message_build_json') {
      resultJson = await nativeModule.messageBuildJson(this.requireHandle(), this.encodeJson(request));
    } else if (descriptor.transport === 'dispatch-json' && descriptor.cApi === 'flare_message_dispatch_json') {
      const op = this.stringField(request, 'op') || descriptor.dispatchOp || '';
      resultJson = await nativeModule.messageDispatchJson(this.requireHandle(), op, this.encodeJson(request));
    } else if (descriptor.cApi === 'flare_sdk_init') {
      resultJson = await nativeModule.sdkInit(
        this.requireHandle(),
        this.encodeJson(this.appInfo.fill(request)),
      );
    } else if (descriptor.cApi === 'flare_sdk_uninit') {
      resultJson = await nativeModule.sdkUninit(this.requireHandle());
    } else if (descriptor.cApi === 'flare_sdk_login') {
      resultJson = await nativeModule.sdkLogin(
        this.requireHandle(),
        this.stringField(request, 'userId'),
        this.stringField(request, 'token'),
        this.storeConfigJsonFromLogin(request),
      );
    } else if (descriptor.cApi === 'flare_sdk_logout') {
      resultJson = await nativeModule.sdkLogout(this.requireHandle());
    } else if (descriptor.cApi === 'flare_sdk_update_access_token') {
      resultJson = await nativeModule.sdkUpdateAccessToken(
        this.requireHandle(),
        this.stringField(request, 'accessToken'),
        this.stringField(request, 'tenantId'),
      );
    } else if (descriptor.cApi === 'flare_sdk_current_user_id') {
      resultJson = await nativeModule.sdkCurrentUserId(this.requireHandle());
    } else if (descriptor.cApi === 'flare_sdk_disconnect') {
      resultJson = await nativeModule.sdkDisconnect(this.requireHandle());
    } else if (descriptor.cApi === 'flare_sdk_data_root') {
      resultJson = await nativeModule.sdkDataRoot(this.requireHandle());
    } else {
      throw new FlareSdkException(
        'bridge.unsupported_operation',
        `Unsupported ArkTS native operation: ${descriptor.operation}`,
        descriptor.operation,
        { cApi: descriptor.cApi, transport: descriptor.transport, returnMode: descriptor.returnMode },
      );
    }
    if (descriptor.responseEncoding === 'unit') {
      return null;
    }
    if (resultJson.length === 0) {
      return {};
    }
    return wireDecodeResponse(JSON.parse(resultJson)) as Object;
  }

  private connectionState(code: number): string {
    switch (code) {
      case 0: return 'disconnected';
      case 1: return 'connecting';
      case 2: return 'connected';
      case 3: return 'ready';
      case 4: return 'reconnecting';
      default: throw new FlareSdkException(
        'invalidParameter',
        `invalid connection state code: ${code}`,
        'connection.get_state',
        { field: 'stateCode' },
      );
    }
  }

  private encodeJson(value?: Object): string {
    return JSON.stringify(wireEncodeRequest(value ?? {}) ?? {});
  }

  private storeConfigJsonFromLogin(value?: Object): string {
    const record = (value ?? {}) as Record<string, Object>;
    const direct = record['storeConfigJson'];
    if (typeof direct === 'string' && direct.length > 0) {
      return direct;
    }
    return '{}';
  }

  private stringField(value: Object | undefined, name: string): string {
    const record = (value ?? {}) as Record<string, Object>;
    const field = record[name];
    if (field !== undefined && field !== null) {
      return `${field}`;
    }
    return '';
  }

  private numberField(value: Object | undefined, name: string): number {
    const text = this.stringField(value, name);
    return text.length > 0 ? Number(text) : 0;
  }
}
