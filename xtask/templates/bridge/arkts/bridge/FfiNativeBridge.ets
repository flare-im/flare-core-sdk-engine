// GENERATED. Do not edit by hand.
import { ConnectTokenRefresher, NativeBridge, NativeCallDescriptor } from '../contract/BridgeContract';
import { SdkErrorCodes, SdkOperations } from '../contract/SdkContract';
import { assertFfiContractVersion } from './FfiContractVersionGuard';
import { FlareSdkException, sdkExceptionFromNative } from './FlareSdkException';
import { FlareAppInfo } from './FlareAppInfo';
import { wireDecodeResponse, wireEncodeRequest } from '../adapter/codec/WireCodec';
import flareCoreNative from 'libflare_core_harmony_arkts_sdk.so';

/** Receives every native SDK event on the ArkTS thread: the C ABI event code and its JSON payload. */
export type NativeEventSink = (eventType: number, eventJson: string) => void;

/** `FLARE_OK` and the C code of a refresh request the core no longer waits for. */
const FLARE_OK = 0;
const FLARE_ERR_INVALID_PARAM = 2;

/** The core waits 20 s for a refreshed token; the host refresher gets a little less so its answer lands in time. */
const CONNECT_TOKEN_REFRESH_BUDGET_MS = 19000;

/**
 * The NAPI module (`napi_bridge.cpp`). Sync calls return values or C result codes directly.
 * Callbacks (`onEvent`, `onRequest`) run on the ArkTS thread that registered them: the module
 * hops from the SDK's threads through threadsafe functions. An async call that fails in the C ABI
 * rejects with an `Error` carrying `nativeCode` (the `FlareError.code`) and `detailsJson`.
 */
interface NativeModule {
  create(): number;
  release(handle: number): void;
  hardReset(): void;
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
  subscribeEvents(handle: number, onEvent: NativeEventSink): number;
  subscribeEventsBatch(handle: number): number;
  unsubscribeEvents(subscription: number): void;
  unsubscribeAllEvents(): void;
  /**
   * `flare_sdk_set_connect_token_refresher`; null removes it. Settles when the core has taken it.
   * `onRequest` runs on this ArkTS thread for every refresh request the core makes.
   */
  setConnectTokenRefresher(handle: number, onRequest: ((requestId: number) => void) | null): Promise<void>;
  /** `flare_sdk_complete_connect_token_refresh`; an empty token means none. Returns the C result code. */
  completeConnectTokenRefresh(handle: number, requestId: number, token: string): number;
}

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
  private eventSink: NativeEventSink | null = null;

  /** `appInfo` 默认从本应用的 bundle 信息自动采集；需要覆盖（如渠道）时显式传入。 */
  constructor(appInfo?: FlareAppInfo) {
    this.appInfo = appInfo ?? FlareAppInfo.fromBundle();
  }

  /** Where native events go once `event.subscribe` runs; the client's events module registers itself. */
  registerEventSink(sink: NativeEventSink | null): void {
    this.eventSink = sink;
  }

  /**
   * Installs (or, with null, removes) the host's connect token refresher. When the gateway rejects
   * the connect token the core asks once, from one of its own threads; the native module carries
   * the request to this ArkTS thread, the refresher gets 19 s, and its answer goes back to the
   * core exactly once (no token, a failure or the budget running out all count as none).
   */
  async setConnectTokenRefresher(refresher: ConnectTokenRefresher | null): Promise<void> {
    this.ensureContractVersion();
    const handle = this.requireHandle();
    const onRequest = refresher === null ? null : (requestId: number) => {
      this.answerTokenRefresh(handle, requestId, refresher);
    };
    try {
      await nativeModule.setConnectTokenRefresher(handle, onRequest);
    } catch (error) {
      throw this.nativeFailure(error as Object, SdkOperations.sdkSetConnectTokenRefresher);
    }
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
      case 'sdk.set_connect_token_refresher':
        // 续期回调是宿主函数，走不了 JSON 请求；由 setConnectTokenRefresher(refresher) 安装。
        throw new FlareSdkException(
          SdkErrorCodes.capabilityUnavailable,
          'Install the connect token refresher with FfiNativeBridge.setConnectTokenRefresher(refresher).',
          descriptor.operation,
          { cApi: descriptor.cApi },
        );
      case 'sdk.complete_connect_token_refresh':
        value = this.completeConnectTokenRefresh(
          this.requireHandle(),
          this.numberField(request, 'requestId'),
          this.stringField(request, 'token'),
        );
        break;
      case 'diagnostics.sdk_version':
        value = { version: nativeModule.sdkVersion() } as Record<string, Object>;
        break;
      case 'diagnostics.ffi_contract_version':
        value = { version: nativeModule.ffiContractVersion() } as Record<string, Object>;
        break;
      case 'event.subscribe':
        value = {
          subscription: nativeModule.subscribeEvents(this.requireHandle(), (eventType: number, eventJson: string) => {
            this.deliverEvent(eventType, eventJson);
          }),
        } as Record<string, Object>;
        break;
      case 'event.subscribe_batch':
        value = { subscription: nativeModule.subscribeEventsBatch(this.requireHandle()) } as Record<string, Object>;
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
    const created: Record<string, Object> = { handle: this.handle };
    return created;
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

  /** A listener that throws must not unwind into the native module. */
  private deliverEvent(eventType: number, eventJson: string): void {
    const sink = this.eventSink;
    if (sink === null) {
      return;
    }
    try {
      sink(eventType, eventJson);
    } catch (error) {
      console.error(`flare-core event ${eventType} listener failed: ${error}`);
    }
  }

  /** OK: answered. Invalid param: the core already gave up on this request. Anything else is a failure. */
  private completeConnectTokenRefresh(handle: number, requestId: number, token: string): boolean {
    const code = nativeModule.completeConnectTokenRefresh(handle, requestId, token);
    if (code === FLARE_OK) {
      return true;
    }
    if (code === FLARE_ERR_INVALID_PARAM) {
      return false;
    }
    throw sdkExceptionFromNative(
      code,
      'flare_sdk_complete_connect_token_refresh failed',
      '',
      SdkOperations.sdkCompleteConnectTokenRefresh,
    );
  }

  /** Runs the refresher for one core request and answers within the budget; a late answer finds the request gone. */
  private answerTokenRefresh(handle: number, requestId: number, refresher: ConnectTokenRefresher): void {
    let answered = false;
    let timer = -1;
    const answer = (token: string | null): void => {
      if (answered) {
        return;
      }
      answered = true;
      clearTimeout(timer);
      try {
        this.completeConnectTokenRefresh(handle, requestId, token ?? '');
      } catch (error) {
        console.error(`flare-core connect token refresh ${requestId} could not be answered: ${error}`);
      }
    };
    timer = setTimeout(() => answer(null), CONNECT_TOKEN_REFRESH_BUDGET_MS);
    try {
      refresher().then((token: string | null) => answer(token), () => answer(null));
    } catch (error) {
      answer(null);
    }
  }

  private async invokeAsync(descriptor: NativeCallDescriptor, request?: Object): Promise<Object | null> {
    let resultJson: string;
    try {
      resultJson = await this.callNative(descriptor, request);
    } catch (error) {
      throw this.nativeFailure(error as Object, descriptor.operation);
    }
    if (descriptor.responseEncoding === 'unit') {
      return null;
    }
    if (resultJson.length === 0) {
      return {} as Record<string, Object>;
    }
    return wireDecodeResponse(JSON.parse(resultJson)) as Object;
  }

  private callNative(descriptor: NativeCallDescriptor, request?: Object): Promise<string> {
    if (descriptor.transport === 'message-dispatch-json' && descriptor.cApi === 'flare_message_dispatch_json') {
      return nativeModule.messageDispatchJson(this.requireHandle(), descriptor.dispatchOp ?? '', this.encodeJson(request));
    } else if (descriptor.transport === 'capability-dispatch-json' && descriptor.cApi === 'flare_capability_dispatch_json') {
      return nativeModule.capabilityDispatchJson(this.requireHandle(), descriptor.dispatchOp ?? '', this.encodeJson(request));
    } else if (descriptor.transport === 'contract-invoke-json' && descriptor.cApi === 'flare_sdk_invoke_json') {
      return nativeModule.sdkInvokeJson(this.requireHandle(), descriptor.operation, this.encodeJson(request));
    } else if (descriptor.transport === 'dispatch-json' && descriptor.cApi === 'flare_message_build_json') {
      return nativeModule.messageBuildJson(this.requireHandle(), this.encodeJson(request));
    } else if (descriptor.transport === 'dispatch-json' && descriptor.cApi === 'flare_message_dispatch_json') {
      const op = this.stringField(request, 'op') || descriptor.dispatchOp || '';
      return nativeModule.messageDispatchJson(this.requireHandle(), op, this.encodeJson(request));
    } else if (descriptor.cApi === 'flare_sdk_init') {
      return nativeModule.sdkInit(
        this.requireHandle(),
        this.encodeJson(this.appInfo.fill(request)),
      );
    } else if (descriptor.cApi === 'flare_sdk_uninit') {
      return nativeModule.sdkUninit(this.requireHandle());
    } else if (descriptor.cApi === 'flare_sdk_login') {
      return nativeModule.sdkLogin(
        this.requireHandle(),
        this.stringField(request, 'userId'),
        this.stringField(request, 'token'),
        this.storeConfigJsonFromLogin(request),
      );
    } else if (descriptor.cApi === 'flare_sdk_logout') {
      return nativeModule.sdkLogout(this.requireHandle());
    } else if (descriptor.cApi === 'flare_sdk_update_access_token') {
      return nativeModule.sdkUpdateAccessToken(
        this.requireHandle(),
        this.stringField(request, 'accessToken'),
        this.stringField(request, 'tenantId'),
      );
    } else if (descriptor.cApi === 'flare_sdk_current_user_id') {
      return nativeModule.sdkCurrentUserId(this.requireHandle());
    } else if (descriptor.cApi === 'flare_sdk_disconnect') {
      return nativeModule.sdkDisconnect(this.requireHandle());
    } else if (descriptor.cApi === 'flare_sdk_data_root') {
      return nativeModule.sdkDataRoot(this.requireHandle());
    }
    throw new FlareSdkException(
      'bridge.unsupported_operation',
      `Unsupported ArkTS native operation: ${descriptor.operation}`,
      descriptor.operation,
      { cApi: descriptor.cApi, transport: descriptor.transport, returnMode: descriptor.returnMode },
    );
  }

  /** A C ABI failure becomes its typed exception (12, 13 and 14 included); anything else passes through. */
  private nativeFailure(error: Object, operation: string): Object {
    if (error instanceof FlareSdkException || error === null || typeof error !== 'object') {
      return error;
    }
    const fields = error as Record<string, Object>;
    const nativeCode = fields['nativeCode'];
    if (typeof nativeCode !== 'number') {
      return error;
    }
    const message = fields['message'];
    const detailsJson = fields['detailsJson'];
    return sdkExceptionFromNative(
      nativeCode as number,
      typeof message === 'string' ? message as string : '',
      typeof detailsJson === 'string' ? detailsJson as string : '',
      operation,
    );
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
