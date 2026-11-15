// GENERATED. Do not edit by hand.
import { ConnectTokenRefresher, NativeBridge, NativeCallMap } from '../contract/BridgeContract';
import { SdkErrorCodes } from '../contract/SdkContract';
import { FlareSdkException } from '../bridge/FlareSdkException';
import type { FlareImClient } from '../api/client';
import type { ConnectionApi } from '../api/modules/connection';
import type { AccessApi } from '../api/modules/access';
import type { LifecycleApi } from '../api/modules/lifecycle';
import type { ConversationsApi } from '../api/modules/conversations';
import type { MessageBuilderApi } from '../api/modules/message_builder';
import type { MessagesApi } from '../api/modules/messages';
import type { SyncApi } from '../api/modules/sync';
import type { UserApi } from '../api/modules/user';
import type { PresenceApi } from '../api/modules/presence';
import type { MediaApi } from '../api/modules/media';
import type { CapabilitiesApi } from '../api/modules/capabilities';
import type { ViewsApi } from '../api/modules/views';
import type { EventsApi } from '../api/modules/events';
import type { DiagnosticsApi } from '../api/modules/diagnostics';
import type { NotificationApi } from '../api/modules/notification';
import { LifecycleEventName } from '../model';
import { emitLifecycleEvent, invokeBool, invokeMap, invokeVoid, sdkErrorPayloadFromError, userIdFromRequest } from './codec/NativeInvoke';
import { setHeartbeatNatTimeoutRequestToMap } from './codec/WireCodec';
import { DefaultEventsApi } from './module/DefaultEventsApi';
import { DefaultConnectionApi } from './module/DefaultConnectionApi';
import { DefaultAccessApi } from './module/DefaultAccessApi';
import { DefaultLifecycleApi } from './module/DefaultLifecycleApi';
import { DefaultConversationsApi } from './module/DefaultConversationsApi';
import { DefaultMessagesApi } from './module/DefaultMessagesApi';
import { DefaultSyncApi } from './module/DefaultSyncApi';
import { DefaultUserApi } from './module/DefaultUserApi';
import { DefaultPresenceApi } from './module/DefaultPresenceApi';
import { DefaultMediaApi } from './module/DefaultMediaApi';
import { DefaultCapabilitiesApi } from './module/DefaultCapabilitiesApi';
import { DefaultViewsApi } from './module/DefaultViewsApi';
import { DefaultDiagnosticsApi } from './module/DefaultDiagnosticsApi';
import { DefaultNotificationApi } from './module/DefaultNotificationApi';
import { DefaultMessageBuilderApi } from './module/DefaultMessageBuilderApi';
import { PlatformLifecycle } from '../lifecycle/PlatformLifecycle';

import {
  ClearPushTokenRequest,
  HandleRemoteMessageRequest,
  HeartbeatEffectiveIntervalResponse,
  NotificationTapResult,
  PushLifecycleStatus,
  PushTokenInput,
  RemoteMessageOutcome,
  ResolveNotificationTapRequest,
  SetHeartbeatNatTimeoutRequest,
  SetPushPermissionRequest,
} from '../model';

export class DefaultFlareImClient implements FlareImClient {
  readonly connection: ConnectionApi;
  readonly access: AccessApi;
  readonly lifecycle: LifecycleApi;
  readonly conversations: ConversationsApi;
  readonly messageBuilder: MessageBuilderApi;
  readonly messages: MessagesApi;
  readonly sync: SyncApi;
  readonly user: UserApi;
  readonly presence: PresenceApi;
  readonly media: MediaApi;
  readonly capabilities: CapabilitiesApi;
  readonly views: ViewsApi;
  readonly events: EventsApi;
  readonly diagnostics: DiagnosticsApi;
  readonly notification: NotificationApi;
  private readonly bridge: NativeBridge;
  private platformLifecycle: PlatformLifecycle | null = null;

  constructor(bridge: NativeBridge) {
    this.bridge = bridge;
    this.connection = new DefaultConnectionApi(bridge);
    this.access = new DefaultAccessApi(bridge);
    this.lifecycle = new DefaultLifecycleApi(bridge);
    this.conversations = new DefaultConversationsApi(bridge);
    this.messageBuilder = new DefaultMessageBuilderApi(bridge);
    this.messages = new DefaultMessagesApi(bridge);
    this.sync = new DefaultSyncApi(bridge);
    this.user = new DefaultUserApi(bridge);
    this.presence = new DefaultPresenceApi(bridge);
    this.media = new DefaultMediaApi(bridge);
    this.capabilities = new DefaultCapabilitiesApi(bridge);
    this.views = new DefaultViewsApi(bridge);
    this.events = new DefaultEventsApi(bridge);
    this.diagnostics = new DefaultDiagnosticsApi(bridge);
    this.notification = new DefaultNotificationApi(bridge);
  }

  async create(request: Record<string, Object>): Promise<Record<string, Object>> {
    return await invokeMap(this.bridge, NativeCallMap.sdkCreate, request);
  }
  async init(request: Record<string, Object>): Promise<void> {
    emitLifecycleEvent(this.events as DefaultEventsApi, LifecycleEventName.Initializing, "sdk.init");
    // `installPlatformLifecycle` belongs to the platform package and never reaches the core.
    const config: Record<string, Object> = {};
    Object.keys(request).forEach((key: string) => {
      if (key !== 'installPlatformLifecycle') {
        config[key] = request[key];
      }
    });
    try {
      await invokeVoid(this.bridge, NativeCallMap.sdkInit, config);
      this.replacePlatformLifecycle(request['installPlatformLifecycle'] !== false);
      emitLifecycleEvent(this.events as DefaultEventsApi, LifecycleEventName.Initialized, "sdk.init");
    } catch (error) {
      emitLifecycleEvent(
        this.events as DefaultEventsApi,
        LifecycleEventName.InitFailed,
        "sdk.init",
        undefined,
        sdkErrorPayloadFromError(error as Object, "sdk.init"),
      );
      throw error;
    }
  }
  async uninit(): Promise<void> {
    this.uninstallPlatformLifecycle();
    await invokeVoid(this.bridge, NativeCallMap.sdkUninit);
  }
  async login(request: Record<string, Object>): Promise<void> {
    const userId = userIdFromRequest(request);
    try {
      await invokeVoid(this.bridge, NativeCallMap.sdkLogin, request);
      emitLifecycleEvent(this.events as DefaultEventsApi, LifecycleEventName.LoginSucceeded, "sdk.login", userId);
    } catch (error) {
      emitLifecycleEvent(
        this.events as DefaultEventsApi,
        LifecycleEventName.LoginFailed,
        "sdk.login",
        userId,
        sdkErrorPayloadFromError(error as Object, "sdk.login"),
      );
      throw error;
    }
  }
  async prepare(request: Record<string, Object>): Promise<void> {
    await invokeVoid(this.bridge, NativeCallMap.sdkPrepare, request);
  }
  async connect(request: Record<string, Object>): Promise<void> {
    await invokeVoid(this.bridge, NativeCallMap.sdkConnect, request);
  }
  async updateAccessToken(request: Record<string, Object>): Promise<void> {
    await invokeVoid(this.bridge, NativeCallMap.sdkUpdateAccessToken, request);
  }
  /** Installs (null removes) the host's connect token refresher; the bridge answers the core's requests. */
  async setConnectTokenRefresher(refresher: ConnectTokenRefresher | null): Promise<void> {
    if (this.bridge.setConnectTokenRefresher === undefined) {
      throw new FlareSdkException(
        SdkErrorCodes.capabilityUnavailable,
        'this bridge cannot call back for a connect token',
        'sdk.set_connect_token_refresher',
      );
    }
    await this.bridge.setConnectTokenRefresher(refresher);
  }
  async setHeartbeatNatTimeout(request: SetHeartbeatNatTimeoutRequest): Promise<void> {
    await invokeVoid(this.bridge, NativeCallMap.sdkSetHeartbeatNatTimeout, setHeartbeatNatTimeoutRequestToMap(request));
  }
  async heartbeatEffectiveInterval(): Promise<HeartbeatEffectiveIntervalResponse> {
    return await this.bridge.invoke<HeartbeatEffectiveIntervalResponse>(NativeCallMap.sdkHeartbeatEffectiveInterval);
  }
  async updatePushToken(request: PushTokenInput): Promise<PushLifecycleStatus> {
    const raw = await invokeMap(this.bridge, NativeCallMap.sdkUpdatePushToken, { provider: request.provider, platform: request.platform, token: request.token });
    return { configuredProviders: raw['configuredProviders'] as number, pendingRevocations: raw['pendingRevocations'] as number, registeredProviders: raw['registeredProviders'] as number, state: raw['state'] as string, lastError: raw['lastError'] as string | undefined };
  }

  async clearPushToken(request: ClearPushTokenRequest): Promise<PushLifecycleStatus> {
    const raw = await invokeMap(this.bridge, NativeCallMap.sdkClearPushToken, { provider: request.provider });
    return { configuredProviders: raw['configuredProviders'] as number, pendingRevocations: raw['pendingRevocations'] as number, registeredProviders: raw['registeredProviders'] as number, state: raw['state'] as string, lastError: raw['lastError'] as string | undefined };
  }

  async setPushPermission(request: SetPushPermissionRequest): Promise<PushLifecycleStatus> {
    const raw = await invokeMap(this.bridge, NativeCallMap.sdkSetPushPermission, { allowed: request.allowed });
    return { configuredProviders: raw['configuredProviders'] as number, pendingRevocations: raw['pendingRevocations'] as number, registeredProviders: raw['registeredProviders'] as number, state: raw['state'] as string, lastError: raw['lastError'] as string | undefined };
  }

  async reconcilePush(): Promise<PushLifecycleStatus> {
    const raw = await invokeMap(this.bridge, NativeCallMap.sdkReconcilePush);
    return { configuredProviders: raw['configuredProviders'] as number, pendingRevocations: raw['pendingRevocations'] as number, registeredProviders: raw['registeredProviders'] as number, state: raw['state'] as string, lastError: raw['lastError'] as string | undefined };
  }

  async pushStatus(): Promise<PushLifecycleStatus> {
    const raw = await invokeMap(this.bridge, NativeCallMap.sdkPushStatus);
    return { configuredProviders: raw['configuredProviders'] as number, pendingRevocations: raw['pendingRevocations'] as number, registeredProviders: raw['registeredProviders'] as number, state: raw['state'] as string, lastError: raw['lastError'] as string | undefined };
  }

  /** HarmonyOS has no platform push module yet: remote pushes are not handed to the core here. */
  async handleRemoteMessage(request: HandleRemoteMessageRequest): Promise<RemoteMessageOutcome> {
    throw new FlareSdkException(
      SdkErrorCodes.capabilityUnavailable,
      'remote push handling is not available on HarmonyOS yet',
      'sdk.handle_remote_message',
    );
  }

  /** HarmonyOS has no platform push module yet: notification taps are not resolved here. */
  async resolveNotificationTap(request: ResolveNotificationTapRequest): Promise<NotificationTapResult> {
    throw new FlareSdkException(
      SdkErrorCodes.capabilityUnavailable,
      'remote push handling is not available on HarmonyOS yet',
      'sdk.resolve_notification_tap',
    );
  }

  async logout(): Promise<void> {
    await invokeVoid(this.bridge, NativeCallMap.sdkLogout);
    emitLifecycleEvent(this.events as DefaultEventsApi, LifecycleEventName.LoggedOut, "sdk.logout");
  }
  async dispose(): Promise<void> {
    this.uninstallPlatformLifecycle();
    await this.events.unsubscribeAll();
    await invokeVoid(this.bridge, NativeCallMap.sdkDispose);
    emitLifecycleEvent(this.events as DefaultEventsApi, LifecycleEventName.Disposed, "sdk.dispose");
  }
  async hardReset(): Promise<void> {
    this.uninstallPlatformLifecycle();
    await invokeVoid(this.bridge, NativeCallMap.sdkHardReset);
  }
  async currentUserId(): Promise<Record<string, Object>> {
    return await invokeMap(this.bridge, NativeCallMap.sdkCurrentUserId);
  }
  async isConnected(): Promise<boolean> {
    return await invokeBool(this.bridge, NativeCallMap.sdkIsConnected);
  }
  async sessionActive(): Promise<boolean> {
    return await invokeBool(this.bridge, NativeCallMap.sdkSessionActive);
  }

  /** Installs the PlatformLifecycle (replacing a previous one) unless init turned it off. */
  private replacePlatformLifecycle(install: boolean): void {
    this.uninstallPlatformLifecycle();
    if (install) {
      this.platformLifecycle = new PlatformLifecycle(this).install();
    }
  }

  private uninstallPlatformLifecycle(): void {
    const installed = this.platformLifecycle;
    this.platformLifecycle = null;
    if (installed !== null) {
      installed.dispose();
    }
  }
}
