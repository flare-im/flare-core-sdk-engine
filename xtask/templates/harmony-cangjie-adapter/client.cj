package flare_core_harmony_cangjie_sdk.adapter

import flare_core_harmony_cangjie_sdk.contract.*
import flare_core_harmony_cangjie_sdk.api.modules.*
import flare_core_harmony_cangjie_sdk.model.*
import flare_core_harmony_cangjie_sdk.adapter.*
import flare_core_harmony_cangjie_sdk.lifecycle.*

// GENERATED. Do not edit by hand.
// Typed adapter implementing `api.FlareImClient` (JSON bridge to native).

public class DefaultFlareImClient <: flare_core_harmony_cangjie_sdk.api.FlareImClient {
    private let bridge: NativeBridge
    private let connectionApi: ConnectionApi
    private let accessApi: AccessApi
    private let lifecycleApi: LifecycleApi
    private let conversationsApi: ConversationsApi
    private let messageBuilderApi: MessageBuilderApi
    private let messagesApi: MessagesApi
    private let syncApi: SyncApi
    private let presenceApi: PresenceApi
    private let mediaApi: MediaApi
    private let capabilitiesApi: CapabilitiesApi
    private let eventsApi: EventsApi
    private let diagnosticsApi: DiagnosticsApi
    private let notificationApi: NotificationApi
    private var platformLifecycle: ?PlatformLifecycle = None

    public prop connection: ConnectionApi {
        get() {
            return connectionApi
        }
    }

    public prop access: AccessApi {
        get() {
            return accessApi
        }
    }

    public prop lifecycle: LifecycleApi {
        get() {
            return lifecycleApi
        }
    }

    public prop conversations: ConversationsApi {
        get() {
            return conversationsApi
        }
    }

    public prop messageBuilder: MessageBuilderApi {
        get() {
            return messageBuilderApi
        }
    }

    public prop messages: MessagesApi {
        get() {
            return messagesApi
        }
    }

    public prop sync: SyncApi {
        get() {
            return syncApi
        }
    }

    public prop presence: PresenceApi {
        get() {
            return presenceApi
        }
    }

    public prop media: MediaApi {
        get() {
            return mediaApi
        }
    }

    public prop capabilities: CapabilitiesApi {
        get() {
            return capabilitiesApi
        }
    }

    public prop events: EventsApi {
        get() {
            return eventsApi
        }
    }

    public prop diagnostics: DiagnosticsApi {
        get() {
            return diagnosticsApi
        }
    }

    public prop notification: NotificationApi {
        get() {
            return notificationApi
        }
    }

    public init(bridge: NativeBridge) {
        this.bridge = bridge
        this.connectionApi = DefaultConnectionApi(bridge)
        this.accessApi = DefaultAccessApi(bridge)
        this.lifecycleApi = DefaultLifecycleApi(bridge)
        this.conversationsApi = DefaultConversationsApi(bridge)
        this.messageBuilderApi = DefaultMessageBuilderApi(bridge)
        this.messagesApi = DefaultMessagesApi(bridge)
        this.syncApi = DefaultSyncApi(bridge)
        this.presenceApi = DefaultPresenceApi(bridge)
        this.mediaApi = DefaultMediaApi(bridge)
        this.capabilitiesApi = DefaultCapabilitiesApi(bridge)
        this.eventsApi = DefaultEventsApi(bridge)
        this.diagnosticsApi = DefaultDiagnosticsApi(bridge)
        this.notificationApi = DefaultNotificationApi(bridge)
    }

    public func create(requestJson: String): String {
        return bridge.invoke(descriptor: NativeCallMap.sdkCreate, requestJson: wireEncodeRequest(requestJson))
    }

    // `installPlatformLifecycle` (default true) belongs to the platform package and never reaches the core.
    public func initSdk(requestJson: String): Unit {
        let install = match (jsonOptionalBoolField(jsonObjectFromRaw(requestJson), "installPlatformLifecycle")) {
            case Some(value) => value
            case None => true
        }
        let config = jsonWithoutField(requestJson, "installPlatformLifecycle")
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkInit, requestJson: wireEncodeRequest(config))
        replacePlatformLifecycle(install)
    }

    public func uninit(): Unit {
        uninstallPlatformLifecycle()
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkUninit, requestJson: wireEncodeRequest("{}"))
    }

    public func login(requestJson: String): Unit {
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkLogin, requestJson: wireEncodeRequest(requestJson))
    }

    public func prepare(requestJson: String): Unit {
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkPrepare, requestJson: wireEncodeRequest(requestJson))
    }

    public func connect(requestJson: String): Unit {
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkConnect, requestJson: wireEncodeRequest(requestJson))
    }

    public func updateAccessToken(requestJson: String): Unit {
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkUpdateAccessToken, requestJson: wireEncodeRequest(requestJson))
    }

    // Installs (None removes) the host's connect token refresher; the bridge answers the core's requests.
    public func setConnectTokenRefresher(refresher: ?ConnectTokenRefresher): Unit {
        bridge.setConnectTokenRefresher(refresher)
    }

    public func setHeartbeatNatTimeout(request: SetHeartbeatNatTimeoutRequest): Unit {
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkSetHeartbeatNatTimeout, requestJson: wireEncodeRequest(setHeartbeatNatTimeoutRequestToJson(request)))
    }

    public func heartbeatEffectiveInterval(): HeartbeatEffectiveIntervalResponse {
        let raw = bridge.invoke(descriptor: NativeCallMap.sdkHeartbeatEffectiveInterval, requestJson: wireEncodeRequest("{}"))
        return heartbeatEffectiveIntervalResponseFromJson(raw)
    }

    public func updatePushToken(request: PushTokenInput): PushLifecycleStatus {
        let body = jsonObjectFromRaw("{}")
        jsonPutString(body, "provider", request.provider)
        jsonPutString(body, "platform", request.platform)
        jsonPutString(body, "token", request.token)
        let raw = bridge.invoke(descriptor: NativeCallMap.sdkUpdatePushToken, requestJson: wireEncodeRequest(body.toString()))
        let value = wireDecodeResponseObject(jsonObjectFromRaw(raw))
        let error = match (value.get("lastError")) { case Some(v) => if (v.toString() == "null") { None } else { Some(v.asString().getValue()) }; case None => None }
        return PushLifecycleStatus(configuredProviders: UInt32(jsonIntField(value, "configuredProviders", 0)), lastError: error, pendingRevocations: UInt32(jsonIntField(value, "pendingRevocations", 0)), registeredProviders: UInt32(jsonIntField(value, "registeredProviders", 0)), state: jsonStringField(value, "state", ""))
    }

    public func clearPushToken(request: ClearPushTokenRequest): PushLifecycleStatus {
        let body = jsonObjectFromRaw("{}")
        jsonPutString(body, "provider", request.provider)
        let raw = bridge.invoke(descriptor: NativeCallMap.sdkClearPushToken, requestJson: wireEncodeRequest(body.toString()))
        let value = wireDecodeResponseObject(jsonObjectFromRaw(raw))
        let error = match (value.get("lastError")) { case Some(v) => if (v.toString() == "null") { None } else { Some(v.asString().getValue()) }; case None => None }
        return PushLifecycleStatus(configuredProviders: UInt32(jsonIntField(value, "configuredProviders", 0)), lastError: error, pendingRevocations: UInt32(jsonIntField(value, "pendingRevocations", 0)), registeredProviders: UInt32(jsonIntField(value, "registeredProviders", 0)), state: jsonStringField(value, "state", ""))
    }

    public func setPushPermission(request: SetPushPermissionRequest): PushLifecycleStatus {
        let body = jsonObjectFromRaw("{}")
        jsonPutBool(body, "allowed", request.allowed)
        let raw = bridge.invoke(descriptor: NativeCallMap.sdkSetPushPermission, requestJson: wireEncodeRequest(body.toString()))
        let value = wireDecodeResponseObject(jsonObjectFromRaw(raw))
        let error = match (value.get("lastError")) { case Some(v) => if (v.toString() == "null") { None } else { Some(v.asString().getValue()) }; case None => None }
        return PushLifecycleStatus(configuredProviders: UInt32(jsonIntField(value, "configuredProviders", 0)), lastError: error, pendingRevocations: UInt32(jsonIntField(value, "pendingRevocations", 0)), registeredProviders: UInt32(jsonIntField(value, "registeredProviders", 0)), state: jsonStringField(value, "state", ""))
    }

    public func reconcilePush(): PushLifecycleStatus {
        let body = jsonObjectFromRaw("{}")
        let raw = bridge.invoke(descriptor: NativeCallMap.sdkReconcilePush, requestJson: wireEncodeRequest(body.toString()))
        let value = wireDecodeResponseObject(jsonObjectFromRaw(raw))
        let error = match (value.get("lastError")) { case Some(v) => if (v.toString() == "null") { None } else { Some(v.asString().getValue()) }; case None => None }
        return PushLifecycleStatus(configuredProviders: UInt32(jsonIntField(value, "configuredProviders", 0)), lastError: error, pendingRevocations: UInt32(jsonIntField(value, "pendingRevocations", 0)), registeredProviders: UInt32(jsonIntField(value, "registeredProviders", 0)), state: jsonStringField(value, "state", ""))
    }

    public func pushStatus(): PushLifecycleStatus {
        let body = jsonObjectFromRaw("{}")
        let raw = bridge.invoke(descriptor: NativeCallMap.sdkPushStatus, requestJson: wireEncodeRequest(body.toString()))
        let value = wireDecodeResponseObject(jsonObjectFromRaw(raw))
        let error = match (value.get("lastError")) { case Some(v) => if (v.toString() == "null") { None } else { Some(v.asString().getValue()) }; case None => None }
        return PushLifecycleStatus(configuredProviders: UInt32(jsonIntField(value, "configuredProviders", 0)), lastError: error, pendingRevocations: UInt32(jsonIntField(value, "pendingRevocations", 0)), registeredProviders: UInt32(jsonIntField(value, "registeredProviders", 0)), state: jsonStringField(value, "state", ""))
    }

    // HarmonyOS has no platform push module yet: remote pushes are not handed to the core here.
    public func handleRemoteMessage(request: HandleRemoteMessageRequest): RemoteMessageOutcome {
        throw Exception("FlareSdkException(code=capabilityUnavailable, operation=sdk.handle_remote_message, message=Remote push handling is not available on HarmonyOS yet.)")
    }

    // HarmonyOS has no platform push module yet: notification taps are not resolved here.
    public func resolveNotificationTap(request: ResolveNotificationTapRequest): NotificationTapResult {
        throw Exception("FlareSdkException(code=capabilityUnavailable, operation=sdk.resolve_notification_tap, message=Remote push handling is not available on HarmonyOS yet.)")
    }

    public func logout(): Unit {
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkLogout, requestJson: wireEncodeRequest("{}"))
    }

    public func dispose(): Unit {
        uninstallPlatformLifecycle()
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkDispose, requestJson: wireEncodeRequest("{}"))
    }

    public func hardReset(): Unit {
        uninstallPlatformLifecycle()
        let _ignored = bridge.invoke(descriptor: NativeCallMap.sdkHardReset, requestJson: wireEncodeRequest("{}"))
    }

    public func currentUserId(): String {
        return bridge.invoke(descriptor: NativeCallMap.sdkCurrentUserId, requestJson: wireEncodeRequest("{}"))
    }

    public func isConnected(): BooleanResponse {
        return booleanResponseFromWire(bridge.invoke(descriptor: NativeCallMap.sdkIsConnected, requestJson: wireEncodeRequest("{}")))
    }

    public func sessionActive(): BooleanResponse {
        return booleanResponseFromWire(bridge.invoke(descriptor: NativeCallMap.sdkSessionActive, requestJson: wireEncodeRequest("{}")))
    }

    // Installs the PlatformLifecycle (replacing a previous one) unless init turned it off.
    private func replacePlatformLifecycle(install: Bool): Unit {
        uninstallPlatformLifecycle()
        if (install) {
            platformLifecycle = Some(PlatformLifecycle(this).install())
        }
    }

    private func uninstallPlatformLifecycle(): Unit {
        match (platformLifecycle) {
            case Some(installed) => installed.dispose()
            case None => ()
        }
        platformLifecycle = None
    }
}
