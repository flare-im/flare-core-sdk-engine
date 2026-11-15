package com.flare.im.adapter

/** GENERATED. Do not edit by hand. */

import com.flare.im.adapter.codec.*
import com.flare.im.adapter.module.*
import com.flare.im.api.FlareImClient
import com.flare.im.api.connection.ConnectionApi
import com.flare.im.api.access.AccessApi
import com.flare.im.api.update.UpdateApi
import com.flare.im.api.lifecycle.LifecycleApi
import com.flare.im.api.conversations.ConversationsApi
import com.flare.im.api.messagebuilder.MessageBuilderApi
import com.flare.im.api.messages.MessagesApi
import com.flare.im.api.sync.SyncApi
import com.flare.im.api.user.UserApi
import com.flare.im.api.presence.PresenceApi
import com.flare.im.api.media.MediaApi
import com.flare.im.api.capabilities.CapabilitiesApi
import com.flare.im.api.views.ViewsApi
import com.flare.im.api.events.EventsApi
import com.flare.im.api.diagnostics.DiagnosticsApi
import com.flare.im.api.notification.NotificationApi
import com.flare.im.contract.ConnectTokenRefresher
import com.flare.im.contract.NativeBridge
import com.flare.im.contract.NativeCallMap
import com.flare.im.lifecycle.PlatformLifecycle
import com.flare.im.lifecycle.PlatformLifecycleFactory
import com.flare.im.model.catalog.*
import com.flare.im.model.command.*
import com.flare.im.model.command.message.*
import com.flare.im.model.command.message.build.*
import com.flare.im.model.common.enums.*
import com.flare.im.model.common.error.*
import com.flare.im.model.content.*
import com.flare.im.model.entity.*
import com.flare.im.model.event.*
import com.flare.im.model.event.access.*
import com.flare.im.model.event.connection.*
import com.flare.im.model.event.conversation.*
import com.flare.im.model.event.lifecycle.*
import com.flare.im.model.event.message.*
import com.flare.im.model.event.plugin.*
import com.flare.im.model.event.presence.*
import com.flare.im.model.event.progress.*
import com.flare.im.model.event.sync.*
import com.flare.im.model.media.*
import com.flare.im.model.query.*
import com.flare.im.model.response.*

/**
 * [platformLifecycle] builds the [PlatformLifecycle] `init` installs (`PlatformLifecycle.system(context)`
 * for the running app); without it network and foreground reporting is left to the caller.
 */
class DefaultFlareImClient(
    private val bridge: NativeBridge,
    private val platformLifecycle: PlatformLifecycleFactory? = null,
) : FlareImClient {
    private val platformLifecycleLock = Any()
    private var installedLifecycle: PlatformLifecycle? = null

    override val connection: ConnectionApi = DefaultConnectionApi(bridge)
    override val access: AccessApi = DefaultAccessApi(bridge)
    override val update: UpdateApi = DefaultUpdateApi(bridge)
    override val lifecycle: LifecycleApi = DefaultLifecycleApi(bridge)
    override val conversations: ConversationsApi = DefaultConversationsApi(bridge)
    override val messageBuilder: MessageBuilderApi = DefaultMessageBuilderApi(bridge)
    override val messages: MessagesApi = DefaultMessagesApi(bridge)
    override val sync: SyncApi = DefaultSyncApi(bridge)
    override val user: UserApi = DefaultUserApi(bridge)
    override val presence: PresenceApi = DefaultPresenceApi(bridge)
    override val media: MediaApi = DefaultMediaApi(bridge)
    override val capabilities: CapabilitiesApi = DefaultCapabilitiesApi(bridge)
    override val views: ViewsApi = DefaultViewsApi(bridge)
    override val events: EventsApi = DefaultEventsApi(bridge)
    override val diagnostics: DiagnosticsApi = DefaultDiagnosticsApi(bridge)
    override val notification: NotificationApi = DefaultNotificationApi(bridge)

    override suspend fun create(request: Map<String, Any?>): Map<String, Any?> {
        return invokeMap(bridge, NativeCallMap.SDK_CREATE, request)
    }
    /**
     * `installPlatformLifecycle` (top level of the request, default true) is read here and never
     * sent to the core: `false` leaves network and foreground reporting to the caller (tests).
     */
    override suspend fun init(request: Map<String, Any?>): Unit {
        (events as DefaultEventsApi).emitLifecycleEvent(
            LifecycleEventName.INITIALIZING,
            "sdk.init",
        )
        val install = request["installPlatformLifecycle"] as? Boolean ?: true
        try {
            invokeUnit(bridge, NativeCallMap.SDK_INIT, request - "installPlatformLifecycle")
            replacePlatformLifecycle(install)
            (events as DefaultEventsApi).emitLifecycleEvent(
                LifecycleEventName.INITIALIZED,
                "sdk.init",
            )
        } catch (error: Throwable) {
            (events as DefaultEventsApi).emitLifecycleEvent(
                LifecycleEventName.INIT_FAILED,
                "sdk.init",
                error = sdkErrorPayloadFromThrowable(error, "sdk.init"),
            )
            throw error
        }
    }
    override suspend fun uninit(): Unit {
        uninstallPlatformLifecycle()
        invokeUnit(bridge, NativeCallMap.SDK_UNINIT)
    }
    override suspend fun login(request: Map<String, Any?>): Unit {
        val userId = userIdFromRequest(request)
        try {
            invokeUnit(bridge, NativeCallMap.SDK_LOGIN, request)
            (events as DefaultEventsApi).emitLifecycleEvent(
                LifecycleEventName.LOGIN_SUCCEEDED,
                "sdk.login",
                userId = userId,
            )
        } catch (error: Throwable) {
            (events as DefaultEventsApi).emitLifecycleEvent(
                LifecycleEventName.LOGIN_FAILED,
                "sdk.login",
                userId = userId,
                error = sdkErrorPayloadFromThrowable(error, "sdk.login"),
            )
            throw error
        }
    }
    override suspend fun prepare(request: Map<String, Any?>): Unit {
        invokeUnit(bridge, NativeCallMap.SDK_PREPARE, request)
    }
    override suspend fun connect(request: Map<String, Any?>): Unit {
        invokeUnit(bridge, NativeCallMap.SDK_CONNECT, request)
    }
    override suspend fun updateAccessToken(request: Map<String, Any?>): Unit {
        invokeUnit(bridge, NativeCallMap.SDK_UPDATE_ACCESS_TOKEN, request)
    }
    /** Hands the refresher to the bridge, which answers the core's requests with it. */
    override suspend fun setConnectTokenRefresher(refresher: ConnectTokenRefresher?): Unit {
        bridge.setConnectTokenRefresher(refresher)
    }
    override suspend fun setHeartbeatNatTimeout(request: SetHeartbeatNatTimeoutRequest): Unit {
        invokeUnit(bridge, NativeCallMap.SDK_SET_HEARTBEAT_NAT_TIMEOUT, setHeartbeatNatTimeoutRequestToMap(request))
    }
    override suspend fun heartbeatEffectiveInterval(): HeartbeatEffectiveIntervalResponse {
        return heartbeatEffectiveIntervalResponseFromJson(invokeMap(bridge, NativeCallMap.SDK_HEARTBEAT_EFFECTIVE_INTERVAL))
    }
    override suspend fun updatePushToken(request: PushTokenInput): PushLifecycleStatus {
        val raw = invokeMap(bridge, NativeCallMap.SDK_UPDATE_PUSH_TOKEN, pushTokenInputToMap(request))
        return PushLifecycleStatus(configuredProviders = java.lang.Math.toIntExact(intValue(raw["configuredProviders"])), pendingRevocations = java.lang.Math.toIntExact(intValue(raw["pendingRevocations"])), registeredProviders = java.lang.Math.toIntExact(intValue(raw["registeredProviders"])), state = raw["state"] as String, lastError = raw["lastError"] as? String)
    }

    override suspend fun clearPushToken(request: ClearPushTokenRequest): PushLifecycleStatus {
        val raw = invokeMap(bridge, NativeCallMap.SDK_CLEAR_PUSH_TOKEN, mapOf("provider" to request.provider))
        return PushLifecycleStatus(configuredProviders = java.lang.Math.toIntExact(intValue(raw["configuredProviders"])), pendingRevocations = java.lang.Math.toIntExact(intValue(raw["pendingRevocations"])), registeredProviders = java.lang.Math.toIntExact(intValue(raw["registeredProviders"])), state = raw["state"] as String, lastError = raw["lastError"] as? String)
    }

    override suspend fun setPushPermission(request: SetPushPermissionRequest): PushLifecycleStatus {
        val raw = invokeMap(bridge, NativeCallMap.SDK_SET_PUSH_PERMISSION, mapOf("allowed" to request.allowed))
        return PushLifecycleStatus(configuredProviders = java.lang.Math.toIntExact(intValue(raw["configuredProviders"])), pendingRevocations = java.lang.Math.toIntExact(intValue(raw["pendingRevocations"])), registeredProviders = java.lang.Math.toIntExact(intValue(raw["registeredProviders"])), state = raw["state"] as String, lastError = raw["lastError"] as? String)
    }

    override suspend fun reconcilePush(): PushLifecycleStatus {
        val raw = invokeMap(bridge, NativeCallMap.SDK_RECONCILE_PUSH)
        return PushLifecycleStatus(configuredProviders = java.lang.Math.toIntExact(intValue(raw["configuredProviders"])), pendingRevocations = java.lang.Math.toIntExact(intValue(raw["pendingRevocations"])), registeredProviders = java.lang.Math.toIntExact(intValue(raw["registeredProviders"])), state = raw["state"] as String, lastError = raw["lastError"] as? String)
    }

    override suspend fun pushStatus(): PushLifecycleStatus {
        val raw = invokeMap(bridge, NativeCallMap.SDK_PUSH_STATUS)
        return PushLifecycleStatus(configuredProviders = java.lang.Math.toIntExact(intValue(raw["configuredProviders"])), pendingRevocations = java.lang.Math.toIntExact(intValue(raw["pendingRevocations"])), registeredProviders = java.lang.Math.toIntExact(intValue(raw["registeredProviders"])), state = raw["state"] as String, lastError = raw["lastError"] as? String)
    }

    override suspend fun handleRemoteMessage(request: HandleRemoteMessageRequest): RemoteMessageOutcome {
        return remoteMessageOutcomeFromJson(invokeMap(bridge, NativeCallMap.SDK_HANDLE_REMOTE_MESSAGE, handleRemoteMessageRequestToMap(request)))
    }

    override suspend fun resolveNotificationTap(request: ResolveNotificationTapRequest): NotificationTapResult {
        return notificationTapResultFromJson(invokeMap(bridge, NativeCallMap.SDK_RESOLVE_NOTIFICATION_TAP, resolveNotificationTapRequestToMap(request)))
    }

    override suspend fun logout(): Unit {
        invokeUnit(bridge, NativeCallMap.SDK_LOGOUT)
        (events as DefaultEventsApi).emitLifecycleEvent(
            LifecycleEventName.LOGGED_OUT,
            "sdk.logout",
        )
    }
    override suspend fun dispose(): Unit {
        uninstallPlatformLifecycle()
        events.unsubscribeAll()
        invokeUnit(bridge, NativeCallMap.SDK_DISPOSE)
        (events as DefaultEventsApi).emitLifecycleEvent(
            LifecycleEventName.DISPOSED,
            "sdk.dispose",
        )
    }
    override suspend fun hardReset(): Unit {
        uninstallPlatformLifecycle()
        invokeUnit(bridge, NativeCallMap.SDK_HARD_RESET)
    }
    override suspend fun currentUserId(): Map<String, Any?> {
        return invokeMap(bridge, NativeCallMap.SDK_CURRENT_USER_ID)
    }
    override suspend fun isConnected(): Boolean {
        return invokeBool(bridge, NativeCallMap.SDK_IS_CONNECTED)
    }
    override suspend fun sessionActive(): Boolean {
        return invokeBool(bridge, NativeCallMap.SDK_SESSION_ACTIVE)
    }

    /** Installs the platform lifecycle (replacing a previous one) unless init turned it off. */
    private fun replacePlatformLifecycle(install: Boolean) {
        val next = if (install) platformLifecycle?.create(connection, lifecycle) else null
        val previous = synchronized(platformLifecycleLock) {
            installedLifecycle.also { installedLifecycle = next }
        }
        previous?.dispose()
        next?.install()
    }

    private fun uninstallPlatformLifecycle() {
        val previous = synchronized(platformLifecycleLock) {
            installedLifecycle.also { installedLifecycle = null }
        }
        previous?.dispose()
    }
}
