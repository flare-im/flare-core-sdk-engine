package com.flare.im.adapter.module

/** GENERATED. Do not edit by hand. */

import com.flare.im.adapter.codec.*
import com.flare.im.api.events.EventsApi
import com.flare.im.bridge.FlareSdkException
import com.flare.im.bridge.JniNativeBridge
import com.flare.im.contract.NativeBridge
import com.flare.im.contract.NativeCallMap
import com.flare.im.contract.SdkErrorCodes
import com.flare.im.listener.*
import com.flare.im.model.catalog.*
import com.flare.im.model.command.*
import com.flare.im.model.command.message.*
import com.flare.im.model.command.message.build.*
import com.flare.im.model.common.enums.*
import com.flare.im.model.common.error.*
import com.flare.im.model.content.*
import com.flare.im.model.entity.*
import com.flare.im.model.event.*
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

class DefaultEventsApi(
    private val bridge: NativeBridge,
) : EventsApi {

    private val subscriptions = linkedMapOf<Int, DefaultEventSubscription>()
    private var nextId = 1

    init {
        if (bridge is JniNativeBridge) {
            bridge.registerEventSink(::emitNativeEvent)
        }
    }

    override suspend fun subscribeEvents(request: Map<String, Any?>): Map<String, Any?> =
        bridge.invoke(NativeCallMap.EVENT_SUBSCRIBE, request)

    override suspend fun subscribeEventsBatch(request: Map<String, Any?>): Map<String, Any?> =
        bridge.invoke(NativeCallMap.EVENT_SUBSCRIBE_BATCH, request)

    override suspend fun unsubscribe(request: Map<String, Any?>): Unit =
        bridge.invoke(NativeCallMap.EVENT_UNSUBSCRIBE, request)

    override suspend fun unsubscribeAll(): Unit {
        subscriptions.clear()
        bridge.invoke<Unit>(NativeCallMap.EVENT_UNSUBSCRIBE_ALL)
    }

    fun emit(event: Any) {
        val snapshot = subscriptions.values.toList()
        for (subscription in snapshot) {
            dispatchSafely {
                when (val handler = subscription.handler) {
                    is FlareImEventListener -> dispatchToListener(handler, event)
                    is Function1<*, *> -> {
                        @Suppress("UNCHECKED_CAST")
                        (handler as EventCallback<Any>).invoke(event)
                    }
                }
            }
        }
    }

    private fun dispatchSafely(dispatch: () -> Unit) {
        try {
            dispatch()
        } catch (error: Throwable) {
            System.err.println("flare-core event listener failed: ${error.message}")
        }
    }

    override fun addEventListener(listener: FlareImEventListener): EventSubscription =
        register(listener)

    override fun removeEventListener(subscription: EventSubscription) {
        subscription.unsubscribe()
    }

    override fun onInitializing(listener: EventCallback<LifecycleEvent>): EventSubscription =
        registerTyped(LifecycleEvent::class.java, listener)

    override fun onInitialized(listener: EventCallback<LifecycleEvent>): EventSubscription =
        registerTyped(LifecycleEvent::class.java, listener)

    override fun onInitFailed(listener: EventCallback<LifecycleEvent>): EventSubscription =
        registerTyped(LifecycleEvent::class.java, listener)

    override fun onLoginSucceeded(listener: EventCallback<LifecycleEvent>): EventSubscription =
        registerTyped(LifecycleEvent::class.java, listener)

    override fun onLoginFailed(listener: EventCallback<LifecycleEvent>): EventSubscription =
        registerTyped(LifecycleEvent::class.java, listener)

    override fun onLoggedOut(listener: EventCallback<LifecycleEvent>): EventSubscription =
        registerTyped(LifecycleEvent::class.java, listener)

    override fun onDisposed(listener: EventCallback<LifecycleEvent>): EventSubscription =
        registerTyped(LifecycleEvent::class.java, listener)

    override fun onConnecting(listener: EventCallback<ConnectionEvent>): EventSubscription =
        registerTyped(ConnectionEvent::class.java, listener)

    override fun onConnectSuccess(listener: EventCallback<ConnectionEvent>): EventSubscription =
        registerTyped(ConnectionEvent::class.java, listener)

    override fun onConnectReady(listener: EventCallback<ConnectionEvent>): EventSubscription =
        registerTyped(ConnectionEvent::class.java, listener)

    override fun onConnectFailed(listener: EventCallback<ConnectionEvent>): EventSubscription =
        registerTyped(ConnectionEvent::class.java, listener)

    override fun onDisconnected(listener: EventCallback<ConnectionEvent>): EventSubscription =
        registerTyped(ConnectionEvent::class.java, listener)

    override fun onReconnecting(listener: EventCallback<ConnectionEvent>): EventSubscription =
        registerTyped(ConnectionEvent::class.java, listener)

    override fun onReconnectFailed(listener: EventCallback<ConnectionEvent>): EventSubscription =
        registerTyped(ConnectionEvent::class.java, listener)

    override fun onKickedOffline(listener: EventCallback<ConnectionEvent>): EventSubscription =
        registerTyped(ConnectionEvent::class.java, listener)

    override fun onUserTokenExpired(listener: EventCallback<ConnectionEvent>): EventSubscription =
        registerTyped(ConnectionEvent::class.java, listener)

    override fun onMessageReceived(listener: EventCallback<MessageReceivedEvent>): EventSubscription =
        registerTyped(MessageReceivedEvent::class.java, listener)

    override fun onMessageReceivedBatch(listener: EventCallback<MessageReceivedBatchEvent>): EventSubscription =
        registerTyped(MessageReceivedBatchEvent::class.java, listener)

    override fun onMessageSendAck(listener: EventCallback<MessageSendAckEvent>): EventSubscription =
        registerTyped(MessageSendAckEvent::class.java, listener)

    override fun onMessageSendFailed(listener: EventCallback<MessageSendFailedEvent>): EventSubscription =
        registerTyped(MessageSendFailedEvent::class.java, listener)

    override fun onMessageRecalled(listener: EventCallback<MessageMutationEvent>): EventSubscription =
        registerTyped(MessageMutationEvent::class.java, listener)

    override fun onMessageEdited(listener: EventCallback<MessageMutationEvent>): EventSubscription =
        registerTyped(MessageMutationEvent::class.java, listener)

    override fun onMessageDeleted(listener: EventCallback<MessageMutationEvent>): EventSubscription =
        registerTyped(MessageMutationEvent::class.java, listener)

    override fun onMessageReadReceipt(listener: EventCallback<ReadReceiptEvent>): EventSubscription =
        registerTyped(ReadReceiptEvent::class.java, listener)

    override fun onMessageReactionChanged(listener: EventCallback<ReactionChangedEvent>): EventSubscription =
        registerTyped(ReactionChangedEvent::class.java, listener)

    override fun onInputStatusChanged(listener: EventCallback<TypingEvent>): EventSubscription =
        registerTyped(TypingEvent::class.java, listener)

    override fun onTypingAggregateChanged(listener: EventCallback<TypingAggregateEvent>): EventSubscription =
        registerTyped(TypingAggregateEvent::class.java, listener)

    override fun onMessageBurned(listener: EventCallback<MessageMutationEvent>): EventSubscription =
        registerTyped(MessageMutationEvent::class.java, listener)

    override fun onMessagePinned(listener: EventCallback<MessageMutationEvent>): EventSubscription =
        registerTyped(MessageMutationEvent::class.java, listener)

    override fun onMessageUnpinned(listener: EventCallback<MessageMutationEvent>): EventSubscription =
        registerTyped(MessageMutationEvent::class.java, listener)

    override fun onMessagePluginStateChanged(listener: EventCallback<MessageMutationEvent>): EventSubscription =
        registerTyped(MessageMutationEvent::class.java, listener)

    override fun onViewUpdated(listener: EventCallback<ViewUpdate>): EventSubscription =
        registerTyped(ViewUpdate::class.java, listener)

    override fun onNewConversation(listener: EventCallback<ConversationEvent>): EventSubscription =
        registerTyped(ConversationEvent::class.java, listener)

    override fun onConversationChanged(listener: EventCallback<ConversationEvent>): EventSubscription =
        registerTyped(ConversationEvent::class.java, listener)

    override fun onTotalUnreadMessageCountChanged(listener: EventCallback<ConversationEvent>): EventSubscription =
        registerTyped(ConversationEvent::class.java, listener)

    override fun onConversationDeleted(listener: EventCallback<ConversationEvent>): EventSubscription =
        registerTyped(ConversationEvent::class.java, listener)

    override fun onSyncServerStart(listener: EventCallback<SyncEvent>): EventSubscription =
        registerTyped(SyncEvent::class.java, listener)

    override fun onSyncServerFinish(listener: EventCallback<SyncEvent>): EventSubscription =
        registerTyped(SyncEvent::class.java, listener)

    override fun onSyncServerFailed(listener: EventCallback<SyncEvent>): EventSubscription =
        registerTyped(SyncEvent::class.java, listener)

    override fun onSyncProgress(listener: EventCallback<ProgressEvent>): EventSubscription =
        registerProgress(ProgressEventName.SYNC_PROGRESS, listener)

    override fun onUploadProgress(listener: EventCallback<ProgressEvent>): EventSubscription =
        registerProgress(ProgressEventName.UPLOAD_PROGRESS, listener)

    override fun onDownloadProgress(listener: EventCallback<ProgressEvent>): EventSubscription =
        registerProgress(ProgressEventName.DOWNLOAD_PROGRESS, listener)

    override fun onPluginRealtime(listener: EventCallback<PluginRealtimeEvent>): EventSubscription =
        registerTyped(PluginRealtimeEvent::class.java, listener)

    override fun onPluginCatalogChanged(listener: EventCallback<PluginCatalogChangedEvent>): EventSubscription =
        registerTyped(PluginCatalogChangedEvent::class.java, listener)

    private fun register(handler: Any): EventSubscription {
        val subscriptionId = nextId++
        val subscription = DefaultEventSubscription(
            id = subscriptionId.toString(),
            onDispose = { subscriptions.remove(subscriptionId) },
            handler = handler,
        )
        subscriptions[subscriptionId] = subscription
        return subscription
    }

    private fun <T : Any> registerTyped(eventClass: Class<T>, listener: EventCallback<T>): EventSubscription =
        register { event: Any ->
            if (eventClass.isInstance(event)) {
                val typedEvent = eventClass.cast(event) ?: return@register
                listener.invoke(typedEvent)
            }
        }

    private fun registerProgress(name: ProgressEventName, listener: EventCallback<ProgressEvent>): EventSubscription =
        register { event: Any ->
            when (event) {
                is ProgressEvent -> {
                    if (event.name == name) {
                        listener.invoke(event)
                    }
                }
                is SyncEvent -> {
                    if (name == ProgressEventName.SYNC_PROGRESS && event.name == SyncEventName.PROGRESS) {
                        listener.invoke(progressEventFromSync(event))
                    }
                }
            }
        }

    fun emitNativeEvent(eventType: Int, payload: Map<String, Any?>) {
        emit(nativeEventFromCode(eventType, payload))
    }

    private fun dispatchToListener(listener: FlareImEventListener, event: Any) {
        when (event) {
            is LifecycleEvent -> when (event.name) {
                LifecycleEventName.INITIALIZING -> listener.onInitializing(event)
                LifecycleEventName.INITIALIZED -> listener.onInitialized(event)
                LifecycleEventName.INIT_FAILED -> listener.onInitFailed(event)
                LifecycleEventName.LOGIN_SUCCEEDED -> listener.onLoginSucceeded(event)
                LifecycleEventName.LOGIN_FAILED -> listener.onLoginFailed(event)
                LifecycleEventName.LOGGED_OUT -> listener.onLoggedOut(event)
                LifecycleEventName.DISPOSED -> listener.onDisposed(event)
            }
            is ConnectionEvent -> when (event.name) {
                ConnectionEventName.CONNECTING -> listener.onConnecting(event)
                ConnectionEventName.CONNECTED -> listener.onConnectSuccess(event)
                ConnectionEventName.READY -> listener.onConnectReady(event)
                ConnectionEventName.DISCONNECTED -> listener.onDisconnected(event)
                ConnectionEventName.RECONNECTING -> listener.onReconnecting(event)
                ConnectionEventName.RECONNECT_FAILED -> listener.onReconnectFailed(event)
                ConnectionEventName.KICKED_OFF -> listener.onKickedOffline(event)
                ConnectionEventName.TOKEN_EXPIRED -> listener.onUserTokenExpired(event)
                ConnectionEventName.STATE_CHANGED,
                ConnectionEventName.SYNC_STATE_CHANGED,
                ConnectionEventName.SERVER_ERROR -> Unit
            }
            is MessageReceivedEvent -> listener.onMessageReceived(event)
            is MessageReceivedBatchEvent -> listener.onMessageReceivedBatch(event)
            is MessageSendAckEvent -> listener.onMessageSendAck(event)
            is MessageSendFailedEvent -> listener.onMessageSendFailed(event)
            is MessageMutationEvent -> when (event.name) {
                MessageEventName.RECALLED -> listener.onMessageRecalled(event)
                MessageEventName.EDITED -> listener.onMessageEdited(event)
                MessageEventName.DELETED -> listener.onMessageDeleted(event)
                MessageEventName.BURNED,
                MessageEventName.RETENTION_EXPIRED,
                MessageEventName.RETENTION_PURGED -> listener.onMessageBurned(event)
                MessageEventName.PINNED -> listener.onMessagePinned(event)
                MessageEventName.UNPINNED -> listener.onMessageUnpinned(event)
                MessageEventName.PLUGIN_STATE_CHANGED -> listener.onMessagePluginStateChanged(event)
                MessageEventName.RECEIVED,
                MessageEventName.RECEIVED_BATCH,
                MessageEventName.SEND_ACK,
                MessageEventName.SEND_FAILED,
                MessageEventName.TYPING,
                MessageEventName.TYPING_AGGREGATE,
                MessageEventName.REACTION_CHANGED,
                MessageEventName.READ_RECEIPT,
                MessageEventName.BURN_SCHEDULED,
                MessageEventName.HARD_DELETED,
                MessageEventName.MARKED,
                MessageEventName.UNMARKED,
                MessageEventName.RETENTION_SCHEDULED,
                MessageEventName.PRESENCE_CHANGED,
                MessageEventName.CUSTOM,
                MessageEventName.READ_SUMMARIES_CHANGED -> Unit
            }
            is ReadReceiptEvent -> listener.onMessageReadReceipt(event)
            is ReactionChangedEvent -> listener.onMessageReactionChanged(event)
            is TypingEvent -> listener.onInputStatusChanged(event)
            is TypingAggregateEvent -> listener.onTypingAggregateChanged(event)
            is ConversationEvent -> when (event.name) {
                ConversationEventName.CREATED -> listener.onNewConversation(event)
                ConversationEventName.DELETED -> listener.onConversationDeleted(event)
                ConversationEventName.UNREAD_COUNT_CHANGED -> listener.onTotalUnreadMessageCountChanged(event)
                ConversationEventName.SYNCED,
                ConversationEventName.UPDATED -> listener.onConversationChanged(event)
            }
            is SyncEvent -> when (event.name) {
                SyncEventName.STARTED -> listener.onSyncServerStart(event)
                SyncEventName.FINISHED -> listener.onSyncServerFinish(event)
                SyncEventName.FAILED -> listener.onSyncServerFailed(event)
                SyncEventName.PROGRESS -> listener.onSyncProgress(progressEventFromSync(event))
                SyncEventName.STATE_CHANGED,
                SyncEventName.TASK_COMPLETED,
                SyncEventName.RESYNC_NEEDED,
                SyncEventName.READINESS -> Unit
            }
            is ProgressEvent -> when (event.name) {
                ProgressEventName.SYNC_PROGRESS -> listener.onSyncProgress(event)
                ProgressEventName.UPLOAD_PROGRESS -> listener.onUploadProgress(event)
                ProgressEventName.DOWNLOAD_PROGRESS -> listener.onDownloadProgress(event)
            }
            is ViewUpdate -> listener.onViewUpdated(event)
            is PluginRealtimeEvent -> listener.onPluginRealtime(event)
            is PluginCatalogChangedEvent -> listener.onPluginCatalogChanged(event)
        }
    }
}

private class DefaultEventSubscription(
    override val id: String,
    private val onDispose: () -> Unit,
    var handler: Any? = null,
) : EventSubscription {
    override fun unsubscribe() = onDispose()
}

private fun nativeEventFromCode(eventType: Int, payload: Map<String, Any?>): Any =
    when (eventType) {
        EventCode.CONNECTION_CONNECTED,
        EventCode.CONNECTION_DISCONNECTED,
        EventCode.CONNECTION_RECONNECTING,
        EventCode.CONNECTION_STATE_CHANGED,
        EventCode.CONNECTION_SYNC_STATE_CHANGED,
        EventCode.CONNECTION_SERVER_ERROR,
        EventCode.CONNECTION_KICKED_OFF,
        EventCode.CONNECTION_TOKEN_EXPIRED -> connectionEventFromCode(eventType, payload)
        EventCode.MESSAGE_SEND_ACK -> MessageSendAckEvent(
            ack = sendAckFromJson(requiredEventMap(payload["ack"], "ack")),
        )
        EventCode.MESSAGE_SEND_FAILED -> MessageSendFailedEvent(
            clientMsgId = requiredEventString(payload["clientMsgId"], "clientMsgId"),
            reason = requiredEventString(payload["reason"], "reason"),
            error = sdkErrorPayloadFromEvent(payload["error"]),
        )
        EventCode.MESSAGE_RECEIVED -> MessageReceivedEvent(
            message = messageFromJson(requiredEventMap(payload["message"], "message")),
        )
        EventCode.MESSAGE_RECEIVED_BATCH -> MessageReceivedBatchEvent(
            messages = requiredEventListOfMaps(payload["messages"], "messages").map(::messageFromJson),
        )
        EventCode.MESSAGE_TYPING -> TypingEvent(
            conversationId = requiredEventString(payload["conversationId"], "conversationId"),
            userId = requiredEventString(payload["userId"], "userId"),
            typing = requiredEventBool(payload["typing"], "typing"),
        )
        EventCode.MESSAGE_TYPING_AGGREGATE -> TypingAggregateEvent(
            conversationId = requiredEventString(payload["conversationId"], "conversationId"),
            typingUserIds = requiredEventStringList(payload["typingUserIds"], "typingUserIds"),
            typingCount = requiredEventInt(payload["typingCount"], "typingCount").toInt(),
        )
        EventCode.MESSAGE_READ_RECEIPT -> ReadReceiptEvent(
            conversationId = requiredEventString(payload["conversationId"], "conversationId"),
            userId = requiredEventString(payload["userId"], "userId"),
            readSeq = requiredPositiveEventInt(payload["readSeq"], "readSeq"),
        )
        EventCode.MESSAGE_REACTION_CHANGED -> ReactionChangedEvent(
            conversationId = requiredEventString(payload["conversationId"], "conversationId"),
            serverMsgId = requiredEventString(payload["serverMsgId"], "serverMsgId"),
            userId = requiredEventString(payload["userId"], "userId"),
            emoji = requiredEventString(payload["emoji"], "emoji"),
            action = requiredEventInt(payload["action"], "action").toInt(),
        )
        EventCode.MESSAGE_RECALLED -> messageMutationFromPayload(MessageEventName.RECALLED, payload)
        EventCode.MESSAGE_EDITED -> messageMutationFromPayload(MessageEventName.EDITED, payload)
        EventCode.MESSAGE_DELETED -> messageMutationFromPayload(MessageEventName.DELETED, payload)
        EventCode.MESSAGE_PINNED -> messageMutationFromPayload(MessageEventName.PINNED, payload)
        EventCode.MESSAGE_UNPINNED -> messageMutationFromPayload(MessageEventName.UNPINNED, payload)
        EventCode.MESSAGE_MARKED -> messageMutationFromPayload(MessageEventName.MARKED, payload)
        EventCode.MESSAGE_UNMARKED -> messageMutationFromPayload(MessageEventName.UNMARKED, payload)
        EventCode.MESSAGE_PLUGIN_STATE_CHANGED -> messageMutationFromPayload(MessageEventName.PLUGIN_STATE_CHANGED, payload)
        EventCode.MESSAGE_RETENTION_SCHEDULED -> messageMutationFromPayload(MessageEventName.RETENTION_SCHEDULED, payload)
        EventCode.MESSAGE_RETENTION_EXPIRED -> messageMutationFromPayload(MessageEventName.RETENTION_EXPIRED, payload)
        EventCode.MESSAGE_RETENTION_PURGED -> messageMutationFromPayload(MessageEventName.RETENTION_PURGED, payload)
        EventCode.CONVERSATION_SYNCED,
        EventCode.CONVERSATION_CREATED,
        EventCode.CONVERSATION_UPDATED,
        EventCode.CONVERSATION_UNREAD_COUNT_CHANGED,
        EventCode.CONVERSATION_DELETED -> conversationEventFromCode(eventType, payload)
        EventCode.SYNC_STARTED,
        EventCode.SYNC_FINISHED,
        EventCode.SYNC_FAILED,
        EventCode.SYNC_PROGRESS,
        EventCode.SYNC_TASK_COMPLETED,
        EventCode.SYNC_STATE_CHANGED,
        EventCode.SYNC_RESYNC_NEEDED -> syncEventFromCode(eventType, payload)
        EventCode.PLUGIN_REALTIME -> PluginRealtimeEvent(
            pluginId = requiredEventString(payload["pluginId"], "pluginId"),
            packetType = requiredEventString(payload["packetType"], "packetType"),
            conversationId = requiredEventString(payload["conversationId"], "conversationId"),
            payload = requiredEventMap(payload["payload"], "payload"),
            deliveryId = requiredEventString(payload["deliveryId"], "deliveryId"),
            expiresAtMs = requiredEventInt(payload["expiresAtMs"], "expiresAtMs"),
        )
        EventCode.PLUGIN_CATALOG_CHANGED -> PluginCatalogChangedEvent(
            version = requiredEventString(payload["version"], "version"),
        )
        EventCode.VIEW_UPDATED -> viewUpdateFromJson(payload)
        else -> unknownEventFromCode(eventType, payload)
    }

private fun unknownEventFromCode(eventType: Int, payload: Map<String, Any?>): Map<String, Any?> =
    mapOf(
        "type" to "unknown",
        "name" to "unknown",
        "event" to "unknown",
        "eventType" to eventType,
        "payload" to payload,
    )

private fun messageMutationFromPayload(
    name: MessageEventName,
    payload: Map<String, Any?>,
): MessageMutationEvent =
    MessageMutationEvent(
        name = name,
        conversationId = requiredEventString(payload["conversationId"], "conversationId"),
        messageId = optionalEventString(payload["messageId"], "messageId"),
        serverMsgId = optionalEventString(payload["serverMsgId"], "serverMsgId"),
        userId = optionalEventString(payload["userId"], "userId"),
        reason = optionalEventString(payload["reason"], "reason"),
    )

private fun connectionEventFromCode(eventType: Int, payload: Map<String, Any?>): ConnectionEvent {
    val name = connectionEventNameFromCode(eventType, payload)
    return ConnectionEvent(
        name = name,
        state = connectionStateForEvent(name, payload["state"]),
        reason = optionalEventString(payload["reason"], "reason"),
        attempt = optionalEventInt(payload["attempt"], "attempt"),
        error = sdkErrorPayloadFromEvent(payload["error"]),
    )
}

private fun connectionEventNameFromCode(
    eventType: Int,
    payload: Map<String, Any?>,
): ConnectionEventName {
    if (eventType == EventCode.CONNECTION_STATE_CHANGED) {
        return when (connectionStateFromWire(payload["state"])) {
            SdkConnectionState.CONNECTING -> ConnectionEventName.CONNECTING
            SdkConnectionState.CONNECTED -> ConnectionEventName.CONNECTED
            SdkConnectionState.READY -> ConnectionEventName.READY
            SdkConnectionState.RECONNECTING -> ConnectionEventName.RECONNECTING
            SdkConnectionState.DISCONNECTED -> ConnectionEventName.DISCONNECTED
        }
    }
    return when (eventType) {
        EventCode.CONNECTION_CONNECTED -> ConnectionEventName.CONNECTED
        EventCode.CONNECTION_DISCONNECTED -> ConnectionEventName.DISCONNECTED
        EventCode.CONNECTION_RECONNECTING -> ConnectionEventName.RECONNECTING
        EventCode.CONNECTION_SYNC_STATE_CHANGED -> ConnectionEventName.SYNC_STATE_CHANGED
        EventCode.CONNECTION_SERVER_ERROR -> ConnectionEventName.SERVER_ERROR
        EventCode.CONNECTION_KICKED_OFF -> ConnectionEventName.KICKED_OFF
        EventCode.CONNECTION_TOKEN_EXPIRED -> ConnectionEventName.TOKEN_EXPIRED
        else -> invalidEventCode(eventType)
    }
}

private fun connectionStateForEvent(name: ConnectionEventName, state: Any?): SdkConnectionState {
    if (state != null) {
        return connectionStateFromWire(state)
    }
    return when (name) {
        ConnectionEventName.CONNECTING -> SdkConnectionState.CONNECTING
        ConnectionEventName.CONNECTED -> SdkConnectionState.CONNECTED
        ConnectionEventName.READY -> SdkConnectionState.READY
        ConnectionEventName.RECONNECTING -> SdkConnectionState.RECONNECTING
        ConnectionEventName.SYNC_STATE_CHANGED -> SdkConnectionState.READY
        ConnectionEventName.SERVER_ERROR -> SdkConnectionState.CONNECTED
        ConnectionEventName.DISCONNECTED,
        ConnectionEventName.RECONNECT_FAILED,
        ConnectionEventName.KICKED_OFF,
        ConnectionEventName.TOKEN_EXPIRED -> SdkConnectionState.DISCONNECTED
        ConnectionEventName.STATE_CHANGED -> invalidEventField("state", "non-empty string")
    }
}

private fun connectionStateFromWire(value: Any?): SdkConnectionState =
    when (val raw = requiredEventString(value, "state").trim().lowercase()) {
        "connecting" -> SdkConnectionState.CONNECTING
        "connected" -> SdkConnectionState.CONNECTED
        "ready" -> SdkConnectionState.READY
        "reconnecting" -> SdkConnectionState.RECONNECTING
        "disconnected" -> SdkConnectionState.DISCONNECTED
        else -> throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "invalid connection state: ${if (raw.isEmpty()) "<empty>" else raw}",
            operation = "event.decode",
            details = mapOf("field" to "state"),
        )
    }

private fun conversationEventFromCode(eventType: Int, payload: Map<String, Any?>): ConversationEvent =
    ConversationEvent(
        name = when (eventType) {
            EventCode.CONVERSATION_SYNCED -> ConversationEventName.SYNCED
            EventCode.CONVERSATION_CREATED -> ConversationEventName.CREATED
            EventCode.CONVERSATION_UPDATED -> ConversationEventName.UPDATED
            EventCode.CONVERSATION_UNREAD_COUNT_CHANGED -> ConversationEventName.UNREAD_COUNT_CHANGED
            EventCode.CONVERSATION_DELETED -> ConversationEventName.DELETED
            else -> invalidEventCode(eventType)
        },
        conversationId = optionalEventString(payload["conversationId"], "conversationId"),
        conversationIds = optionalEventStringList(payload["conversationIds"], "conversationIds"),
        unreadCount = optionalEventInt(payload["unreadCount"], "unreadCount"),
    )

private fun syncEventFromCode(eventType: Int, payload: Map<String, Any?>): SyncEvent =
    SyncEvent(
        name = when (eventType) {
            EventCode.SYNC_STATE_CHANGED -> SyncEventName.STATE_CHANGED
            EventCode.SYNC_STARTED -> SyncEventName.STARTED
            EventCode.SYNC_FINISHED -> SyncEventName.FINISHED
            EventCode.SYNC_FAILED -> SyncEventName.FAILED
            EventCode.SYNC_PROGRESS -> SyncEventName.PROGRESS
            EventCode.SYNC_TASK_COMPLETED -> SyncEventName.TASK_COMPLETED
            EventCode.SYNC_RESYNC_NEEDED -> SyncEventName.RESYNC_NEEDED
            else -> invalidEventCode(eventType)
        },
        trigger = optionalEventString(payload["trigger"], "trigger"),
        phase = optionalEventString(payload["phase"], "phase"),
        task = optionalEventString(payload["task"], "task"),
        progress = optionalEventInt(payload["progress"], "progress"),
        error = sdkErrorPayloadFromEvent(payload["error"]),
    )

private fun progressEventFromSync(event: SyncEvent): ProgressEvent =
    ProgressEvent(
        name = ProgressEventName.SYNC_PROGRESS,
        operation = event.task ?: event.phase ?: "sync",
        current = (event.progress ?: 0).toLong(),
        total = 100L,
    )

private fun sdkErrorPayloadFromEvent(value: Any?): SdkErrorPayload? {
    if (value == null) return null
    val json = requiredEventMap(value, "error")
    return SdkErrorPayload(
        code = requiredEventString(json["code"], "error.code"),
        machineReason = machineReasonFromEvent(json["machineReason"]),
        message = requiredEventString(json["message"], "error.message"),
        operation = optionalEventString(json["operation"], "error.operation"),
        retryable = optionalEventBool(json["retryable"], "error.retryable"),
        details = optionalEventStringMap(json["details"], "error.details"),
    )
}

/** The server's typed reason; empty for a local failure, which reads as no reason. */
private fun machineReasonFromEvent(value: Any?): String? = when (value) {
    null -> null
    is String -> value.ifEmpty { null }
    else -> invalidEventField("error.machineReason", "string")
}

private fun invalidEventCode(eventType: Int): Nothing =
    throw FlareSdkException(
        SdkErrorCodes.INVALIDPARAMETER,
        "invalid event type: $eventType",
        operation = "event.decode",
        details = mapOf("eventType" to eventType.toString()),
    )

private fun invalidEventField(field: String, expected: String): Nothing =
    throw FlareSdkException(
        SdkErrorCodes.INVALIDPARAMETER,
        "invalid event payload field: $field",
        operation = "event.decode",
        details = mapOf("field" to field, "expected" to expected),
    )

private fun requiredEventMap(value: Any?, field: String): Map<String, Any?> {
    if (value !is Map<*, *>) invalidEventField(field, "object")
    return value.entries.associate { (key, item) -> key.toString() to item }
}

private fun requiredEventString(value: Any?, field: String): String {
    if (value is String && value.isNotEmpty()) return value
    invalidEventField(field, "non-empty string")
}

private fun optionalEventString(value: Any?, field: String): String? {
    if (value == null) return null
    return requiredEventString(value, field)
}

private fun requiredEventInt(value: Any?, field: String): Long {
    val number = value as? Number ?: invalidEventField(field, "unsigned integer")
    val doubleValue = number.toDouble()
    val longValue = number.toLong()
    if (doubleValue >= 0.0 && !doubleValue.isNaN() && !doubleValue.isInfinite() && longValue.toDouble() == doubleValue) {
        return longValue
    }
    invalidEventField(field, "unsigned integer")
}

private fun requiredPositiveEventInt(value: Any?, field: String): Long {
    val parsed = requiredEventInt(value, field)
    if (parsed > 0) return parsed
    invalidEventField(field, "positive integer")
}

private fun optionalEventInt(value: Any?, field: String): Int? {
    if (value == null) return null
    return requiredEventInt(value, field).toInt()
}

private fun requiredEventBool(value: Any?, field: String): Boolean {
    if (value is Boolean) return value
    invalidEventField(field, "boolean")
}

private fun optionalEventBool(value: Any?, field: String): Boolean? {
    if (value == null) return null
    return requiredEventBool(value, field)
}

private fun requiredEventListOfMaps(value: Any?, field: String): List<Map<String, Any?>> {
    if (value !is List<*>) invalidEventField(field, "array")
    return value.mapIndexed { index, item -> requiredEventMap(item, "$field.$index") }
}

private fun optionalEventStringList(value: Any?, field: String): List<String> {
    if (value == null) return emptyList()
    if (value !is List<*>) invalidEventField(field, "array")
    return value.mapIndexed { index, item -> requiredEventString(item, "$field.$index") }
}

private fun requiredEventStringList(value: Any?, field: String): List<String> {
    if (value !is List<*>) invalidEventField(field, "array")
    return value.mapIndexed { index, item -> requiredEventString(item, "$field.$index") }
}

private fun optionalEventStringMap(value: Any?, field: String): Map<String, String> {
    if (value == null) return emptyMap()
    val json = requiredEventMap(value, field)
    return json.entries.associate { (key, item) -> key to requiredEventString(item, "$field.$key") }
}
