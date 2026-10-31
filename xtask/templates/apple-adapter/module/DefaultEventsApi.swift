import Foundation

/// GENERATED. Do not edit by hand.
public final class DefaultEventsApi: EventsApiProtocol {
    private let bridge: any NativeBridgeProtocol
    private var subscriptions: [Int: DefaultEventSubscription] = [:]
    private var nextId = 1

    public init(bridge: any NativeBridgeProtocol) {
        self.bridge = bridge
    }

    public func subscribeEvents(_ request: [String: AnySendable]) async throws -> [String: AnySendable] {
        try await invokeMap(bridge, descriptor: NativeCallMap.eventSubscribe, request: requestWithDefaultHandler(request))
    }

    public func subscribeEventsBatch(_ request: [String: AnySendable]) async throws -> [String: AnySendable] {
        try await invokeMap(bridge, descriptor: NativeCallMap.eventSubscribeBatch, request: requestWithDefaultHandler(request))
    }

    public func unsubscribe(_ request: [String: AnySendable]) async throws {
        try await invokeVoid(bridge, descriptor: NativeCallMap.eventUnsubscribe, request: AnySendable(request))
    }

    public func unsubscribeAll() async throws {
        subscriptions.removeAll()
        try await invokeVoid(bridge, descriptor: NativeCallMap.eventUnsubscribeAll, request: nil)
    }

    public func emit(_ event: Any) {
        for subscription in subscriptions.values {
            subscription.dispatch(event)
        }
    }

    private func requestWithDefaultHandler(_ request: [String: AnySendable]) -> [String: AnySendable] {
        if request["handler"] != nil { return request }
        var next = request
        next["handler"] = AnySendable({ [weak self] eventType, payload in
            self?.emitNativeEvent(eventType: eventType, payload: payload)
        } as (Int, Any?) -> Void)
        return next
    }

    private func emitNativeEvent(eventType: Int, payload: Any?) {
        do {
            emit(try nativeEventFromCode(eventType: eventType, payload: payload))
        } catch {
            emit(LifecycleEvent(
                name: .initFailed,
                operation: "event.decode",
                error: sdkErrorPayload(from: error, operation: "event.decode")
            ))
        }
    }

    public func addEventListener(_ listener: any FlareImEventListener) -> any EventSubscription {
        register(handler: listener)
    }

    public func removeEventListener(_ subscription: any EventSubscription) {
        subscription.unsubscribe()
    }
    public func onInitializing(_ listener: @escaping EventCallback<LifecycleEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onInitialized(_ listener: @escaping EventCallback<LifecycleEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onInitFailed(_ listener: @escaping EventCallback<LifecycleEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onLoginSucceeded(_ listener: @escaping EventCallback<LifecycleEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onLoginFailed(_ listener: @escaping EventCallback<LifecycleEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onLoggedOut(_ listener: @escaping EventCallback<LifecycleEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onDisposed(_ listener: @escaping EventCallback<LifecycleEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onConnecting(_ listener: @escaping EventCallback<ConnectionEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onConnectSuccess(_ listener: @escaping EventCallback<ConnectionEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onConnectReady(_ listener: @escaping EventCallback<ConnectionEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onConnectFailed(_ listener: @escaping EventCallback<ConnectionEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onDisconnected(_ listener: @escaping EventCallback<ConnectionEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onReconnecting(_ listener: @escaping EventCallback<ConnectionEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onReconnectFailed(_ listener: @escaping EventCallback<ConnectionEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onKickedOffline(_ listener: @escaping EventCallback<ConnectionEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onUserTokenExpired(_ listener: @escaping EventCallback<ConnectionEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageReceived(_ listener: @escaping EventCallback<MessageReceivedEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageReceivedBatch(_ listener: @escaping EventCallback<MessageReceivedBatchEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageSendAck(_ listener: @escaping EventCallback<MessageSendAckEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageSendFailed(_ listener: @escaping EventCallback<MessageSendFailedEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageRecalled(_ listener: @escaping EventCallback<MessageMutationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageEdited(_ listener: @escaping EventCallback<MessageMutationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageDeleted(_ listener: @escaping EventCallback<MessageMutationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageReadReceipt(_ listener: @escaping EventCallback<ReadReceiptEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageReactionChanged(_ listener: @escaping EventCallback<ReactionChangedEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onInputStatusChanged(_ listener: @escaping EventCallback<TypingEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onTypingAggregateChanged(_ listener: @escaping EventCallback<TypingAggregateEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageBurned(_ listener: @escaping EventCallback<MessageMutationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessagePinned(_ listener: @escaping EventCallback<MessageMutationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessageUnpinned(_ listener: @escaping EventCallback<MessageMutationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onMessagePluginStateChanged(_ listener: @escaping EventCallback<MessageMutationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onViewUpdated(_ listener: @escaping EventCallback<ViewUpdate>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onNewConversation(_ listener: @escaping EventCallback<ConversationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onConversationChanged(_ listener: @escaping EventCallback<ConversationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onTotalUnreadMessageCountChanged(_ listener: @escaping EventCallback<ConversationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onConversationDeleted(_ listener: @escaping EventCallback<ConversationEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onSyncServerStart(_ listener: @escaping EventCallback<SyncEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onSyncServerFinish(_ listener: @escaping EventCallback<SyncEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onSyncServerFailed(_ listener: @escaping EventCallback<SyncEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onSyncProgress(_ listener: @escaping EventCallback<ProgressEvent>) -> any EventSubscription {
        register(handler: { (event: Any) in
            if let progress = event as? ProgressEvent, progress.name == .syncProgress {
                listener(progress)
            }
        })
    }
    public func onUploadProgress(_ listener: @escaping EventCallback<ProgressEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onDownloadProgress(_ listener: @escaping EventCallback<ProgressEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onPluginRealtime(_ listener: @escaping EventCallback<PluginRealtimeEvent>) -> any EventSubscription {
        registerTyped(listener)
    }
    public func onPluginCatalogChanged(_ listener: @escaping EventCallback<PluginCatalogChangedEvent>) -> any EventSubscription {
        registerTyped(listener)
    }

    private func register(handler: Any) -> any EventSubscription {
        let id = nextId
        nextId += 1
        let subscription = DefaultEventSubscription(id: String(id), handler: handler) { [weak self] in
            self?.subscriptions.removeValue(forKey: id)
        }
        subscriptions[id] = subscription
        return subscription
    }

    private func registerTyped<T>(_ listener: @escaping EventCallback<T>) -> any EventSubscription {
        register(handler: { (event: Any) in
            if let typed = event as? T { listener(typed) }
        })
    }
}

private func nativeEventFromCode(eventType: Int, payload: Any?) throws -> Any {
    let json = jsonObjectMap(payload)
    switch eventType {
    case EventCode.connectionConnected,
         EventCode.connectionDisconnected,
         EventCode.connectionReconnecting,
         EventCode.connectionStateChanged,
         EventCode.connectionSyncStateChanged,
         EventCode.connectionServerError,
         EventCode.connectionKickedOff,
         EventCode.connectionTokenExpired:
        return try connectionEventFromCode(eventType, json)
    case EventCode.messageSendAck:
        return MessageSendAckEvent(ack: try sendAckFromJson(jsonObjectMap(json["ack"]?.value)))
    case EventCode.messageSendFailed:
        return MessageSendFailedEvent(
            clientMsgId: try requiredString(json["clientMsgId"]?.value, "clientMsgId"),
            reason: try requiredString(json["reason"]?.value, "reason"),
            error: try sdkErrorPayloadFromJson(json["error"]?.value)
        )
    case EventCode.messageReceived:
        return MessageReceivedEvent(message: try messageFromJson(jsonObjectMap(json["message"]?.value)))
    case EventCode.messageReceivedBatch:
        let messages = try requiredListOfMaps(json["messages"]?.value, "messages").map { try messageFromJson($0) }
        return MessageReceivedBatchEvent(messages: messages)
    case EventCode.messageTyping:
        return TypingEvent(
            conversationId: try requiredString(json["conversationId"]?.value, "conversationId"),
            userId: try requiredString(json["userId"]?.value, "userId"),
            typing: try requiredBool(json["typing"]?.value, "typing")
        )
    case EventCode.messageTypingAggregate:
        return TypingAggregateEvent(
            conversationId: try requiredString(json["conversationId"]?.value, "conversationId"),
            typingUserIds: try requiredStringList(json["typingUserIds"]?.value, "typingUserIds", "TypingAggregateEvent"),
            typingCount: UInt32(try requiredUInt64(json["typingCount"]?.value, "typingCount"))
        )
    case EventCode.messageReadReceipt:
        return ReadReceiptEvent(
            conversationId: try requiredString(json["conversationId"]?.value, "conversationId"),
            userId: try requiredString(json["userId"]?.value, "userId"),
            readSeq: try requiredUInt64(json["readSeq"]?.value, "readSeq")
        )
    case EventCode.messageReactionChanged:
        return ReactionChangedEvent(
            conversationId: try requiredString(json["conversationId"]?.value, "conversationId"),
            serverMsgId: try requiredString(json["serverMsgId"]?.value, "serverMsgId"),
            userId: try requiredString(json["userId"]?.value, "userId"),
            emoji: try requiredString(json["emoji"]?.value, "emoji"),
            action: try requiredInt32(json["action"]?.value, "action")
        )
    case EventCode.messageRecalled:
        return try messageMutationFromPayload(.recalled, json)
    case EventCode.messageEdited:
        return try messageMutationFromPayload(.edited, json)
    case EventCode.messageDeleted:
        return try messageMutationFromPayload(.deleted, json)
    case EventCode.messagePinned:
        return try messageMutationFromPayload(.pinned, json)
    case EventCode.messageUnpinned:
        return try messageMutationFromPayload(.unpinned, json)
    case EventCode.messageMarked:
        return try messageMutationFromPayload(.marked, json)
    case EventCode.messageUnmarked:
        return try messageMutationFromPayload(.unmarked, json)
    case EventCode.messagePluginStateChanged:
        return try messageMutationFromPayload(.pluginStateChanged, json)
    case EventCode.messageRetentionScheduled:
        return try messageMutationFromPayload(.retentionScheduled, json)
    case EventCode.messageRetentionExpired:
        return try messageMutationFromPayload(.retentionExpired, json)
    case EventCode.messageRetentionPurged:
        return try messageMutationFromPayload(.retentionPurged, json)
    case EventCode.conversationSynced,
         EventCode.conversationCreated,
         EventCode.conversationUpdated,
         EventCode.conversationUnreadCountChanged,
         EventCode.conversationDeleted:
        return try conversationEventFromCode(eventType, json)
    case EventCode.syncStarted,
         EventCode.syncFinished,
         EventCode.syncFailed,
         EventCode.syncTaskCompleted,
         EventCode.syncStateChanged,
         EventCode.syncResyncNeeded:
        return try syncEventFromCode(eventType, json)
    case EventCode.syncProgress:
        let task = try optionalString(json["task"]?.value, "task")
        return ProgressEvent(
            name: .syncProgress,
            operation: task ?? "sync",
            current: try requiredUInt64(json["progress"]?.value, "progress"),
            total: 100,
            taskId: task,
            detail: try optionalString(json["detail"]?.value, "detail")
        )
    case EventCode.pluginRealtime:
        return PluginRealtimeEvent(
            pluginId: try requiredString(json["pluginId"]?.value, "pluginId"),
            packetType: try requiredString(json["packetType"]?.value, "packetType"),
            conversationId: try requiredString(json["conversationId"]?.value, "conversationId"),
            payload: try requiredJsonObject(json["payload"]?.value, "payload"),
            deliveryId: try requiredString(json["deliveryId"]?.value, "deliveryId"),
            expiresAtMs: try requiredUInt64(json["expiresAtMs"]?.value, "expiresAtMs")
        )
    case EventCode.pluginCatalogChanged:
        return PluginCatalogChangedEvent(version: try requiredString(json["version"]?.value, "version"))
    case EventCode.viewUpdated:
        return try viewUpdateFromJson(json)
    default:
        return [
            "type": "unknown",
            "name": "unknown",
            "event": "unknown",
            "eventType": eventType,
            "payload": json,
        ]
    }
}

private func sdkErrorPayloadFromJson(_ value: Any?) throws -> SdkErrorPayload? {
    guard let value, !(value is NSNull) else {
        return nil
    }
    let json: [String: AnySendable]
    if let map = value as? [String: AnySendable] {
        json = map
    } else if let map = value as? [String: Any] {
        json = map.mapValues { AnySendable($0) }
    } else {
        try invalidEventField("error", "object")
    }
    return SdkErrorPayload(
        code: try requiredString(json["code"]?.value, "error.code"),
        machineReason: try machineReason(json["machineReason"]?.value),
        message: try requiredString(json["message"]?.value, "error.message"),
        operation: try optionalString(json["operation"]?.value, "error.operation"),
        retryable: try optionalBool(json["retryable"]?.value, "error.retryable"),
        details: try stringMap(json["details"]?.value, "error.details")
    )
}

/// The server's typed reason; empty for a local failure, which reads as no reason.
private func machineReason(_ value: Any?) throws -> String? {
    guard let value, !(value is NSNull) else { return nil }
    guard let text = value as? String else { try invalidEventField("error.machineReason", "string") }
    return text.isEmpty ? nil : text
}

private func messageMutationFromPayload(_ name: MessageEventName, _ json: [String: AnySendable]) throws -> MessageMutationEvent {
    MessageMutationEvent(
        name: name,
        conversationId: try requiredString(json["conversationId"]?.value, "conversationId"),
        messageId: try optionalString(json["messageId"]?.value, "messageId"),
        serverMsgId: try optionalString(json["serverMsgId"]?.value, "serverMsgId"),
        userId: try optionalString(json["userId"]?.value, "userId"),
        reason: try optionalString(json["reason"]?.value, "reason")
    )
}

private func connectionEventFromCode(_ eventType: Int, _ json: [String: AnySendable]) throws -> ConnectionEvent {
    let name = try connectionEventNameFromCode(eventType, json)
    return ConnectionEvent(
        name: name,
        state: try connectionStateForEvent(name, json["state"]?.value),
        reason: try optionalString(json["reason"]?.value, "reason"),
        attempt: try optionalUInt32(json["attempt"]?.value, "attempt"),
        error: try sdkErrorPayloadFromJson(json["error"]?.value)
    )
}

private func connectionEventNameFromCode(_ eventType: Int, _ json: [String: AnySendable]) throws -> ConnectionEventName {
    if eventType == EventCode.connectionStateChanged {
        switch try connectionStateFromWire(json["state"]?.value) {
        case .connecting: return .connecting
        case .connected: return .connected
        case .ready: return .ready
        case .reconnecting: return .reconnecting
        case .disconnected: return .disconnected
        }
    }
    switch eventType {
    case EventCode.connectionConnected: return .connected
    case EventCode.connectionDisconnected: return .disconnected
    case EventCode.connectionReconnecting: return .reconnecting
    case EventCode.connectionSyncStateChanged: return .syncStateChanged
    case EventCode.connectionServerError: return .serverError
    case EventCode.connectionKickedOff: return .kickedOff
    case EventCode.connectionTokenExpired: return .tokenExpired
    default:
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "invalid connection event type: \(eventType)",
            operation: "event.decode",
            details: ["field": "eventType"]
        )
    }
}

private func connectionStateForEvent(_ name: ConnectionEventName, _ value: Any?) throws -> SdkConnectionState {
    if value != nil {
        return try connectionStateFromWire(value)
    }
    switch name {
    case .connecting: return .connecting
    case .connected: return .connected
    case .ready: return .ready
    case .reconnecting: return .reconnecting
    case .syncStateChanged: return .ready
    case .serverError: return .connected
    case .disconnected, .reconnectFailed, .kickedOff, .tokenExpired:
        return .disconnected
    case .stateChanged:
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "missing connection state for state_changed event",
            operation: "event.decode",
            details: ["field": "state"]
        )
    }
}

private func connectionStateFromWire(_ value: Any?) throws -> SdkConnectionState {
    let raw = try requiredString(value, "state").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    switch raw {
    case "connecting": return .connecting
    case "connected": return .connected
    case "ready": return .ready
    case "reconnecting": return .reconnecting
    case "disconnected": return .disconnected
    default:
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "invalid connection state: \(raw.isEmpty ? "<empty>" : raw)",
            operation: "event.decode",
            details: ["field": "state"]
        )
    }
}

private func conversationEventFromCode(_ eventType: Int, _ json: [String: AnySendable]) throws -> ConversationEvent {
    let name: ConversationEventName
    switch eventType {
    case EventCode.conversationSynced: name = .synced
    case EventCode.conversationCreated: name = .created
    case EventCode.conversationUpdated: name = .updated
    case EventCode.conversationUnreadCountChanged: name = .unreadCountChanged
    case EventCode.conversationDeleted: name = .deleted
    default:
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "invalid conversation event type: \(eventType)",
            operation: "event.decode",
            details: ["field": "eventType"]
        )
    }
    let conversationId = try optionalString(json["conversationId"]?.value, "conversationId")
    return ConversationEvent(
        name: name,
        conversationId: conversationId,
        conversationIds: try optionalStringList(json["conversationIds"]?.value, "conversationIds"),
        unreadCount: try optionalUInt32(json["unreadCount"]?.value, "unreadCount")
    )
}

private func syncEventFromCode(_ eventType: Int, _ json: [String: AnySendable]) throws -> SyncEvent {
    let name: SyncEventName
    switch eventType {
    case EventCode.syncStateChanged: name = .stateChanged
    case EventCode.syncStarted: name = .started
    case EventCode.syncFinished: name = .finished
    case EventCode.syncFailed: name = .failed
    case EventCode.syncTaskCompleted: name = .taskCompleted
    case EventCode.syncResyncNeeded: name = .resyncNeeded
    default:
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "invalid sync event type: \(eventType)",
            operation: "event.decode",
            details: ["field": "eventType"]
        )
    }
    return SyncEvent(
        name: name,
        trigger: try optionalString(json["trigger"]?.value, "trigger"),
        phase: try optionalString(json["phase"]?.value, "phase"),
        task: try optionalString(json["task"]?.value, "task"),
        progress: try optionalUInt32(json["progress"]?.value, "progress"),
        error: try sdkErrorPayloadFromJson(json["error"]?.value)
    )
}

private func invalidEventField(_ field: String, _ expected: String) throws -> Never {
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "invalid event payload field: \(field)",
        operation: "event.decode",
        details: ["field": field, "expected": expected]
    )
}

private func requiredString(_ value: Any?, _ field: String) throws -> String {
    if let text = value as? String, !text.isEmpty { return text }
    try invalidEventField(field, "non-empty string")
}

private func optionalString(_ value: Any?, _ field: String) throws -> String? {
    guard let value, !(value is NSNull) else { return nil }
    if let text = value as? String, !text.isEmpty { return text }
    try invalidEventField(field, "non-empty string")
}

private func requiredUInt64(_ value: Any?, _ field: String) throws -> UInt64 {
    if let value = value as? UInt64 { return value }
    if let value = value as? UInt32 { return UInt64(value) }
    if let value = value as? UInt { return UInt64(value) }
    if let value = value as? Int, value >= 0 { return UInt64(value) }
    if let value = value as? Int64, value >= 0 { return UInt64(value) }
    if let value = value as? Int32, value >= 0 { return UInt64(value) }
    if let value = value as? Double, value.isFinite, value >= 0, value.rounded(.towardZero) == value {
        return UInt64(value)
    }
    if let value = value as? NSNumber, !(value is Bool) {
        let number = value.doubleValue
        if number.isFinite, number >= 0, number.rounded(.towardZero) == number {
            return UInt64(number)
        }
    }
    try invalidEventField(field, "unsigned integer")
}

private func optionalUInt32(_ value: Any?, _ field: String) throws -> UInt32? {
    guard let value, !(value is NSNull) else { return nil }
    let number = try requiredUInt64(value, field)
    guard number <= UInt64(UInt32.max) else {
        try invalidEventField(field, "uint32")
    }
    return UInt32(number)
}

private func requiredInt32(_ value: Any?, _ field: String) throws -> Int32 {
    let number = try requiredUInt64(value, field)
    guard number <= UInt64(Int32.max) else {
        try invalidEventField(field, "int32")
    }
    return Int32(number)
}

private func requiredBool(_ value: Any?, _ field: String) throws -> Bool {
    if let value = value as? Bool { return value }
    try invalidEventField(field, "boolean")
}

private func optionalBool(_ value: Any?, _ field: String) throws -> Bool? {
    guard let value, !(value is NSNull) else { return nil }
    return try requiredBool(value, field)
}

private func requiredJsonObject(_ value: Any?, _ field: String) throws -> [String: AnySendable] {
    if let map = value as? [String: AnySendable] { return map }
    if let map = value as? [String: Any] { return map.mapValues { AnySendable($0) } }
    try invalidEventField(field, "object")
}

private func requiredListOfMaps(_ value: Any?, _ field: String) throws -> [[String: Any]] {
    guard let items = value as? [Any] else {
        try invalidEventField(field, "array")
    }
    return try items.enumerated().map { index, item in
        guard let map = item as? [String: Any] else {
            try invalidEventField("\(field).\(index)", "object")
        }
        return map
    }
}

private func optionalStringList(_ value: Any?, _ field: String) throws -> [String] {
    guard let value, !(value is NSNull) else { return [] }
    guard let items = value as? [Any] else {
        try invalidEventField(field, "array")
    }
    return try items.enumerated().map { index, item in
        try requiredString(item, "\(field).\(index)")
    }
}

private func stringMap(_ value: Any?, _ field: String) throws -> [String: String] {
    guard let value, !(value is NSNull) else { return [:] }
    let raw: [String: Any]
    if let map = value as? [String: Any] {
        raw = map
    } else if let map = value as? [String: AnySendable] {
        raw = map.mapValues { $0.value }
    } else {
        try invalidEventField(field, "object")
    }
    var out: [String: String] = [:]
    for (key, item) in raw {
        guard let text = item as? String else {
            try invalidEventField("\(field).\(key)", "string")
        }
        out[key] = text
    }
    return out
}

private final class DefaultEventSubscription: EventSubscription, @unchecked Sendable {
    let id: String
    private let onDispose: @Sendable () -> Void
    let handler: Any

    init(id: String, handler: Any, onDispose: @escaping @Sendable () -> Void) {
        self.id = id
        self.handler = handler
        self.onDispose = onDispose
    }

    public func unsubscribe() { onDispose() }

    func dispatch(_ event: Any) {
        if let listener = handler as? any FlareImEventListener {
            dispatchToListener(listener, event: event)
            return
        }
        if let callback = handler as? (Any) -> Void {
            callback(event)
        }
    }
}

private func dispatchToListener(_ listener: any FlareImEventListener, event: Any) {
    if let lifecycle = event as? LifecycleEvent {
        switch lifecycle.name {
        case .initializing: listener.onInitializing(lifecycle)
        case .initialized: listener.onInitialized(lifecycle)
        case .initFailed: listener.onInitFailed(lifecycle)
        case .loginSucceeded: listener.onLoginSucceeded(lifecycle)
        case .loginFailed: listener.onLoginFailed(lifecycle)
        case .loggedOut: listener.onLoggedOut(lifecycle)
        case .disposed: listener.onDisposed(lifecycle)
        }
        return
    }
    if let connection = event as? ConnectionEvent {
        switch connection.name {
        case .connecting: listener.onConnecting(connection)
        case .connected: listener.onConnectSuccess(connection)
        case .ready: listener.onConnectReady(connection)
        case .serverError: listener.onConnectFailed(connection)
        case .disconnected: listener.onDisconnected(connection)
        case .reconnecting: listener.onReconnecting(connection)
        case .reconnectFailed: listener.onReconnectFailed(connection)
        case .kickedOff: listener.onKickedOffline(connection)
        case .tokenExpired: listener.onUserTokenExpired(connection)
        case .stateChanged, .syncStateChanged: break
        }
        return
    }
    if let received = event as? MessageReceivedEvent { listener.onMessageReceived(received); return }
    if let batch = event as? MessageReceivedBatchEvent { listener.onMessageReceivedBatch(batch); return }
    if let ack = event as? MessageSendAckEvent { listener.onMessageSendAck(ack); return }
    if let failed = event as? MessageSendFailedEvent { listener.onMessageSendFailed(failed); return }
    if let mutation = event as? MessageMutationEvent {
        switch mutation.name {
        case .recalled: listener.onMessageRecalled(mutation)
        case .edited: listener.onMessageEdited(mutation)
        case .deleted: listener.onMessageDeleted(mutation)
        case .burned, .retentionExpired, .retentionPurged: listener.onMessageBurned(mutation)
        case .pinned: listener.onMessagePinned(mutation)
        case .unpinned: listener.onMessageUnpinned(mutation)
        case .pluginStateChanged: listener.onMessagePluginStateChanged(mutation)
        case .received, .receivedBatch, .sendAck, .sendFailed,
             .typing, .typingAggregate, .reactionChanged, .readReceipt, .burnScheduled,
             .hardDeleted, .marked, .unmarked, .retentionScheduled,
             .presenceChanged, .custom, .readSummariesChanged:
            break
        }
        return
    }
    if let read = event as? ReadReceiptEvent { listener.onMessageReadReceipt(read); return }
    if let reaction = event as? ReactionChangedEvent { listener.onMessageReactionChanged(reaction); return }
    if let typing = event as? TypingEvent { listener.onInputStatusChanged(typing); return }
    if let aggregate = event as? TypingAggregateEvent { listener.onTypingAggregateChanged(aggregate); return }
    if let view = event as? ViewUpdate { listener.onViewUpdated(view); return }
    if let conversation = event as? ConversationEvent {
        switch conversation.name {
        case .created: listener.onNewConversation(conversation)
        case .updated, .synced: listener.onConversationChanged(conversation)
        case .unreadCountChanged: listener.onTotalUnreadMessageCountChanged(conversation)
        case .deleted: listener.onConversationDeleted(conversation)
        }
        return
    }
    if let sync = event as? SyncEvent {
        switch sync.name {
        case .started: listener.onSyncServerStart(sync)
        case .finished: listener.onSyncServerFinish(sync)
        case .failed: listener.onSyncServerFailed(sync)
        case .progress:
            listener.onSyncProgress(
                ProgressEvent(
                    name: .syncProgress,
                    operation: sync.task ?? sync.phase ?? "sync",
                    current: UInt64(sync.progress ?? 0),
                    total: 100
                )
            )
        case .stateChanged, .taskCompleted, .resyncNeeded, .readiness:
            break
        }
        return
    }
    if let progress = event as? ProgressEvent {
        listener.onSyncProgress(progress)
        return
    }
    if let realtime = event as? PluginRealtimeEvent {
        listener.onPluginRealtime(realtime)
        return
    }
    if let catalog = event as? PluginCatalogChangedEvent {
        listener.onPluginCatalogChanged(catalog)
        return
    }
}
