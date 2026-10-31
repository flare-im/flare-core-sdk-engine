import Foundation

func pluginUiEvaluationRequestToMap(_ request: PluginUiEvaluationRequest) -> [String: AnySendable] {
    ["payload": wrapSendable(request.payload)]
}

func pluginCommandRequestToMap(_ request: PluginCommandRequest) -> [String: AnySendable] {
    ["payload": wrapSendable(request.payload)]
}

func messageReadReceiptMembersRequestToMap(_ request: MessageReadReceiptMembersRequest) -> [String: AnySendable] {
    ["payload": wrapSendable(request.payload)]
}

/// GENERATED. Do not edit by hand.

func unwrapRequest(_ value: AnySendable) -> [String: AnySendable]? {
    value.value as? [String: AnySendable]
}

func wrapSendable(_ value: Any?) -> AnySendable {
    AnySendable(value as Any)
}

func invokeVoid(_ bridge: any NativeBridgeProtocol, descriptor: NativeCallDescriptor, request: AnySendable?) async throws {
    _ = try await bridge.invoke(descriptor, request: request)
}

func invokeBool(_ bridge: any NativeBridgeProtocol, descriptor: NativeCallDescriptor, request: AnySendable?) async throws -> Bool {
    let value = try await bridge.invoke(descriptor, request: request)
    guard let bool = value.value as? Bool else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "native response must be a boolean",
            operation: descriptor.operation,
            details: ["expected": "boolean"]
        )
    }
    return bool
}

func invokeConnectionState(_ bridge: any NativeBridgeProtocol, descriptor: NativeCallDescriptor, request: AnySendable?) async throws -> ConnectionState {
    let value = try await bridge.invoke(descriptor, request: request)
    if let state = value.value as? ConnectionState { return state }
    if let raw = value.value as? String, let state = ConnectionState(rawValue: raw) { return state }
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "native response must be a canonical connection state",
        operation: descriptor.operation,
        details: ["expected": "ConnectionState"]
    )
}

func invokeMap(_ bridge: any NativeBridgeProtocol, descriptor: NativeCallDescriptor, request: [String: AnySendable]?) async throws -> [String: AnySendable] {
    let payload = request.map { dict in AnySendable(dict.mapValues { $0.value }) }
    let value = try await bridge.invoke(descriptor, request: payload)
    if let map = value.value as? [String: AnySendable] {
        return map
    }
    if let map = value.value as? [String: Any] {
        return map.mapValues { AnySendable($0) }
    }
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "native response must be an object",
        operation: descriptor.operation,
        details: ["expected": "object"]
    )
}

func sdkErrorPayload(from error: Error, operation: String) -> SdkErrorPayload {
    let message = (error as? LocalizedError)?.errorDescription ?? String(describing: error)
    return SdkErrorPayload(
        code: "internal",
        message: message,
        operation: operation,
        retryable: false,
        details: ["type": String(describing: type(of: error))]
    )
}

func userIdFromRequest(_ request: [String: AnySendable]) -> String? {
    request["userId"]?.value as? String
}

private let HEARTBEAT_APP_STATE_WIRE_ORDER: [HeartbeatAppState] = [.foreground, .background]
private let MESSAGE_SEARCH_KIND_WIRE_ORDER: [MessageSearchKind] = [.message, .text, .media, .image, .video, .audio, .file]
private let SDK_EVENT_KIND_WIRE_ORDER: [SdkEventKind] = [.lifecycle, .connection, .message, .notification, .conversation, .sync, .`extension`, .extensionEvent, .presence, .media, .plugin, .view]
private let LIFECYCLE_EVENT_NAME_WIRE_ORDER: [LifecycleEventName] = [.initializing, .initialized, .initFailed, .loginSucceeded, .loginFailed, .loggedOut, .disposed]
private let SDK_CONNECTION_STATE_WIRE_ORDER: [SdkConnectionState] = [.disconnected, .connecting, .connected, .ready, .reconnecting]
private let CONNECTION_EVENT_NAME_WIRE_ORDER: [ConnectionEventName] = [.connecting, .connected, .ready, .disconnected, .reconnecting, .reconnectFailed, .stateChanged, .syncStateChanged, .serverError, .kickedOff, .tokenExpired]
private let MESSAGE_EVENT_NAME_WIRE_ORDER: [MessageEventName] = [.received, .receivedBatch, .sendAck, .sendFailed, .recalled, .typing, .typingAggregate, .edited, .reactionChanged, .deleted, .readReceipt, .burnScheduled, .burned, .hardDeleted, .pinned, .unpinned, .marked, .unmarked, .retentionScheduled, .retentionExpired, .retentionPurged, .presenceChanged, .custom, .pluginStateChanged, .readSummariesChanged]
private let CONVERSATION_EVENT_NAME_WIRE_ORDER: [ConversationEventName] = [.synced, .created, .updated, .unreadCountChanged, .deleted]
private let SYNC_EVENT_NAME_WIRE_ORDER: [SyncEventName] = [.stateChanged, .started, .finished, .failed, .progress, .taskCompleted, .resyncNeeded]
private let PROGRESS_EVENT_NAME_WIRE_ORDER: [ProgressEventName] = [.syncProgress, .uploadProgress, .downloadProgress]

func setHeartbeatAppStateRequestToMap(_ request: SetHeartbeatAppStateRequest) -> [String: AnySendable] {
    [
        "appState": wrapSendable(request.appState),
    ]
}
func setHeartbeatNatTimeoutRequestToMap(_ request: SetHeartbeatNatTimeoutRequest) -> [String: AnySendable] {
    [
        "natTimeoutSecs": wrapSendable(request.natTimeoutSecs),
    ]
}
func networkChangeRequestToMap(_ request: NetworkChangeRequest) -> [String: AnySendable] {
    [
        "available": wrapSendable(request.available),
        "interface": wrapSendable(request.interface?.rawValue),
        "expensive": wrapSendable(request.expensive),
        "metered": wrapSendable(request.metered),
        "reason": wrapSendable(request.reason),
    ]
}
func networkChangeResponseToMap(_ request: NetworkChangeResponse) -> [String: AnySendable] {
    [
        "reconnected": wrapSendable(request.reconnected),
    ]
}
func updateConversationDraftRequestToMap(_ request: UpdateConversationDraftRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "draft": wrapSendable(request.draft),
    ]
}
func heartbeatEffectiveIntervalResponseToMap(_ request: HeartbeatEffectiveIntervalResponse) -> [String: AnySendable] {
    [
        "connected": wrapSendable(request.connected),
        "intervalMs": wrapSendable(request.intervalMs),
        "intervalSecs": wrapSendable(request.intervalSecs),
    ]
}
func runtimeHealthResponseToMap(_ request: RuntimeHealthResponse) -> [String: AnySendable] {
    [
        "metricsEnabled": wrapSendable(request.metricsEnabled),
        "state": wrapSendable(request.state),
        "stateCode": wrapSendable(request.stateCode),
        "sessionGeneration": wrapSendable(request.sessionGeneration),
        "rawSubscriberDroppedTotal": wrapSendable(request.rawSubscriberDroppedTotal),
        "metricsJson": wrapSendable(request.metricsJson),
    ]
}
func conversationParticipantToMap(_ request: ConversationParticipant) -> [String: AnySendable] {
    [
        "userId": wrapSendable(request.userId),
        "roles": wrapSendable(request.roles),
        "muted": wrapSendable(request.muted),
        "pinned": wrapSendable(request.pinned),
        "attributes": wrapSendable(request.attributes),
        "joinedAt": wrapSendable(request.joinedAt),
        "nickname": wrapSendable(request.nickname),
        "name": wrapSendable(request.name.map { displayNameToMap($0).mapValues { $0.value } }),
    ]
}
func displayNameToMap(_ request: DisplayName) -> [String: AnySendable] {
    var out: [String: AnySendable] = ["kind": AnySendable(request.kind.rawValue)]
    if let text = request.text { out["text"] = AnySendable(text) }
    if let source = request.source { out["source"] = AnySendable(source.rawValue) }
    if let names = request.names { out["names"] = AnySendable(names) }
    if let total = request.total { out["total"] = AnySendable(total) }
    if let entity = request.entity { out["entity"] = AnySendable(entity.rawValue) }
    if let idTail = request.idTail { out["idTail"] = AnySendable(idTail) }
    return out
}
func messagePreviewToMap(_ request: MessagePreview) -> [String: AnySendable] {
    [
        "messageId": wrapSendable(request.messageId),
        "senderId": wrapSendable(request.senderId),
        "type": wrapSendable(request.type),
        "text": wrapSendable(request.text),
        "time": wrapSendable(request.time),
    ]
}
func conversationToMap(_ request: Conversation) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "conversationType": wrapSendable(request.conversationType),
        "businessType": wrapSendable(request.businessType),
        "channelId": wrapSendable(request.channelId),
        "membersCount": wrapSendable(request.membersCount),
        "displayName": wrapSendable(request.displayName),
        "title": wrapSendable(request.title.map { displayNameToMap($0).mapValues { $0.value } }),
        "avatarUrl": wrapSendable(request.avatarUrl),
        "remark": wrapSendable(request.remark),
        "description": wrapSendable(request.description),
        "lastMessageId": wrapSendable(request.lastMessageId),
        "lastSenderId": wrapSendable(request.lastSenderId),
        "lastMessageAt": wrapSendable(request.lastMessageAt),
        "lastMessagePreview": wrapSendable(request.lastMessagePreview),
        "lastMessage": wrapSendable(request.lastMessage),
        "lastSenderNickname": wrapSendable(request.lastSenderNickname),
        "lastSender": wrapSendable(request.lastSender.map { displayNameToMap($0).mapValues { $0.value } }),
        "lastSenderAvatarUrl": wrapSendable(request.lastSenderAvatarUrl),
        "unreadCount": wrapSendable(request.unreadCount),
        "lastReadSeq": wrapSendable(request.lastReadSeq),
        "peerReadSeq": wrapSendable(request.peerReadSeq),
        "maxSeq": wrapSendable(request.maxSeq),
        "visibleAfterSeq": wrapSendable(request.visibleAfterSeq),
        "isPinned": wrapSendable(request.isPinned),
        "isMuted": wrapSendable(request.isMuted),
        "isArchived": wrapSendable(request.isArchived),
        "version": wrapSendable(request.version),
        "updatedAt": wrapSendable(request.updatedAt),
        "createdAt": wrapSendable(request.createdAt),
        "updatedAtTs": wrapSendable(request.updatedAtTs),
        "ext": wrapSendable(request.ext),
        "participantVersion": wrapSendable(request.participantVersion),
        "memberPreview": AnySendable(request.memberPreview.map { conversationParticipantToMap($0).mapValues { $0.value } }),
        "draft": wrapSendable(request.draft),
        "mentionCount": wrapSendable(request.mentionCount),
        "mentionMe": wrapSendable(request.mentionMe),
        "badge": wrapSendable(request.badge),
        "role": wrapSendable(request.role),
        "participants": AnySendable(request.participants.map { conversationParticipantToMap($0).mapValues { $0.value } }),
    ]
}
func conversationListQueryToMap(_ request: ConversationListQuery) -> [String: AnySendable] {
    [
        "keyword": wrapSendable(request.keyword),
        "includeArchived": wrapSendable(request.includeArchived),
        "unreadOnly": wrapSendable(request.unreadOnly),
        "mentionMeOnly": wrapSendable(request.mentionMeOnly),
        "pinnedOnly": wrapSendable(request.pinnedOnly),
        "mutedOnly": wrapSendable(request.mutedOnly),
        "hasDraftOnly": wrapSendable(request.hasDraftOnly),
        "hasMarkedMessages": wrapSendable(request.hasMarkedMessages),
        "conversationTypes": wrapSendable(request.conversationTypes),
        "cursor": wrapSendable(request.cursor),
        "limit": wrapSendable(request.limit),
    ]
}
func listConversationsResponseToMap(_ request: ListConversationsResponse) -> [String: AnySendable] {
    [
        "conversations": AnySendable(request.conversations.map { conversationToMap($0).mapValues { $0.value } }),
    ]
}
func bootstrapHomeTimelineRequestToMap(_ request: BootstrapHomeTimelineRequest) -> [String: AnySendable] {
    [
        "conversationLimit": wrapSendable(request.conversationLimit),
    ]
}
func openConversationTimelineRequestToMap(_ request: OpenConversationTimelineRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "messageLimit": wrapSendable(request.messageLimit),
    ]
}
func homeTimelineSnapshotToMap(_ request: HomeTimelineSnapshot) -> [String: AnySendable] {
    [
        "conversations": AnySendable(request.conversations.map { conversationToMap($0).mapValues { $0.value } }),
        "totalUnread": wrapSendable(request.totalUnread),
        "syncState": wrapSendable(request.syncState),
    ]
}
func conversationTimelineSnapshotToMap(_ request: ConversationTimelineSnapshot) -> [String: AnySendable] {
    [
        "conversation": wrapSendable(request.conversation),
        "messages": AnySendable(request.messages.map { messageToWireMap($0).mapValues { $0.value } }),
        "hasMore": wrapSendable(request.hasMore),
    ]
}
func openTimelineViewRequestToMap(_ request: OpenTimelineViewRequest) -> [String: AnySendable] {
    var out: [String: AnySendable] = [
        "conversationId": wrapSendable(request.conversationId),
        "messageLimit": wrapSendable(request.messageLimit),
    ]
    if let anchor = request.anchor {
        var map: [String: AnySendable] = [:]
        if let messageId = anchor.messageId {
            map["messageId"] = wrapSendable(messageId)
        }
        if let seq = anchor.seq {
            map["seq"] = wrapSendable(seq)
        }
        out["anchor"] = AnySendable(map.mapValues { $0.value })
    }
    return out
}
func loadOlderTimelineViewRequestToMap(_ request: LoadOlderTimelineViewRequest) -> [String: AnySendable] {
    [
        "viewId": wrapSendable(request.viewId),
        "messageLimit": wrapSendable(request.messageLimit),
    ]
}
func loadNewerTimelineViewRequestToMap(_ request: LoadNewerTimelineViewRequest) -> [String: AnySendable] {
    [
        "viewId": wrapSendable(request.viewId),
        "messageLimit": wrapSendable(request.messageLimit),
    ]
}
func timelineViewRequestToMap(_ request: TimelineViewRequest) -> [String: AnySendable] {
    [
        "viewId": wrapSendable(request.viewId),
    ]
}
func openConversationListViewRequestToMap(_ request: OpenConversationListViewRequest) -> [String: AnySendable] {
    [
        "conversationLimit": wrapSendable(request.conversationLimit),
    ]
}
func closeViewRequestToMap(_ request: CloseViewRequest) -> [String: AnySendable] {
    [
        "viewId": wrapSendable(request.viewId),
    ]
}
func viewSnapshotToMap(_ request: ViewSnapshot) -> [String: AnySendable] {
    [
        "viewType": wrapSendable(request.viewType),
        "data": wrapSendable(plainJsonObject(request.data)),
    ]
}
func viewOpenResponseToMap(_ request: ViewOpenResponse) -> [String: AnySendable] {
    [
        "viewId": wrapSendable(request.viewId),
        "revision": wrapSendable(request.revision),
        "snapshot": AnySendable(viewSnapshotToMap(request.snapshot).mapValues { $0.value }),
    ]
}
func viewDeltaOpToMap(_ request: ViewDeltaOp) -> [String: AnySendable] {
    var out: [String: AnySendable] = [
        "op": wrapSendable(request.op),
        "key": wrapSendable(request.key),
        "index": wrapSendable(request.index),
    ]
    if let fromIndex = request.fromIndex {
        out["fromIndex"] = wrapSendable(fromIndex)
    }
    if let item = request.item {
        out["item"] = wrapSendable(plainJsonObject(item))
    }
    return out
}
func viewDeltaToMap(_ request: ViewDelta) -> [String: AnySendable] {
    var out: [String: AnySendable] = [
        "viewType": wrapSendable(request.viewType),
    ]
    if let ops = request.ops {
        out["ops"] = AnySendable(ops.map { viewDeltaOpToMap($0).mapValues { $0.value } })
    }
    for (name, rows) in [("prepend", request.prepend), ("append", request.append), ("upsert", request.upsert)] {
        if let rows {
            out[name] = AnySendable(rows.map { messageToWireMap($0).mapValues { $0.value } })
        }
    }
    if let remove = request.remove {
        out["remove"] = wrapSendable(remove)
    }
    if let reorder = request.reorder {
        out["reorder"] = wrapSendable(reorder)
    }
    if let hasOlder = request.hasOlder {
        out["hasOlder"] = wrapSendable(hasOlder)
    }
    if let hasNewer = request.hasNewer {
        out["hasNewer"] = wrapSendable(hasNewer)
    }
    if let attachedToLatest = request.attachedToLatest {
        out["attachedToLatest"] = wrapSendable(attachedToLatest)
    }
    if let conversation = request.conversation {
        out["conversation"] = AnySendable(conversationToMap(conversation).mapValues { $0.value })
    }
    if let totalUnread = request.totalUnread {
        out["totalUnread"] = wrapSendable(totalUnread)
    }
    if let syncState = request.syncState {
        out["syncState"] = wrapSendable(syncState)
    }
    return out
}
func viewUpdateToMap(_ request: ViewUpdate) -> [String: AnySendable] {
    var out: [String: AnySendable] = [
        "viewId": wrapSendable(request.viewId),
        "revision": wrapSendable(request.revision),
        "kind": wrapSendable(request.kind),
    ]
    if let snapshot = request.snapshot {
        out["snapshot"] = AnySendable(viewSnapshotToMap(snapshot).mapValues { $0.value })
    }
    if let delta = request.delta {
        out["delta"] = AnySendable(viewDeltaToMap(delta).mapValues { $0.value })
    }
    return out
}
func closeViewResponseToMap(_ request: CloseViewResponse) -> [String: AnySendable] {
    [
        "closed": wrapSendable(request.closed),
    ]
}
func conversationVersionToMap(_ request: ConversationVersion) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "version": wrapSendable(request.version),
    ]
}
func syncConversationSummariesRequestToMap(_ request: SyncConversationSummariesRequest) -> [String: AnySendable] {
    [
        "knownVersions": AnySendable(request.knownVersions.map { conversationVersionToMap($0).mapValues { $0.value } }),
    ]
}
func syncConversationSummariesResponseToMap(_ request: SyncConversationSummariesResponse) -> [String: AnySendable] {
    [
        "changedConversations": AnySendable(request.changedConversations.map { conversationVersionToMap($0).mapValues { $0.value } }),
    ]
}
func startupHomeSyncRequestToMap(_ request: StartupHomeSyncRequest) -> [String: AnySendable] {
    [
        "backfillVisibleHistories": AnySendable(request.backfillVisibleHistories),
        "conversationLimit": AnySendable(request.conversationLimit),
        "historyBackfillLimit": AnySendable(request.historyBackfillLimit),
        "historyBackfillMaxConversations": AnySendable(request.historyBackfillMaxConversations),
        "historyBackfillMaxPagesPerConversation": AnySendable(request.historyBackfillMaxPagesPerConversation),
        "startBackgroundConvergence": AnySendable(request.startBackgroundConvergence),
    ]
}

func startupHomeSyncResponseFromJson(_ value: [String: AnySendable]) throws -> StartupHomeSyncResponse {
    let json = plainMap(value)
    return StartupHomeSyncResponse(
        backgroundConvergenceStarted: (json["backgroundConvergenceStarted"] as? Bool) ?? false,
        coldSyncPerformed: (json["coldSyncPerformed"] as? Bool) ?? false,
        degradedReason: json["degradedReason"] as? String,
        servedFromLocal: (json["servedFromLocal"] as? Bool) ?? false,
        snapshot: try homeTimelineSnapshotFromJson(jsonObjectMap(json["snapshot"]))
    )
}

func conversationHistoryBackfillRequestToMap(_ request: ConversationHistoryBackfillRequest) -> [String: AnySendable] {
    var out: [String: AnySendable] = ["conversationId": AnySendable(request.conversationId)]
    if let limit = request.limit { out["limit"] = AnySendable(limit) }
    if let maxPages = request.maxPages { out["maxPages"] = AnySendable(maxPages) }
    return out
}

func conversationHistoryBackfillResponseFromJson(_ value: [String: AnySendable]) throws -> ConversationHistoryBackfillResponse {
    let json = plainMap(value)
    return ConversationHistoryBackfillResponse(
        conversationId: try stringValue(json["conversationId"]),
        pagesLoaded: UInt32(try intValue(json["pagesLoaded"])),
        oldestSeqBefore: try intValue(json["oldestSeqBefore"]),
        oldestSeqAfter: try intValue(json["oldestSeqAfter"]),
        hasMore: try boolValue(json["hasMore"]),
        completed: try boolValue(json["completed"])
    )
}

func reactionEntryToMap(_ request: ReactionEntry) -> [String: AnySendable] {
    [
        "emoji": wrapSendable(request.emoji),
        "userIds": wrapSendable(request.userIds),
        "count": wrapSendable(request.count),
    ]
}
func messageLocalStateToMap(_ request: MessageLocalState) -> [String: AnySendable] {
    [
        "sending": wrapSendable(request.sending),
        "failed": wrapSendable(request.failed),
        "isLocal": wrapSendable(request.isLocal),
        "sortTs": wrapSendable(request.sortTs),
    ]
}
func messageContentToMap(_ request: MessageContent) -> [String: AnySendable] {
    [
        "contentType": wrapSendable(request.contentType),
        "data": wrapSendable(plainJsonObject(request.data)),
    ]
}
func createTextMessageRequestToMap(_ request: CreateTextMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "text": wrapSendable(request.text),
    ]
}
func sendMessageResponseToMap(_ request: SendMessageResponse) -> [String: AnySendable] {
    [
        "serverId": wrapSendable(request.serverId),
        "clientMsgId": wrapSendable(request.clientMsgId),
        "conversationId": wrapSendable(request.conversationId),
        "seq": wrapSendable(request.seq),
        "timestamp": wrapSendable(request.timestamp),
    ]
}
func listMessagesRequestToMap(_ request: ListMessagesRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "beforeSeq": wrapSendable(request.beforeSeq),
        "limit": wrapSendable(request.limit),
    ]
}
func listMessagesResponseToMap(_ request: ListMessagesResponse) -> [String: AnySendable] {
    [
        "messages": AnySendable(request.messages.map { messageToWireMap($0).mapValues { $0.value } }),
    ]
}
func messageSearchQueryToMap(_ request: MessageSearchQuery) -> [String: AnySendable] {
    [
        "keyword": wrapSendable(request.keyword),
        "conversationId": wrapSendable(request.conversationId),
        "senderId": wrapSendable(request.senderId),
        "fromTime": wrapSendable(request.fromTime),
        "toTime": wrapSendable(request.toTime),
        "kinds": wrapSendable(request.kinds),
        "limit": wrapSendable(request.limit),
        "includeRecalled": wrapSendable(request.includeRecalled),
    ]
}
func mediaSourceInfoToMap(_ request: MediaSourceInfo) -> [String: AnySendable] {
    [
        "uuid": wrapSendable(request.uuid),
        "imageId": wrapSendable(request.imageId),
        "url": wrapSendable(request.url),
        "mimeType": wrapSendable(request.mimeType),
        "size": wrapSendable(request.size),
        "width": wrapSendable(request.width),
        "height": wrapSendable(request.height),
        "durationMs": wrapSendable(request.durationMs),
    ]
}
func textContentPayloadToMap(_ request: TextContentPayload) -> [String: AnySendable] {
    [
        "text": wrapSendable(request.text),
    ]
}
func imageContentPayloadToMap(_ request: ImageContentPayload) -> [String: AnySendable] {
    [
        "imageId": wrapSendable(request.imageId),
        "source": wrapSendable(optionalPlainWireMap(request.source, mediaSourceInfoToMap)),
        "thumbnail": wrapSendable(optionalPlainWireMap(request.thumbnail, mediaSourceInfoToMap)),
        "description": wrapSendable(request.description),
    ]
}
func imageGroupItemToMap(_ request: ImageGroupItem) -> [String: AnySendable] {
    [
        "imageId": wrapSendable(request.imageId),
        "url": wrapSendable(request.url),
        "title": wrapSendable(request.title),
        "width": wrapSendable(request.width),
        "height": wrapSendable(request.height),
    ]
}
func imageGroupContentPayloadToMap(_ request: ImageGroupContentPayload) -> [String: AnySendable] {
    [
        "images": AnySendable(request.images.map { imageGroupItemToMap($0).mapValues { $0.value } }),
        "title": wrapSendable(request.title),
    ]
}
func videoContentPayloadToMap(_ request: VideoContentPayload) -> [String: AnySendable] {
    [
        "videoId": wrapSendable(request.videoId),
        "source": wrapSendable(optionalPlainWireMap(request.source, mediaSourceInfoToMap)),
        "cover": wrapSendable(optionalPlainWireMap(request.cover, mediaSourceInfoToMap)),
        "description": wrapSendable(request.description),
    ]
}
func audioContentPayloadToMap(_ request: AudioContentPayload) -> [String: AnySendable] {
    [
        "audioId": wrapSendable(request.audioId),
        "source": wrapSendable(optionalPlainWireMap(request.source, mediaSourceInfoToMap)),
        "durationMs": wrapSendable(request.durationMs),
    ]
}
func fileContentPayloadToMap(_ request: FileContentPayload) -> [String: AnySendable] {
    [
        "fileId": wrapSendable(request.fileId),
        "name": wrapSendable(request.name),
        "url": wrapSendable(request.url),
        "mimeType": wrapSendable(request.mimeType),
        "size": wrapSendable(request.size),
    ]
}
func emojiContentPayloadToMap(_ request: EmojiContentPayload) -> [String: AnySendable] {
    [
        "emoji": wrapSendable(request.emoji),
    ]
}
func stickerContentPayloadToMap(_ request: StickerContentPayload) -> [String: AnySendable] {
    [
        "stickerId": wrapSendable(request.stickerId),
        "packageId": wrapSendable(request.packageId),
        "url": wrapSendable(request.url),
        "width": wrapSendable(request.width),
        "height": wrapSendable(request.height),
        "format": wrapSendable(request.format),
    ]
}
func forwardSourceMessageToMap(_ request: ForwardSourceMessage) -> [String: AnySendable] {
    [
        "sourceMessageId": wrapSendable(request.sourceMessageId),
        "sourceConversationId": wrapSendable(request.sourceConversationId),
        "sourceSenderId": wrapSendable(request.sourceSenderId),
        "plainText": wrapSendable(request.plainText),
    ]
}
func forwardContentPayloadToMap(_ request: ForwardContentPayload) -> [String: AnySendable] {
    [
        "merge": wrapSendable(request.merge),
        "title": wrapSendable(request.title),
        "sourceMessages": AnySendable(request.sourceMessages.map { forwardSourceMessageToMap($0).mapValues { $0.value } }),
    ]
}
func messageBuildCatalogEntryToMap(_ request: MessageBuildCatalogEntry) -> [String: AnySendable] {
    [
        "op": wrapSendable(request.op),
        "method": wrapSendable(request.method),
        "requestType": wrapSendable(request.requestType),
        "contentType": wrapSendable(request.contentType),
        "messageType": wrapSendable(request.messageType),
        "summary": wrapSendable(request.summary),
        "stability": wrapSendable(request.stability),
    ]
}
func listMessageBuildCatalogResponseToMap(_ request: ListMessageBuildCatalogResponse) -> [String: AnySendable] {
    [
        "entries": AnySendable(request.entries.map { messageBuildCatalogEntryToMap($0).mapValues { $0.value } }),
    ]
}
func buildTypedMessageRequestToMap(_ request: BuildTypedMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "op": wrapSendable(request.op),
        "data": wrapSendable(optionalPlainJsonObject(request.data)),
    ]
}
func buildTextMessageRequestToMap(_ request: BuildTextMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "text": wrapSendable(request.text),
    ]
}
func buildQuoteMessageRequestToMap(_ request: BuildQuoteMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "quotedMessageId": wrapSendable(request.quotedMessageId),
        "text": wrapSendable(request.text),
        "quotedSenderId": wrapSendable(request.quotedSenderId),
        "quotedTextPreview": wrapSendable(request.quotedTextPreview),
    ]
}
func buildThreadReplyMessageRequestToMap(_ request: BuildThreadReplyMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "threadId": wrapSendable(request.threadId),
        "text": wrapSendable(request.text),
    ]
}
func buildForwardMessageRequestToMap(_ request: BuildForwardMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "merge": wrapSendable(request.merge),
        "title": wrapSendable(request.title),
                // sourceMessages 是**完整消息**而不是 id 存根：转发载荷要把原文嵌进去，
        // 核心侧 forward_item_from_source 会读 content / senderId / conversationId。
        // 上面 forwardContentPayloadToMap 里有一段长得一模一样的代码，**那处本来就该**
        // 用 id 存根 —— 只有这里要改，别改错位置。
        "sourceMessages": AnySendable(request.sourceMessages.map { messageToWireMap($0).mapValues { $0.value } }),
    ]
}
func buildImageMessageRequestToMap(_ request: BuildImageMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "imageId": wrapSendable(request.imageId),
        "payload": wrapSendable(optionalPlainWireMap(request.payload, imageContentPayloadToMap)),
    ]
}
func buildImageGroupMessageRequestToMap(_ request: BuildImageGroupMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "payload": AnySendable(imageGroupContentPayloadToMap(request.payload).mapValues { $0.value }),
    ]
}
func buildVideoMessageRequestToMap(_ request: BuildVideoMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "videoId": wrapSendable(request.videoId),
        "payload": wrapSendable(optionalPlainWireMap(request.payload, videoContentPayloadToMap)),
    ]
}
func buildAudioMessageRequestToMap(_ request: BuildAudioMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "audioId": wrapSendable(request.audioId),
        "payload": wrapSendable(optionalPlainWireMap(request.payload, audioContentPayloadToMap)),
    ]
}
func buildFileMessageRequestToMap(_ request: BuildFileMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "fileId": wrapSendable(request.fileId),
        "payload": wrapSendable(optionalPlainWireMap(request.payload, fileContentPayloadToMap)),
    ]
}
func buildEmojiMessageRequestToMap(_ request: BuildEmojiMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "emoji": wrapSendable(request.emoji),
    ]
}
func buildLocationMessageRequestToMap(_ request: BuildLocationMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "latitude": wrapSendable(request.latitude),
        "longitude": wrapSendable(request.longitude),
        "title": wrapSendable(request.title),
        "address": wrapSendable(request.address),
    ]
}
func buildStickerMessageRequestToMap(_ request: BuildStickerMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "stickerId": wrapSendable(request.stickerId),
        "packageId": wrapSendable(request.packageId),
        "payload": wrapSendable(optionalPlainWireMap(request.payload, stickerContentPayloadToMap)),
    ]
}
func buildLinkCardMessageRequestToMap(_ request: BuildLinkCardMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "url": wrapSendable(request.url),
        "title": wrapSendable(request.title),
        "description": wrapSendable(request.description),
    ]
}
func buildCardMessageRequestToMap(_ request: BuildCardMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "id": wrapSendable(request.id),
        "cardType": wrapSendable(request.cardType),
        "title": wrapSendable(request.title),
        "subtitle": wrapSendable(request.subtitle),
        "avatar": wrapSendable(request.avatar),
        "attributes": wrapSendable(request.attributes),
    ]
}
func buildMiniProgramMessageRequestToMap(_ request: BuildMiniProgramMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "appId": wrapSendable(request.appId),
        "pagePath": wrapSendable(request.pagePath),
        "title": wrapSendable(request.title),
        "thumbnailUrl": wrapSendable(request.thumbnailUrl),
        "extra": wrapSendable(request.extra),
    ]
}
func buildRichDocMessageRequestToMap(_ request: BuildRichDocMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "docJson": wrapSendable(request.docJson),
        "contentSchema": wrapSendable(request.contentSchema),
        "plainText": wrapSendable(request.plainText),
        "inputFormat": wrapSendable(request.inputFormat),
        "inputFormatVersion": wrapSendable(request.inputFormatVersion),
        "sourcePayload": wrapSendable(request.sourcePayload),
        "title": wrapSendable(request.title),
        "searchText": wrapSendable(request.searchText),
        "renderHintsJson": wrapSendable(request.renderHintsJson),
    ]
}
func normalizeRichDocFromMarkdownRequestToMap(_ request: NormalizeRichDocFromMarkdownRequest) -> [String: AnySendable] {
    [
        "markdown": wrapSendable(request.markdown),
    ]
}
func normalizeRichDocFromHtmlRequestToMap(_ request: NormalizeRichDocFromHtmlRequest) -> [String: AnySendable] {
    [
        "html": wrapSendable(request.html),
    ]
}
func normalizeRichDocFromDocJsonRequestToMap(_ request: NormalizeRichDocFromDocJsonRequest) -> [String: AnySendable] {
    [
        "docJson": wrapSendable(request.docJson),
    ]
}
func richDocV2NormalizedToMap(_ request: RichDocV2Normalized) -> [String: AnySendable] {
    [
        "docJson": wrapSendable(request.docJson),
        "contentSchema": wrapSendable(request.contentSchema),
        "version": wrapSendable(request.version),
        "plainText": wrapSendable(request.plainText),
        "searchText": wrapSendable(request.searchText),
        "renderHints": wrapSendable(plainJsonObject(request.renderHints)),
        "inputFormat": wrapSendable(request.inputFormat),
        "sourcePayload": wrapSendable(optionalPlainJsonObject(request.sourcePayload)),
    ]
}
func buildSystemMessageRequestToMap(_ request: BuildSystemMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "eventKind": wrapSendable(request.eventKind),
        "body": wrapSendable(request.body),
    ]
}
func buildNotificationMessageRequestToMap(_ request: BuildNotificationMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "title": wrapSendable(request.title),
        "body": wrapSendable(request.body),
    ]
}
func buildVoteMessageRequestToMap(_ request: BuildVoteMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "voteId": wrapSendable(request.voteId),
        "title": wrapSendable(request.title),
        "options": wrapSendable(request.options),
        "participantUserIds": wrapSendable(request.participantUserIds),
    ]
}
func buildTaskMessageRequestToMap(_ request: BuildTaskMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "taskId": wrapSendable(request.taskId),
        "title": wrapSendable(request.title),
        "status": wrapSendable(request.status),
        "participantUserIds": wrapSendable(request.participantUserIds),
    ]
}
func buildScheduleMessageRequestToMap(_ request: BuildScheduleMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "scheduleId": wrapSendable(request.scheduleId),
        "title": wrapSendable(request.title),
        "startTimeMs": wrapSendable(request.startTimeMs),
        "endTimeMs": wrapSendable(request.endTimeMs),
        "participantUserIds": wrapSendable(request.participantUserIds),
    ]
}
func buildAnnouncementMessageRequestToMap(_ request: BuildAnnouncementMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "title": wrapSendable(request.title),
        "body": wrapSendable(request.body),
    ]
}
func buildCustomMessageRequestToMap(_ request: BuildCustomMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "type": wrapSendable(request.type),
    ]
}
func buildPlaceholderMessageRequestToMap(_ request: BuildPlaceholderMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "reason": wrapSendable(request.reason),
    ]
}
func buildWithContentMessageRequestToMap(_ request: BuildWithContentMessageRequest) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "content": AnySendable(messageContentToWireMap(request.content).mapValues { $0.value }),
    ]
}
func sdkErrorPayloadToMap(_ request: SdkErrorPayload) -> [String: AnySendable] {
    [
        "code": wrapSendable(request.code),
        "machineReason": wrapSendable(request.machineReason),
        "message": wrapSendable(request.message),
        "operation": wrapSendable(request.operation),
        "retryable": wrapSendable(request.retryable),
        "details": wrapSendable(request.details),
    ]
}
func sdkEventEnvelopeToMap(_ request: SdkEventEnvelope) -> [String: AnySendable] {
    [
        "eventId": wrapSendable(request.eventId),
        "kind": wrapSendable(request.kind),
        "name": wrapSendable(request.name),
        "occurredAt": wrapSendable(request.occurredAt),
        "traceId": wrapSendable(request.traceId),
        "payload": wrapSendable(plainJsonObject(request.payload)),
    ]
}
func lifecycleEventToMap(_ request: LifecycleEvent) -> [String: AnySendable] {
    [
        "name": wrapSendable(request.name),
        "operation": wrapSendable(request.operation),
        "userId": wrapSendable(request.userId),
        "sessionId": wrapSendable(request.sessionId),
        "error": wrapSendable(request.error),
    ]
}
func connectionEventToMap(_ request: ConnectionEvent) -> [String: AnySendable] {
    [
        "name": wrapSendable(request.name),
        "state": wrapSendable(request.state),
        "reason": wrapSendable(request.reason),
        "attempt": wrapSendable(request.attempt),
        "error": wrapSendable(request.error),
    ]
}
func messageReceivedEventToMap(_ request: MessageReceivedEvent) -> [String: AnySendable] {
    [
        "message": wrapSendable(request.message),
    ]
}
func messageReceivedBatchEventToMap(_ request: MessageReceivedBatchEvent) -> [String: AnySendable] {
    [
        "messages": AnySendable(request.messages.map { messageToWireMap($0).mapValues { $0.value } }),
    ]
}
func messageSendAckEventToMap(_ request: MessageSendAckEvent) -> [String: AnySendable] {
    [
        "ack": wrapSendable(request.ack),
    ]
}
func messageSendFailedEventToMap(_ request: MessageSendFailedEvent) -> [String: AnySendable] {
    [
        "clientMsgId": wrapSendable(request.clientMsgId),
        "reason": wrapSendable(request.reason),
        "error": wrapSendable(request.error),
    ]
}
func messageMutationEventToMap(_ request: MessageMutationEvent) -> [String: AnySendable] {
    [
        "name": wrapSendable(request.name),
        "conversationId": wrapSendable(request.conversationId),
        "messageId": wrapSendable(request.messageId),
        "serverMsgId": wrapSendable(request.serverMsgId),
        "userId": wrapSendable(request.userId),
        "reason": wrapSendable(request.reason),
    ]
}
func typingEventToMap(_ request: TypingEvent) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "userId": wrapSendable(request.userId),
        "typing": wrapSendable(request.typing),
    ]
}
func readReceiptEventToMap(_ request: ReadReceiptEvent) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "userId": wrapSendable(request.userId),
        "readSeq": wrapSendable(request.readSeq),
    ]
}
func reactionChangedEventToMap(_ request: ReactionChangedEvent) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "serverMsgId": wrapSendable(request.serverMsgId),
        "userId": wrapSendable(request.userId),
        "emoji": wrapSendable(request.emoji),
        "action": wrapSendable(request.action),
    ]
}
func conversationEventToMap(_ request: ConversationEvent) -> [String: AnySendable] {
    [
        "name": wrapSendable(request.name),
        "conversationId": wrapSendable(request.conversationId),
        "conversationIds": wrapSendable(request.conversationIds),
        "unreadCount": wrapSendable(request.unreadCount),
    ]
}
func presenceChangedEventToMap(_ request: PresenceChangedEvent) -> [String: AnySendable] {
    [
        "conversationId": wrapSendable(request.conversationId),
        "userId": wrapSendable(request.userId),
        "status": wrapSendable(request.status),
        "extra": wrapSendable(request.extra),
    ]
}
func progressEventToMap(_ request: ProgressEvent) -> [String: AnySendable] {
    [
        "name": wrapSendable(request.name),
        "operation": wrapSendable(request.operation),
        "current": wrapSendable(request.current),
        "total": wrapSendable(request.total),
        "taskId": wrapSendable(request.taskId),
        "detail": wrapSendable(request.detail),
    ]
}
func syncEventToMap(_ request: SyncEvent) -> [String: AnySendable] {
    [
        "name": wrapSendable(request.name),
        "trigger": wrapSendable(request.trigger),
        "phase": wrapSendable(request.phase),
        "task": wrapSendable(request.task),
        "progress": wrapSendable(request.progress),
        "error": wrapSendable(request.error),
    ]
}

func sendMessageRequestToMap(_ request: SendMessageRequest) -> [String: AnySendable] {
    var result: [String: AnySendable] = ["message": AnySendable(messageToWireMap(request.message))]
    if let expected = request.expectedUserId { result["expectedUserId"] = AnySendable(expected) }
    return result
}

func messageContentToWireMap(_ content: MessageContent) -> [String: AnySendable] {
    var out = content.data.mapValues { AnySendable($0) }
    out["contentType"] = AnySendable(content.contentType.rawValue)
    return out
}

func messageToWireMap(_ message: Message) -> [String: AnySendable] {
    var out: [String: AnySendable] = [
        "serverId": AnySendable(message.serverId),
        "clientMsgId": AnySendable(message.clientMsgId),
        "conversationId": AnySendable(message.conversationId),
        "conversationType": AnySendable(message.conversationType),
        "channelId": AnySendable(message.channelId),
        "senderId": AnySendable(message.senderId),
        "source": AnySendable(message.source),
        "conversationSeq": AnySendable(message.conversationSeq),
        "createdAt": AnySendable(message.createdAt),
        "clientCreatedAt": AnySendable(message.clientCreatedAt),
        "messageType": AnySendable(message.messageType),
        "senderName": AnySendable(message.senderName),
        "senderAvatar": AnySendable(message.senderAvatar),
        "senderDisplayName": AnySendable(message.senderDisplayName),
        "status": AnySendable(message.status),
        "isRead": AnySendable(message.isRead),
        "isRecalled": AnySendable(message.isRecalled),
        "isEdited": AnySendable(message.isEdited),
        "mentionUsers": AnySendable(message.mentionUsers),
        "mentionAll": AnySendable(message.mentionAll),
        "attributes": AnySendable(message.attributes),
        "extensions": AnySendable(message.extensions),
        "reactions": AnySendable(message.reactions.map { reactionEntryToMap($0).mapValues { $0.value } }),
        "version": AnySendable(message.version),
        "updatedAt": AnySendable(message.updatedAt),
        "textPreview": AnySendable(message.textPreview),
        "timelineKey": AnySendable(message.timelineKey),
        "timelineSortTs": AnySendable(message.timelineSortTs),
    ]
    if let content = message.content { out["content"] = AnySendable(messageContentToWireMap(content).mapValues { $0.value }) }
    if let replyTo = message.replyTo { out["replyTo"] = AnySendable(replyTo) }
    if let quotePreview = message.quotePreview { out["quotePreview"] = AnySendable(quotePreview) }
    if let threadId = message.threadId { out["threadId"] = AnySendable(threadId) }
    if let localState = message.localState { out["localState"] = AnySendable(messageLocalStateToMap(localState).mapValues { $0.value }) }
    return out
}

func intValue(_ value: Any?) throws -> UInt64 {
    if let number = value as? UInt64 { return number }
    if let number = value as? UInt { return UInt64(number) }
    if let number = value as? Int, number >= 0 { return UInt64(number) }
    if let number = value as? Int64, number >= 0 { return UInt64(number) }
    if let number = value as? Double, number.isFinite, number >= 0, number.rounded(.towardZero) == number {
        return UInt64(number)
    }
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "wire field must be an unsigned integer",
        operation: "wire.decode",
        details: ["expected": "unsigned integer"]
    )
}

func signedIntValue(_ value: Any?) throws -> Int64 {
    if let number = value as? Int64 { return number }
    if let number = value as? Int { return Int64(number) }
    if let number = value as? UInt64, number <= UInt64(Int64.max) { return Int64(number) }
    if let number = value as? UInt, UInt64(number) <= UInt64(Int64.max) { return Int64(number) }
    if let number = value as? Double, number.isFinite, number.rounded(.towardZero) == number,
       number >= -9_223_372_036_854_775_808, number < 9_223_372_036_854_775_808 {
        return Int64(number)
    }
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "wire field must be an integer",
        operation: "wire.decode",
        details: ["expected": "integer"]
    )
}

func listOfMaps(_ value: Any?) -> [[String: Any]] {
    guard let items = value as? [Any] else { return [] }
    return items.compactMap { $0 as? [String: Any] }
}

func requiredListOfMaps(_ value: Any?, _ field: String, _ context: String) throws -> [[String: Any]] {
    guard let items = value as? [Any] else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "\(context).\(field) must be an array",
            operation: "wire.decode",
            details: ["field": "\(context).\(field)", "expected": "array"]
        )
    }
    return try items.enumerated().map { index, item in
        guard let map = item as? [String: Any] else {
            throw FlareSdkException(
                code: SdkErrorCodes.invalidParameter,
                message: "\(context).\(field)[\(index)] must be an object",
                operation: "wire.decode",
                details: ["field": "\(context).\(field)[\(index)]", "expected": "object"]
            )
        }
        return map
    }
}

func requiredStringField(_ json: [String: Any], _ key: String, _ context: String) throws -> String {
    guard let text = json[key] as? String else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "\(context).\(key) is required",
            operation: "wire.decode",
            details: ["field": "\(context).\(key)", "expected": "string"]
        )
    }
    return text
}

func requiredUInt64Field(_ json: [String: Any], _ key: String, _ context: String) throws -> UInt64 {
    guard let value = json[key] else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "\(context).\(key) is required",
            operation: "wire.decode",
            details: ["field": "\(context).\(key)", "expected": "unsigned integer"]
        )
    }
    if let number = value as? UInt64 { return number }
    if let number = value as? UInt { return UInt64(number) }
    if let number = value as? Int, number >= 0 { return UInt64(number) }
    if let number = value as? Int64, number >= 0 { return UInt64(number) }
    if let number = value as? Double, number.isFinite, number >= 0, number.rounded(.towardZero) == number {
        return UInt64(number)
    }
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "\(context).\(key) must be an unsigned integer",
        operation: "wire.decode",
        details: ["field": "\(context).\(key)", "expected": "unsigned integer"]
    )
}

func requiredBoolField(_ json: [String: Any], _ key: String, _ context: String) throws -> Bool {
    guard let value = json[key] else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "\(context).\(key) is required",
            operation: "wire.decode",
            details: ["field": "\(context).\(key)", "expected": "boolean"]
        )
    }
    guard let bool = value as? Bool else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "\(context).\(key) must be a boolean",
            operation: "wire.decode",
            details: ["field": "\(context).\(key)", "expected": "boolean"]
        )
    }
    return bool
}

func requiredStringList(_ value: Any?, _ field: String, _ context: String) throws -> [String] {
    guard let items = value as? [Any] else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "\(context).\(field) must be an array",
            operation: "wire.decode",
            details: ["field": "\(context).\(field)", "expected": "array"]
        )
    }
    return try items.enumerated().map { index, item in
        guard let text = item as? String else {
            throw FlareSdkException(
                code: SdkErrorCodes.invalidParameter,
                message: "\(context).\(field)[\(index)] must be a string",
                operation: "wire.decode",
                details: ["field": "\(context).\(field)[\(index)]", "expected": "string"]
            )
        }
        return text
    }
}

func requiredStringMap(_ value: Any?, _ field: String, _ context: String) throws -> [String: String] {
    guard let map = value as? [String: Any] else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "\(context).\(field) must be an object",
            operation: "wire.decode",
            details: ["field": "\(context).\(field)", "expected": "object"]
        )
    }
    return try map.reduce(into: [String: String]()) { out, item in
        guard let value = item.value as? String else {
            throw FlareSdkException(
                code: SdkErrorCodes.invalidParameter,
                message: "\(context).\(field).\(item.key) must be a string",
                operation: "wire.decode",
                details: ["field": "\(context).\(field).\(item.key)", "expected": "string"]
            )
        }
        out[item.key] = value
    }
}

func plainMap(_ value: [String: AnySendable]) -> [String: Any] {
    value.mapValues { $0.value }
}

func plainJsonObject(_ value: [String: AnySendable]) -> [String: Any] {
    value.mapValues { $0.value }
}

func optionalPlainJsonObject(_ value: [String: AnySendable]?) -> [String: Any]? {
    value.map { plainJsonObject($0) }
}

func optionalPlainWireMap<T>(
    _ value: T?,
    _ encode: (T) -> [String: AnySendable]
) -> [String: Any]? {
    value.map { encode($0).mapValues { $0.value } }
}

func jsonObjectMap(_ value: Any?) -> [String: AnySendable] {
    if let map = value as? [String: AnySendable] { return map }
    guard let map = value as? [String: Any] else { return [:] }
    return map.mapValues { AnySendable($0) }
}

func stringValue(_ value: Any?) throws -> String {
    guard let text = value as? String else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "wire field must be a string",
            operation: "wire.decode",
            details: ["expected": "string"]
        )
    }
    return text
}

func stringMap(_ value: Any?) throws -> [String: String] {
    guard let map = value as? [String: Any] else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "wire field must be a string map",
            operation: "wire.decode",
            details: ["expected": "string map"]
        )
    }
    return try map.reduce(into: [String: String]()) { out, item in
        out[item.key] = try stringValue(item.value)
    }
}

func stringList(_ value: Any?) throws -> [String] {
    if let items = value as? [String] { return items }
    guard let items = value as? [Any] else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "wire field must be a string array",
            operation: "wire.decode",
            details: ["expected": "string array"]
        )
    }
    return try items.enumerated().map { index, item in
        guard let text = item as? String else {
            throw FlareSdkException(
                code: SdkErrorCodes.invalidParameter,
                message: "wire string array item \(index) must be a string",
                operation: "wire.decode",
                details: ["expected": "string"]
            )
        }
        return text
    }
}

func bytesMap(_ value: Any?) -> [String: [UInt8]] {
    if let map = value as? [String: [UInt8]] { return map }
    guard let map = value as? [String: Any] else { return [:] }
    return map.reduce(into: [String: [UInt8]]()) { out, item in
        if let bytes = item.value as? [UInt8] {
            out[item.key] = bytes
        } else if let numbers = item.value as? [Any] {
            out[item.key] = numbers.compactMap { element in
                if let byte = element as? UInt8 { return byte }
                if let number = element as? Int, (0...255).contains(number) { return UInt8(number) }
                if let number = element as? UInt64, number <= 255 { return UInt8(number) }
                return nil
            }
        } else if let text = item.value as? String, let data = Data(base64Encoded: text) {
            out[item.key] = Array(data)
        }
    }
}

func boolValue(_ value: Any?) throws -> Bool {
    guard let bool = value as? Bool else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "wire field must be a boolean",
            operation: "wire.decode",
            details: ["expected": "boolean"]
        )
    }
    return bool
}

func optionalUInt64Value(_ value: Any?) throws -> UInt64? {
    guard let value, !(value is NSNull) else { return nil }
    return try intValue(value)
}

func timelineSyncStateFromJson(_ value: Any?) throws -> TimelineSyncState {
    let raw = try stringValue(value).trimmingCharacters(in: .whitespacesAndNewlines)
    if let state = TimelineSyncState(rawValue: raw) { return state }
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "invalid timeline sync state: \(raw.isEmpty ? "<empty>" : raw)",
        operation: "wire.timeline.decode",
        details: ["field": "syncState"]
    )
}

func heartbeatEffectiveIntervalResponseFromJson(_ value: [String: AnySendable]) throws -> HeartbeatEffectiveIntervalResponse {
    let json = plainMap(value)
    return HeartbeatEffectiveIntervalResponse(
        connected: try boolValue(json["connected"]),
        intervalMs: try optionalUInt64Value(json["intervalMs"]),
        intervalSecs: try optionalUInt64Value(json["intervalSecs"])
    )
}

func serverClockResponseFromJson(_ value: [String: AnySendable]) throws -> ServerClockResponse {
    let json = plainMap(value)
    return ServerClockResponse(
        offsetMs: try signedIntValue(json["offsetMs"]),
        estimated: try boolValue(json["estimated"])
    )
}

func networkChangeResponseFromJson(_ value: [String: AnySendable]) throws -> NetworkChangeResponse {
    let json = plainMap(value)
    return NetworkChangeResponse(reconnected: try boolValue(json["reconnected"]))
}

func runtimeHealthResponseFromJson(_ value: [String: AnySendable]) throws -> RuntimeHealthResponse {
    let json = plainMap(value)
    return RuntimeHealthResponse(
        metricsEnabled: try boolValue(json["metricsEnabled"]),
        state: try stringValue(json["state"]),
        stateCode: Int32(try intValue(json["stateCode"])),
        sessionGeneration: try intValue(json["sessionGeneration"]),
        rawSubscriberDroppedTotal: try intValue(json["rawSubscriberDroppedTotal"]),
        metricsJson: try stringValue(json["metricsJson"])
    )
}

public func homeTimelineSnapshotFromJson(_ value: [String: AnySendable]) throws -> HomeTimelineSnapshot {
    let json = plainMap(value)
    var conversations: [Conversation] = []
    for item in try requiredListOfMaps(json["conversations"], "conversations", "HomeTimelineSnapshot") {
        conversations.append(try conversationFromJson(item))
    }
    return HomeTimelineSnapshot(
        conversations: conversations,
        syncState: try timelineSyncStateFromJson(json["syncState"]),
        totalUnread: try intValue(json["totalUnread"])
    )
}

public func conversationTimelineSnapshotFromJson(_ value: [String: AnySendable]) throws -> ConversationTimelineSnapshot {
    let json = plainMap(value)
    let conversation: Conversation?
    if let rawConversation = json["conversation"] as? [String: Any] {
        conversation = try conversationFromJson(rawConversation)
    } else {
        conversation = nil
    }
    var messages: [Message] = []
    for item in try requiredListOfMaps(json["messages"], "messages", "ConversationTimelineSnapshot") {
        messages.append(try messageFromJson(item))
    }
    return ConversationTimelineSnapshot(
        conversation: conversation,
        hasMore: try boolValue(json["hasMore"]),
        messages: messages
    )
}

func viewSnapshotFromJson(_ value: Any?) throws -> ViewSnapshot {
    let json: [String: Any]
    if let map = value as? [String: AnySendable] {
        json = plainMap(map)
    } else {
        json = value as? [String: Any] ?? [:]
    }
    let viewType = try viewTypeFromJson(json["viewType"], "ViewSnapshot")
    return ViewSnapshot(
        viewType: viewType,
        data: jsonObjectMap(json["data"])
    )
}

func viewTypeFromJson(_ value: Any?, _ context: String) throws -> String {
    let raw = try stringValue(value).trimmingCharacters(in: .whitespacesAndNewlines)
    if raw == "timeline" || raw == "conversationList" {
        return raw
    }
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "invalid view type: \(raw.isEmpty ? "<empty>" : raw)",
        operation: "wire.view.decode",
        details: ["field": "\(context).viewType"]
    )
}

func viewOpenResponseFromJson(_ value: [String: AnySendable]) throws -> ViewOpenResponse {
    let json = plainMap(value)
    return ViewOpenResponse(
        viewId: try requiredStringField(json, "viewId", "ViewOpenResponse"),
        revision: try requiredUInt64Field(json, "revision", "ViewOpenResponse"),
        snapshot: try viewSnapshotFromJson(json["snapshot"])
    )
}

/// Decodes `ViewSnapshot.data` of a timeline view.
public func timelineViewSnapshotFromJson(_ value: [String: AnySendable]) throws -> TimelineViewSnapshot {
    let json = plainMap(value)
    let conversation: Conversation?
    if let rawConversation = json["conversation"] as? [String: Any] {
        conversation = try conversationFromJson(rawConversation)
    } else {
        conversation = nil
    }
    var messages: [Message] = []
    for item in try requiredListOfMaps(json["messages"], "messages", "TimelineViewSnapshot") {
        messages.append(try messageFromJson(item))
    }
    let anchorKey = (json["anchorKey"] as? String).flatMap { $0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : $0 }
    return TimelineViewSnapshot(
        conversation: conversation,
        messages: messages,
        hasOlder: try boolValue(json["hasOlder"]),
        hasNewer: try boolValue(json["hasNewer"]),
        attachedToLatest: try boolValue(json["attachedToLatest"]),
        anchorKey: anchorKey
    )
}

func timelineViewPageResponseFromJson(_ value: [String: AnySendable]) throws -> TimelineViewPageResponse {
    let json = plainMap(value)
    return TimelineViewPageResponse(
        viewId: try requiredStringField(json, "viewId", "TimelineViewPageResponse"),
        loadedCount: UInt32(try requiredUInt64Field(json, "loadedCount", "TimelineViewPageResponse")),
        revision: try requiredUInt64Field(json, "revision", "TimelineViewPageResponse")
    )
}

func viewDeltaOpKindFromJson(_ value: Any?) throws -> String {
    let raw = try stringValue(value).trimmingCharacters(in: .whitespacesAndNewlines)
    if raw == "insert" || raw == "update" || raw == "remove" || raw == "move" {
        return raw
    }
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "invalid view delta op: \(raw.isEmpty ? "<empty>" : raw)",
        operation: "wire.view.decode",
        details: ["field": "ViewDeltaOp.op"]
    )
}

func viewDeltaOpFromJson(_ value: Any?) throws -> ViewDeltaOp {
    let json: [String: Any]
    if let map = value as? [String: AnySendable] {
        json = plainMap(map)
    } else {
        json = value as? [String: Any] ?? [:]
    }
    let rawFromIndex = json["fromIndex"]
    let rawItem = json["item"]
    if rawItem != nil && !(rawItem is [String: Any]) && !(rawItem is [String: AnySendable]) {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "ViewDeltaOp.item must be an object",
            operation: "wire.view.decode",
            details: ["field": "ViewDeltaOp.item", "expected": "object"]
        )
    }
    return ViewDeltaOp(
        op: try viewDeltaOpKindFromJson(json["op"]),
        key: try requiredStringField(json, "key", "ViewDeltaOp"),
        index: UInt32(try requiredUInt64Field(json, "index", "ViewDeltaOp")),
        fromIndex: rawFromIndex == nil ? nil : UInt32(try requiredUInt64Field(json, "fromIndex", "ViewDeltaOp")),
        item: rawItem == nil ? nil : jsonObjectMap(rawItem)
    )
}

func viewDeltaRowKeys(_ value: Any?, _ field: String) throws -> [String] {
    guard let list = value as? [Any], list.allSatisfy({ $0 is String }) else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "ViewDelta.\(field) must be a list of row keys",
            operation: "wire.view.decode",
            details: ["field": "ViewDelta.\(field)", "expected": "string[]"]
        )
    }
    return list.compactMap { $0 as? String }
}

/// Timeline deltas work by row key (`clientMsgId`, else `serverId`, else
/// `seq:<conversationId>:<conversationSeq>`): remove, upsert in place, prepend, append, then
/// reorder when present. Conversation-list deltas keep the indexed `ops`.
func viewDeltaFromJson(_ value: Any?) throws -> ViewDelta {
    let json: [String: Any]
    if let map = value as? [String: AnySendable] {
        json = plainMap(map)
    } else {
        json = value as? [String: Any] ?? [:]
    }
    let viewType = try viewTypeFromJson(json["viewType"], "ViewDelta")
    if viewType == "conversationList" {
        let rawTotalUnread = json["totalUnread"]
        let rawSyncState = json["syncState"]
        return ViewDelta(
            viewType: viewType,
            ops: try requiredListOfMaps(json["ops"], "ops", "ViewDelta").map { try viewDeltaOpFromJson($0) },
            totalUnread: rawTotalUnread == nil ? nil : try intValue(rawTotalUnread),
            syncState: rawSyncState == nil ? nil : try stringValue(rawSyncState)
        )
    }
    let rawConversation = json["conversation"]
    let conversation: Conversation?
    if let map = rawConversation as? [String: AnySendable] {
        conversation = try conversationFromJson(plainMap(map))
    } else if let map = rawConversation as? [String: Any] {
        conversation = try conversationFromJson(map)
    } else {
        conversation = nil
    }
    func rows(_ name: String) throws -> [Message] {
        guard json[name] != nil, !(json[name] is NSNull) else { return [] }
        return try requiredListOfMaps(json[name], name, "ViewDelta").map { try messageFromJson($0) }
    }
    let rawRemove = json["remove"]
    let rawReorder = json["reorder"]
    return ViewDelta(
        viewType: viewType,
        prepend: try rows("prepend"),
        append: try rows("append"),
        upsert: try rows("upsert"),
        remove: rawRemove == nil || rawRemove is NSNull ? [] : try viewDeltaRowKeys(rawRemove, "remove"),
        reorder: rawReorder == nil || rawReorder is NSNull ? nil : try viewDeltaRowKeys(rawReorder, "reorder"),
        hasOlder: (json["hasOlder"] as? Bool) ?? false,
        hasNewer: (json["hasNewer"] as? Bool) ?? false,
        attachedToLatest: (json["attachedToLatest"] as? Bool) ?? false,
        conversation: conversation
    )
}

func viewUpdateFromJson(_ value: [String: AnySendable]) throws -> ViewUpdate {
    let json = plainMap(value)
    let kind = try stringValue(json["kind"]).trimmingCharacters(in: .whitespacesAndNewlines)
    if kind != "snapshot" && kind != "delta" {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "invalid view update kind: \(kind.isEmpty ? "<empty>" : kind)",
            operation: "view.update.decode",
            details: ["field": "kind"]
        )
    }
    return ViewUpdate(
        viewId: try requiredStringField(json, "viewId", "ViewUpdate"),
        revision: try requiredUInt64Field(json, "revision", "ViewUpdate"),
        kind: kind,
        snapshot: kind == "delta" ? nil : try viewSnapshotFromJson(json["snapshot"]),
        delta: kind == "delta" ? try viewDeltaFromJson(json["delta"]) : nil
    )
}

func closeViewResponseFromJson(_ value: [String: AnySendable]) throws -> CloseViewResponse {
    let json = plainMap(value)
    return CloseViewResponse(closed: try boolValue(json["closed"]))
}

func messageContentTypeFromJson(_ value: Any?) throws -> MessageContentType {
    let raw = try stringValue(value).trimmingCharacters(in: .whitespacesAndNewlines)
    if let contentType = MessageContentType(rawValue: raw) { return contentType }
    throw FlareSdkException(
        code: SdkErrorCodes.invalidParameter,
        message: "invalid message content type: \(raw.isEmpty ? "<empty>" : raw)",
        operation: "wire.message.decode",
        details: ["field": "content.contentType"]
    )
}

func messageContentDataFromJson(_ rawContent: [String: Any]) -> [String: AnySendable] {
    var out: [String: AnySendable] = [:]
    for (key, value) in rawContent {
        if key == "contentType" || key == "messageType" { continue }
        out[key] = AnySendable(value)
    }
    return out
}

func optionalStringMap(_ value: Any?) throws -> [String: String]? {
    guard value != nil else { return nil }
    return try stringMap(value)
}

func optionalJsonObjectMap(_ value: Any?) -> [String: AnySendable]? {
    guard value != nil else { return nil }
    return jsonObjectMap(value)
}

func reactionEntryFromJson(_ json: [String: Any]) throws -> ReactionEntry {
    try ReactionEntry(
        count: UInt32(requiredUInt64Field(json, "count", "ReactionEntry")),
        emoji: requiredStringField(json, "emoji", "ReactionEntry"),
        userIds: requiredStringList(json["userIds"], "userIds", "ReactionEntry")
    )
}

func reactionList(_ value: Any?) throws -> [ReactionEntry] {
    try requiredListOfMaps(value, "reactions", "Message").map { try reactionEntryFromJson($0) }
}

func messageLocalStateFromJson(_ value: Any?) throws -> MessageLocalState? {
    guard let json = value as? [String: Any] else { return nil }
    return MessageLocalState(
        failed: try boolValue(json["failed"]),
        isLocal: try boolValue(json["isLocal"]),
        sending: try boolValue(json["sending"]),
        sortTs: try intValue(json["sortTs"])
    )
}

/// The core's receipt projection; nil when absent or malformed (an older core).
func messageReceiptViewFromJson(_ value: Any?) -> MessageReceiptView? {
    guard let json = value as? [String: Any],
          let raw = json["status"] as? String,
          let status = MessageReceiptStatus(rawValue: raw) else { return nil }
    func count(_ summary: [String: Any], _ key: String) -> UInt32? {
        guard let number = summary[key] as? NSNumber, number.int64Value >= 0,
              number.int64Value <= Int64(UInt32.max) else { return nil }
        return UInt32(number.int64Value)
    }
    var readSummary: MessageReadSummary?
    if let summary = json["readSummary"] as? [String: Any],
       let readCount = count(summary, "readCount"),
       let unreadCount = count(summary, "unreadCount"),
       let total = count(summary, "total"), total > 0 {
        readSummary = MessageReadSummary(readCount: readCount, unreadCount: unreadCount, total: total)
    }
    return MessageReceiptView(status: status, readSummary: readSummary)
}

private let UNKNOWN_PERSON_NAME = DisplayName(kind: .unnamed, entity: .person, idTail: "")

/// A projected name: conversation `title` and `lastSender`, message `sender`, participant `name`. Every value the SDK
/// hands over carries `title`, `sender` and `name`, and `lastSender` when there is a latest message; the fields stay
/// optional while hosts move to them, so a value without one (from an older core) reads as nil. It never fails, so a
/// core newer than this package still yields a name: an unknown `entity` reads as `person`, an unknown `kind`, a
/// `named` value without text or a value that is not an object reads as an unnamed person without an id tail, an
/// unknown `source` is dropped. The kits read names by the same rule.
func displayNameFromJson(_ value: Any?) -> DisplayName? {
    guard let value, !(value is NSNull) else { return nil }
    let json: [String: Any]
    if let wrapped = value as? [String: AnySendable] {
        json = plainMap(wrapped)
    } else if let map = value as? [String: Any] {
        json = map
    } else {
        return UNKNOWN_PERSON_NAME
    }
    switch json["kind"] as? String {
    case DisplayNameKind.named.rawValue:
        guard let text = json["text"] as? String else { return UNKNOWN_PERSON_NAME }
        return DisplayName(kind: .named, text: text, source: (json["source"] as? String).flatMap(NameSource.init(rawValue:)))
    case DisplayNameKind.members.rawValue:
        return DisplayName(
            kind: .members,
            names: (json["names"] as? [Any])?.compactMap { $0 as? String } ?? [],
            total: displayNameTotal(json["total"])
        )
    case DisplayNameKind.unnamed.rawValue:
        return DisplayName(
            kind: .unnamed,
            entity: (json["entity"] as? String).flatMap(NameEntity.init(rawValue:)) ?? .person,
            idTail: json["idTail"] as? String ?? ""
        )
    default:
        return UNKNOWN_PERSON_NAME
    }
}

private func displayNameTotal(_ value: Any?) -> UInt32 {
    guard let number = value as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID() else { return 0 }
    let total = number.doubleValue
    guard total.isFinite, total >= 0 else { return 0 }
    return total >= Double(UInt32.max) ? UInt32.max : UInt32(total.rounded(.towardZero))
}

func messagePreviewFromJson(_ value: Any?) throws -> MessagePreview? {
    guard let json = value as? [String: Any] else { return nil }
    return try MessagePreview(
        messageId: requiredStringField(json, "messageId", "MessagePreview"),
        senderId: requiredStringField(json, "senderId", "MessagePreview"),
        text: requiredStringField(json, "text", "MessagePreview"),
        time: requiredUInt64Field(json, "time", "MessagePreview"),
        type: Int32(requiredUInt64Field(json, "type", "MessagePreview"))
    )
}

func conversationParticipantFromJson(_ json: [String: Any]) throws -> ConversationParticipant {
    try ConversationParticipant(
        attributes: requiredStringMap(json["attributes"], "attributes", "ConversationParticipant"),
        joinedAt: requiredUInt64Field(json, "joinedAt", "ConversationParticipant"),
        muted: requiredBoolField(json, "muted", "ConversationParticipant"),
        name: displayNameFromJson(json["name"]),
        nickname: requiredStringField(json, "nickname", "ConversationParticipant"),
        pinned: requiredBoolField(json, "pinned", "ConversationParticipant"),
        roles: requiredStringList(json["roles"], "roles", "ConversationParticipant"),
        userId: requiredStringField(json, "userId", "ConversationParticipant")
    )
}

func conversationParticipantList(_ value: Any?, _ field: String, _ context: String) throws -> [ConversationParticipant] {
    try requiredListOfMaps(value, field, context).map { try conversationParticipantFromJson($0) }
}

func richDocV2NormalizedFromJson(_ value: [String: AnySendable]) throws -> RichDocV2Normalized {
    let json = plainMap(value)
    return RichDocV2Normalized(
        docJson: try stringValue(json["docJson"]),
        contentSchema: try stringValue(json["contentSchema"]),
        version: UInt32(try intValue(json["version"])),
        plainText: try stringValue(json["plainText"]),
        searchText: try stringValue(json["searchText"]),
        renderHints: jsonObjectMap(json["renderHints"]),
        inputFormat: json["inputFormat"] as? String,
        sourcePayload: optionalJsonObjectMap(json["sourcePayload"])
    )
}

public func messageFromJson(_ value: Any) throws -> Message {
    let json: [String: Any]
    if let wrapped = value as? [String: AnySendable] {
        json = plainMap(wrapped)
    } else {
        json = (value as? [String: Any]) ?? [:]
    }
    var content: MessageContent?
    if let rawContent = json["content"] as? [String: Any] {
        let contentType = try messageContentTypeFromJson(rawContent["contentType"])
        let data = messageContentDataFromJson(rawContent)
        content = MessageContent(contentType: contentType, data: data)
    }
    return Message(
        attributes: try requiredStringMap(json["attributes"], "attributes", "Message"),
        channelId: try requiredStringField(json, "channelId", "Message"),
        clientCreatedAt: try requiredUInt64Field(json, "clientCreatedAt", "Message"),
        clientMsgId: try requiredStringField(json, "clientMsgId", "Message"),
        content: content,
        conversationId: try requiredStringField(json, "conversationId", "Message"),
        conversationSeq: try requiredUInt64Field(json, "conversationSeq", "Message"),
        conversationType: Int32(try requiredUInt64Field(json, "conversationType", "Message")),
        createdAt: try requiredUInt64Field(json, "createdAt", "Message"),
        extensions: jsonObjectMap(json["extensions"]).mapValues { value in
            if let bytes = value.value as? [UInt8] { return bytes }
            return []
        },
        isEdited: try requiredBoolField(json, "isEdited", "Message"),
        isRead: try requiredBoolField(json, "isRead", "Message"),
        isRecalled: try requiredBoolField(json, "isRecalled", "Message"),
        localState: try messageLocalStateFromJson(json["localState"]),
        mentionAll: try requiredBoolField(json, "mentionAll", "Message"),
        mentionUsers: try requiredStringList(json["mentionUsers"], "mentionUsers", "Message"),
        messageType: Int32(try requiredUInt64Field(json, "messageType", "Message")),
        quotePreview: json["quotePreview"] as? String,
        reactions: try reactionList(json["reactions"]),
        replyTo: json["replyTo"] as? String,
        sender: displayNameFromJson(json["sender"]),
        senderAvatar: try requiredStringField(json, "senderAvatar", "Message"),
        senderDisplayName: try requiredStringField(json, "senderDisplayName", "Message"),
        senderId: try requiredStringField(json, "senderId", "Message"),
        senderName: try requiredStringField(json, "senderName", "Message"),
        serverId: try requiredStringField(json, "serverId", "Message"),
        source: Int32(try requiredUInt64Field(json, "source", "Message")),
        status: Int32(try requiredUInt64Field(json, "status", "Message")),
        textPreview: try requiredStringField(json, "textPreview", "Message"),
        threadId: json["threadId"] as? String,
        updatedAt: try requiredUInt64Field(json, "updatedAt", "Message"),
        version: try requiredUInt64Field(json, "version", "Message"),
        timelineKey: try requiredStringField(json, "timelineKey", "Message"),
        timelineSortTs: try requiredUInt64Field(json, "timelineSortTs", "Message"),
        receipt: messageReceiptViewFromJson(json["receipt"])
    )
}

func sendAckFromJson(_ value: [String: AnySendable]) throws -> SendMessageResponse {
    let json = plainMap(value)
    return SendMessageResponse(
        ackId: try requiredStringField(json, "ackId", "SendMessageResponse"),
        serverId: try requiredStringField(json, "serverId", "SendMessageResponse"),
        clientMsgId: try requiredStringField(json, "clientMsgId", "SendMessageResponse"),
        conversationId: try requiredStringField(json, "conversationId", "SendMessageResponse"),
        seq: try requiredUInt64Field(json, "seq", "SendMessageResponse"),
        timestamp: try requiredUInt64Field(json, "timestamp", "SendMessageResponse"),
        success: try boolValue(json["success"]),
        errorCode: Int32(try intValue(json["errorCode"])),
        errorMessage: try stringValue(json["errorMessage"])
    )
}

func conversationVersionFromJson(_ value: [String: AnySendable]) throws -> ConversationVersion {
    try conversationVersionFromJson(plainMap(value))
}

func conversationVersionFromJson(_ json: [String: Any]) throws -> ConversationVersion {
    ConversationVersion(
        conversationId: try requiredStringField(json, "conversationId", "ConversationVersion"),
        version: try requiredUInt64Field(json, "version", "ConversationVersion")
    )
}

func syncConversationSummariesResponseFromJson(_ value: [String: AnySendable]) throws -> SyncConversationSummariesResponse {
    let json = plainMap(value)
    var changedConversations: [ConversationVersion] = []
    for item in try requiredListOfMaps(json["changedConversations"], "changedConversations", "SyncConversationSummariesResponse") {
        changedConversations.append(try conversationVersionFromJson(item))
    }
    return SyncConversationSummariesResponse(changedConversations: changedConversations)
}

func listConversationsResponseFromJson(_ value: [String: AnySendable]) throws -> ListConversationsResponse {
    let json = plainMap(value)
    var conversations: [Conversation] = []
    for item in try requiredListOfMaps(json["conversations"], "conversations", "ListConversationsResponse") {
        conversations.append(try conversationFromJson(item))
    }
    return ListConversationsResponse(conversations: conversations)
}

func listMessagesResponseFromJson(_ value: [String: AnySendable]) throws -> ListMessagesResponse {
    let json = plainMap(value)
    var messages: [Message] = []
    for item in try requiredListOfMaps(json["messages"], "messages", "ListMessagesResponse") {
        messages.append(try messageFromJson(item))
    }
    return ListMessagesResponse(messages: messages)
}

public func conversationFromJson(_ value: [String: AnySendable]) throws -> Conversation {
    try conversationFromJson(plainMap(value))
}

public func conversationFromJson(_ json: [String: Any]) throws -> Conversation {
    let rawConversationType = try stringValue(json["conversationType"]).trimmingCharacters(in: .whitespacesAndNewlines)
    guard let conversationType = ConversationType(rawValue: rawConversationType) else {
        throw FlareSdkException(
            code: SdkErrorCodes.invalidParameter,
            message: "invalid conversation type: \(rawConversationType.isEmpty ? "<empty>" : rawConversationType)",
            operation: "wire.conversation.decode",
            details: ["field": "conversationType"]
        )
    }
    return Conversation(
        avatarUrl: try requiredStringField(json, "avatarUrl", "Conversation"),
        badge: json["badge"] as? String,
        businessType: try requiredStringField(json, "businessType", "Conversation"),
        channelId: try requiredStringField(json, "channelId", "Conversation"),
        conversationId: try requiredStringField(json, "conversationId", "Conversation"),
        conversationType: conversationType,
        createdAt: try requiredUInt64Field(json, "createdAt", "Conversation"),
        description: json["description"] as? String,
        displayName: try requiredStringField(json, "displayName", "Conversation"),
        draft: json["draft"] as? String,
        ext: try requiredStringMap(json["ext"], "ext", "Conversation"),
        isArchived: try requiredBoolField(json, "isArchived", "Conversation"),
        isMuted: try requiredBoolField(json, "isMuted", "Conversation"),
        isPinned: try requiredBoolField(json, "isPinned", "Conversation"),
        lastMessage: try messagePreviewFromJson(json["lastMessage"]),
        lastMessageAt: try optionalUInt64Value(json["lastMessageAt"]),
        lastMessageId: json["lastMessageId"] as? String,
        lastMessagePreview: json["lastMessagePreview"] as? String,
        lastReadSeq: try requiredUInt64Field(json, "lastReadSeq", "Conversation"),
        lastSender: displayNameFromJson(json["lastSender"]),
        lastSenderAvatarUrl: try requiredStringField(json, "lastSenderAvatarUrl", "Conversation"),
        lastSenderId: json["lastSenderId"] as? String,
        lastSenderNickname: try requiredStringField(json, "lastSenderNickname", "Conversation"),
        maxSeq: try requiredUInt64Field(json, "maxSeq", "Conversation"),
        memberPreview: try conversationParticipantList(json["memberPreview"], "memberPreview", "Conversation"),
        membersCount: UInt32(try requiredUInt64Field(json, "membersCount", "Conversation")),
        mentionCount: UInt32(try requiredUInt64Field(json, "mentionCount", "Conversation")),
        mentionMe: try requiredBoolField(json, "mentionMe", "Conversation"),
        participantVersion: try requiredUInt64Field(json, "participantVersion", "Conversation"),
        participants: try conversationParticipantList(json["participants"], "participants", "Conversation"),
        peerReadSeq: try requiredUInt64Field(json, "peerReadSeq", "Conversation"),
        remark: json["remark"] as? String,
        role: json["role"] as? String,
        title: displayNameFromJson(json["title"]),
        unreadCount: UInt32(try requiredUInt64Field(json, "unreadCount", "Conversation")),
        updatedAt: try requiredUInt64Field(json, "updatedAt", "Conversation"),
        updatedAtTs: try optionalUInt64Value(json["updatedAtTs"]),
        version: try requiredUInt64Field(json, "version", "Conversation"),
        visibleAfterSeq: try requiredUInt64Field(json, "visibleAfterSeq", "Conversation")
    )
}
