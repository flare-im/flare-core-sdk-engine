package flare_core_harmony_cangjie_sdk.adapter

import encoding.json.JsonBool as CjJsonBool
import encoding.json.JsonInt as CjJsonInt
import encoding.json.JsonObject as CjJsonObject
import encoding.json.JsonString as CjJsonString
import encoding.json.JsonValue as CjJsonValue
import flare_core_harmony_cangjie_sdk.contract.*
import flare_core_harmony_cangjie_sdk.model.*

// GENERATED. Do not edit by hand.

public func jsonObjectFromRaw(raw: String): CjJsonObject {
    try {
        return CjJsonValue.fromStr(raw).asObject()
    } catch (e: Exception) {
        throw Exception("Invalid SDK JSON object: ${e}")
    }
}

public func jsonValueFromRaw(raw: String): CjJsonValue {
    try {
        return CjJsonValue.fromStr(raw)
    } catch (e: Exception) {
        throw Exception("Invalid SDK JSON value: ${e}")
    }
}

public func jsonPutRaw(json: CjJsonObject, key: String, raw: String): Unit {
    json.put(key, jsonValueFromRaw(raw))
}

public func pluginUiEvaluationRequestToJson(request: PluginUiEvaluationRequest): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "payload", request.payload)
    return json.toJsonString()
}

public func messageReadReceiptMembersRequestToJson(request: MessageReadReceiptMembersRequest): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "payload", request.payload)
    return json.toJsonString()
}

public func pluginCommandRequestToJson(request: PluginCommandRequest): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "payload", request.payload)
    return json.toJsonString()
}

public func jsonPutString(json: CjJsonObject, key: String, value: String): Unit {
    json.put(key, CjJsonString(value))
}

public func jsonPutBool(json: CjJsonObject, key: String, value: Bool): Unit {
    json.put(key, CjJsonBool(value))
}

public func jsonPutInt(json: CjJsonObject, key: String, value: Int64): Unit {
    json.put(key, CjJsonInt(value))
}

public func jsonPutFloat(json: CjJsonObject, key: String, value: Float64): Unit {
    jsonPutRaw(json, key, "${value}")
}

public func jsonStringField(object: CjJsonObject, key: String, fallback: String): String {
    return match (object.get(key)) {
        case Some(value) => value.asString().getValue()
        case None => fallback
    }
}

public func jsonOptionalStringField(object: CjJsonObject, key: String): ?String {
    return match (object.get(key)) {
        case Some(value) => Some(value.asString().getValue())
        case None => None
    }
}

public func jsonBoolField(object: CjJsonObject, key: String, fallback: Bool): Bool {
    return match (object.get(key)) {
        case Some(value) => value.asBool().getValue()
        case None => fallback
    }
}

public func jsonOptionalBoolField(object: CjJsonObject, key: String): ?Bool {
    return match (object.get(key)) {
        case Some(value) => Some(value.asBool().getValue())
        case None => None
    }
}

public func jsonIntField(object: CjJsonObject, key: String, fallback: Int64): Int64 {
    return match (object.get(key)) {
        case Some(value) => value.asInt().getValue()
        case None => fallback
    }
}

public func jsonFloatField(object: CjJsonObject, key: String, fallback: Float64): Float64 {
    return match (object.get(key)) {
        case Some(value) => Float64.parse(value.toJsonString())
        case None => fallback
    }
}

public func jsonOptionalInt32Field(object: CjJsonObject, key: String): ?Int32 {
    return match (object.get(key)) {
        case Some(value) => Some(Int32(value.asInt().getValue()))
        case None => None
    }
}

public func jsonOptionalInt64Field(object: CjJsonObject, key: String): ?Int64 {
    return match (object.get(key)) {
        case Some(value) => Some(value.asInt().getValue())
        case None => None
    }
}

public func jsonOptionalUInt32Field(object: CjJsonObject, key: String): ?UInt32 {
    return match (object.get(key)) {
        case Some(value) => Some(UInt32(value.asInt().getValue()))
        case None => None
    }
}

public func jsonOptionalUInt64Field(object: CjJsonObject, key: String): ?UInt64 {
    return match (object.get(key)) {
        case Some(value) => Some(UInt64(value.asInt().getValue()))
        case None => None
    }
}

public func jsonRawField(object: CjJsonObject, key: String, fallback: String): String {
    return match (object.get(key)) {
        case Some(value) => value.toJsonString()
        case None => fallback
    }
}

public func jsonOptionalRawField(object: CjJsonObject, key: String): ?String {
    return match (object.get(key)) {
        case Some(value) => Some(value.toJsonString())
        case None => None
    }
}

public func enumWireIndex<T>(order: Array<T>, value: T): Int64 {
    var index: Int64 = 0
    for (item in order) {
        if (item == value) {
            return index
        }
        index = index + 1
    }
    return 0
}

public func enumFromWireIndex<T>(order: Array<T>, index: Int64, fallback: T): T {
    var current: Int64 = 0
    for (item in order) {
        if (current == index) {
            return item
        }
        current = current + 1
    }
    return fallback
}

public func jsonEnumField<T>(order: Array<T>, object: CjJsonObject, key: String, fallback: T): T {
    return match (object.get(key)) {
        case Some(value) => enumFromWireIndex(order, value.asInt().getValue(), fallback)
        case None => fallback
    }
}

public func jsonOptionalEnumField<T>(order: Array<T>, object: CjJsonObject, key: String): ?T {
    return match (object.get(key)) {
        case Some(value) => Some(enumFromWireIndex(order, value.asInt().getValue(), order[0]))
        case None => None
    }
}

public func conversationTypeWireValue(value: ConversationType): String {
    if (value == ConversationType.Single) { return "single" }
    if (value == ConversationType.Group) { return "group" }
    if (value == ConversationType.Ai) { return "ai" }
    if (value == ConversationType.System) { return "system" }
    if (value == ConversationType.Customer) { return "customer" }
    if (value == ConversationType.Temp) { return "temp" }
    if (value == ConversationType.Channel) { return "channel" }
    if (value == ConversationType.Broadcast) { return "broadcast" }
    return "unspecified"
}

public func conversationTypeFromWireString(value: String): ConversationType {
    if (value == "single") { return ConversationType.Single }
    if (value == "group") { return ConversationType.Group }
    if (value == "ai") { return ConversationType.Ai }
    if (value == "system") { return ConversationType.System }
    if (value == "customer") { return ConversationType.Customer }
    if (value == "temp") { return ConversationType.Temp }
    if (value == "channel") { return ConversationType.Channel }
    if (value == "broadcast") { return ConversationType.Broadcast }
    return ConversationType.Unspecified
}

public func conversationTypeFromWireValue(value: CjJsonValue): ConversationType {
    try {
        return conversationTypeFromWireString(value.asString().getValue())
    } catch (_) {
        return ConversationType.Unspecified
    }
}

public func jsonConversationTypeField(object: CjJsonObject, key: String, fallback: ConversationType): ConversationType {
    return match (object.get(key)) {
        case Some(value) => conversationTypeFromWireValue(value)
        case None => fallback
    }
}

public func timelineSyncStateWireValue(value: TimelineSyncState): String {
    if (value == TimelineSyncState.Synced) { return "synced" }
    if (value == TimelineSyncState.Partial) { return "partial" }
    return "localReady"
}

public func timelineSyncStateFromWireString(value: String): TimelineSyncState {
    if (value == "synced") { return TimelineSyncState.Synced }
    if (value == "partial") { return TimelineSyncState.Partial }
    return TimelineSyncState.LocalReady
}

public func timelineSyncStateFromWireValue(value: CjJsonValue): TimelineSyncState {
    try {
        return timelineSyncStateFromWireString(value.asString().getValue())
    } catch (_) {
        return TimelineSyncState.LocalReady
    }
}

public func jsonTimelineSyncStateField(object: CjJsonObject, key: String, fallback: TimelineSyncState): TimelineSyncState {
    return match (object.get(key)) {
        case Some(value) => timelineSyncStateFromWireValue(value)
        case None => fallback
    }
}

public func networkInterfaceKindWireValue(value: NetworkInterfaceKind): String {
    if (value == NetworkInterfaceKind.Wifi) { return "wifi" }
    if (value == NetworkInterfaceKind.Cellular) { return "cellular" }
    if (value == NetworkInterfaceKind.Ethernet) { return "ethernet" }
    if (value == NetworkInterfaceKind.Other) { return "other" }
    return "unknown"
}

public func networkInterfaceKindFromWireString(value: String): NetworkInterfaceKind {
    if (value == "wifi") { return NetworkInterfaceKind.Wifi }
    if (value == "cellular") { return NetworkInterfaceKind.Cellular }
    if (value == "ethernet") { return NetworkInterfaceKind.Ethernet }
    if (value == "other") { return NetworkInterfaceKind.Other }
    return NetworkInterfaceKind.Unknown
}

public func jsonOptionalNetworkInterfaceKindField(object: CjJsonObject, key: String): ?NetworkInterfaceKind {
    return match (object.get(key)) {
        case Some(value) => Some(networkInterfaceKindFromWireString(value.asString().getValue()))
        case None => None
    }
}

public func messageContentTypeWireValue(value: MessageContentType): String {
    if (value == MessageContentType.Text) { return "text" }
    if (value == MessageContentType.Image) { return "image" }
    if (value == MessageContentType.Video) { return "video" }
    if (value == MessageContentType.Audio) { return "audio" }
    if (value == MessageContentType.File) { return "file" }
    if (value == MessageContentType.Location) { return "location" }
    if (value == MessageContentType.Card) { return "card" }
    if (value == MessageContentType.Sticker) { return "sticker" }
    if (value == MessageContentType.Emoji) { return "emoji" }
    if (value == MessageContentType.Quote) { return "quote" }
    if (value == MessageContentType.LinkCard) { return "link_card" }
    if (value == MessageContentType.Forward) { return "forward" }
    if (value == MessageContentType.Thread) { return "thread" }
    if (value == MessageContentType.MiniProgram) { return "mini_program" }
    if (value == MessageContentType.RichText) { return "rich_text" }
    if (value == MessageContentType.ImageGroup) { return "image_group" }
    if (value == MessageContentType.System) { return "system" }
    if (value == MessageContentType.Notification) { return "notification" }
    if (value == MessageContentType.Vote) { return "vote" }
    if (value == MessageContentType.Task) { return "task" }
    if (value == MessageContentType.Schedule) { return "schedule" }
    if (value == MessageContentType.Announcement) { return "announcement" }
    if (value == MessageContentType.Placeholder) { return "placeholder" }
    return "custom"
}

public func messageContentTypeFromWireString(value: String): MessageContentType {
    if (value == "text") { return MessageContentType.Text }
    if (value == "image") { return MessageContentType.Image }
    if (value == "video") { return MessageContentType.Video }
    if (value == "audio") { return MessageContentType.Audio }
    if (value == "file") { return MessageContentType.File }
    if (value == "location") { return MessageContentType.Location }
    if (value == "card") { return MessageContentType.Card }
    if (value == "sticker") { return MessageContentType.Sticker }
    if (value == "emoji") { return MessageContentType.Emoji }
    if (value == "quote") { return MessageContentType.Quote }
    if (value == "link_card") { return MessageContentType.LinkCard }
    if (value == "forward") { return MessageContentType.Forward }
    if (value == "thread") { return MessageContentType.Thread }
    if (value == "mini_program") { return MessageContentType.MiniProgram }
    if (value == "rich_text") { return MessageContentType.RichText }
    if (value == "image_group") { return MessageContentType.ImageGroup }
    if (value == "system") { return MessageContentType.System }
    if (value == "notification") { return MessageContentType.Notification }
    if (value == "vote") { return MessageContentType.Vote }
    if (value == "task") { return MessageContentType.Task }
    if (value == "schedule") { return MessageContentType.Schedule }
    if (value == "announcement") { return MessageContentType.Announcement }
    if (value == "placeholder") { return MessageContentType.Placeholder }
    return MessageContentType.Custom
}

public func messageContentTypeFromWireValue(value: CjJsonValue): MessageContentType {
    try {
        return messageContentTypeFromWireString(value.asString().getValue())
    } catch (_) {
        return MessageContentType.Custom
    }
}

public func jsonMessageContentTypeField(object: CjJsonObject, key: String, fallback: MessageContentType): MessageContentType {
    return match (object.get(key)) {
        case Some(value) => messageContentTypeFromWireValue(value)
        case None => fallback
    }
}

public func messageBuildOpWireValue(value: MessageBuildOp): String {
    if (value == MessageBuildOp.CreateQuote) { return "create_quote" }
    if (value == MessageBuildOp.CreateThreadReply) { return "create_thread_reply" }
    if (value == MessageBuildOp.CreateForward) { return "create_forward" }
    if (value == MessageBuildOp.CreateImage) { return "create_image" }
    if (value == MessageBuildOp.CreateImageGroup) { return "create_image_group" }
    if (value == MessageBuildOp.CreateVideo) { return "create_video" }
    if (value == MessageBuildOp.CreateAudio) { return "create_audio" }
    if (value == MessageBuildOp.CreateFile) { return "create_file" }
    if (value == MessageBuildOp.CreateEmoji) { return "create_emoji" }
    if (value == MessageBuildOp.CreateLocation) { return "create_location" }
    if (value == MessageBuildOp.CreateSticker) { return "create_sticker" }
    if (value == MessageBuildOp.CreateLinkCard) { return "create_link_card" }
    if (value == MessageBuildOp.CreateCard) { return "create_card" }
    if (value == MessageBuildOp.CreateMiniProgram) { return "create_mini_program" }
    if (value == MessageBuildOp.CreateRichDoc) { return "create_rich_doc" }
    if (value == MessageBuildOp.CreateSystem) { return "create_system" }
    if (value == MessageBuildOp.CreateNotification) { return "create_notification" }
    if (value == MessageBuildOp.CreateVote) { return "create_vote" }
    if (value == MessageBuildOp.CreateTask) { return "create_task" }
    if (value == MessageBuildOp.CreateSchedule) { return "create_schedule" }
    if (value == MessageBuildOp.CreateAnnouncement) { return "create_announcement" }
    if (value == MessageBuildOp.CreateCustom) { return "create_custom" }
    if (value == MessageBuildOp.CreatePlaceholder) { return "create_placeholder" }
    if (value == MessageBuildOp.CreateWithContent) { return "create_with_content" }
    return "create_text"
}

public func messageBuildOpFromWireString(value: String): MessageBuildOp {
    if (value == "create_quote") { return MessageBuildOp.CreateQuote }
    if (value == "create_thread_reply") { return MessageBuildOp.CreateThreadReply }
    if (value == "create_forward") { return MessageBuildOp.CreateForward }
    if (value == "create_image") { return MessageBuildOp.CreateImage }
    if (value == "create_image_group") { return MessageBuildOp.CreateImageGroup }
    if (value == "create_video") { return MessageBuildOp.CreateVideo }
    if (value == "create_audio") { return MessageBuildOp.CreateAudio }
    if (value == "create_file") { return MessageBuildOp.CreateFile }
    if (value == "create_emoji") { return MessageBuildOp.CreateEmoji }
    if (value == "create_location") { return MessageBuildOp.CreateLocation }
    if (value == "create_sticker") { return MessageBuildOp.CreateSticker }
    if (value == "create_link_card") { return MessageBuildOp.CreateLinkCard }
    if (value == "create_card") { return MessageBuildOp.CreateCard }
    if (value == "create_mini_program") { return MessageBuildOp.CreateMiniProgram }
    if (value == "create_rich_doc") { return MessageBuildOp.CreateRichDoc }
    if (value == "create_system") { return MessageBuildOp.CreateSystem }
    if (value == "create_notification") { return MessageBuildOp.CreateNotification }
    if (value == "create_vote") { return MessageBuildOp.CreateVote }
    if (value == "create_task") { return MessageBuildOp.CreateTask }
    if (value == "create_schedule") { return MessageBuildOp.CreateSchedule }
    if (value == "create_announcement") { return MessageBuildOp.CreateAnnouncement }
    if (value == "create_custom") { return MessageBuildOp.CreateCustom }
    if (value == "create_placeholder") { return MessageBuildOp.CreatePlaceholder }
    if (value == "create_with_content") { return MessageBuildOp.CreateWithContent }
    return MessageBuildOp.CreateText
}

public func messageBuildOpFromWireValue(value: CjJsonValue): MessageBuildOp {
    try {
        return messageBuildOpFromWireString(value.asString().getValue())
    } catch (_) {
        return MessageBuildOp.CreateText
    }
}

public func jsonMessageBuildOpField(object: CjJsonObject, key: String, fallback: MessageBuildOp): MessageBuildOp {
    return match (object.get(key)) {
        case Some(value) => messageBuildOpFromWireValue(value)
        case None => fallback
    }
}

let HEARTBEAT_APP_STATE_WIRE_ORDER: Array<HeartbeatAppState> = [HeartbeatAppState.Foreground, HeartbeatAppState.Background]
let MESSAGE_SEARCH_KIND_WIRE_ORDER: Array<MessageSearchKind> = [MessageSearchKind.Message, MessageSearchKind.Text, MessageSearchKind.Media, MessageSearchKind.Image, MessageSearchKind.Video, MessageSearchKind.Audio, MessageSearchKind.File]
let SDK_EVENT_KIND_WIRE_ORDER: Array<SdkEventKind> = [SdkEventKind.Lifecycle, SdkEventKind.Connection, SdkEventKind.Message, SdkEventKind.Notification, SdkEventKind.Conversation, SdkEventKind.Sync, SdkEventKind.Extension, SdkEventKind.ExtensionEvent, SdkEventKind.Presence, SdkEventKind.Media, SdkEventKind.Plugin]
let LIFECYCLE_EVENT_NAME_WIRE_ORDER: Array<LifecycleEventName> = [LifecycleEventName.Initializing, LifecycleEventName.Initialized, LifecycleEventName.InitFailed, LifecycleEventName.LoginSucceeded, LifecycleEventName.LoginFailed, LifecycleEventName.LoggedOut, LifecycleEventName.Disposed]
let SDK_CONNECTION_STATE_WIRE_ORDER: Array<SdkConnectionState> = [SdkConnectionState.Disconnected, SdkConnectionState.Connecting, SdkConnectionState.Connected, SdkConnectionState.Ready, SdkConnectionState.Reconnecting]
let CONNECTION_EVENT_NAME_WIRE_ORDER: Array<ConnectionEventName> = [ConnectionEventName.Connecting, ConnectionEventName.Connected, ConnectionEventName.Ready, ConnectionEventName.Disconnected, ConnectionEventName.Reconnecting, ConnectionEventName.ReconnectFailed, ConnectionEventName.StateChanged, ConnectionEventName.SyncStateChanged, ConnectionEventName.ServerError, ConnectionEventName.KickedOff, ConnectionEventName.TokenExpired]
let MESSAGE_EVENT_NAME_WIRE_ORDER: Array<MessageEventName> = [MessageEventName.Received, MessageEventName.ReceivedBatch, MessageEventName.SendAck, MessageEventName.SendFailed, MessageEventName.Recalled, MessageEventName.Typing, MessageEventName.Edited, MessageEventName.ReactionChanged, MessageEventName.Deleted, MessageEventName.ReadReceipt, MessageEventName.BurnScheduled, MessageEventName.Burned, MessageEventName.HardDeleted, MessageEventName.Pinned, MessageEventName.Unpinned, MessageEventName.Marked, MessageEventName.Unmarked, MessageEventName.RetentionScheduled, MessageEventName.RetentionExpired, MessageEventName.RetentionPurged, MessageEventName.PresenceChanged, MessageEventName.Custom, MessageEventName.PluginStateChanged, MessageEventName.ReadSummariesChanged]
let CONVERSATION_EVENT_NAME_WIRE_ORDER: Array<ConversationEventName> = [ConversationEventName.Synced, ConversationEventName.Created, ConversationEventName.Updated, ConversationEventName.UnreadCountChanged, ConversationEventName.Deleted]
let SYNC_EVENT_NAME_WIRE_ORDER: Array<SyncEventName> = [SyncEventName.StateChanged, SyncEventName.Started, SyncEventName.Finished, SyncEventName.Failed, SyncEventName.Progress, SyncEventName.TaskCompleted, SyncEventName.ResyncNeeded]
let PROGRESS_EVENT_NAME_WIRE_ORDER: Array<ProgressEventName> = [ProgressEventName.SyncProgress, ProgressEventName.UploadProgress, ProgressEventName.DownloadProgress]

public func connectionStateFromWire(value: String): ConnectionState {
    if (value == "connecting") { return ConnectionState.Connecting }
    if (value == "connected") { return ConnectionState.Connected }
    if (value == "ready") { return ConnectionState.Ready }
    if (value == "reconnecting") { return ConnectionState.Reconnecting }
    try {
        let object = jsonObjectFromRaw(value)
        return connectionStateFromWire(jsonStringField(object, "state", "disconnected"))
    } catch (_) {
        return ConnectionState.Disconnected
    }
}

public func booleanResponseFromWire(raw: String): BooleanResponse {
    if (raw == "true") { return BooleanResponse(true) }
    if (raw == "false") { return BooleanResponse(false) }
    return booleanResponseFromJson(raw)
}

public func setHeartbeatAppStateRequestToJson(request: SetHeartbeatAppStateRequest): String {
    let json = CjJsonObject()
    jsonPutInt(json, "appState", enumWireIndex(HEARTBEAT_APP_STATE_WIRE_ORDER, request.appState))
    return json.toJsonString()
}

public func setHeartbeatAppStateRequestFromJson(raw: String): SetHeartbeatAppStateRequest {
    return setHeartbeatAppStateRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func setHeartbeatAppStateRequestFromObject(object: CjJsonObject): SetHeartbeatAppStateRequest {
    let appState = jsonEnumField(HEARTBEAT_APP_STATE_WIRE_ORDER, object, "appState", HeartbeatAppState.Foreground)
    return SetHeartbeatAppStateRequest(
        appState: appState
    )
}

public func setHeartbeatNatTimeoutRequestToJson(request: SetHeartbeatNatTimeoutRequest): String {
    let json = CjJsonObject()
    match (request.natTimeoutSecs) {
        case Some(value) => jsonPutInt(json, "natTimeoutSecs", Int64(value))
        case None => ()
    }
    return json.toJsonString()
}

public func setHeartbeatNatTimeoutRequestFromJson(raw: String): SetHeartbeatNatTimeoutRequest {
    return setHeartbeatNatTimeoutRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func setHeartbeatNatTimeoutRequestFromObject(object: CjJsonObject): SetHeartbeatNatTimeoutRequest {
    let natTimeoutSecs: ?UInt32 = jsonOptionalUInt32Field(object, "natTimeoutSecs")
    return SetHeartbeatNatTimeoutRequest(
        natTimeoutSecs: natTimeoutSecs
    )
}

public func networkChangeRequestToJson(request: NetworkChangeRequest): String {
    let json = CjJsonObject()
    match (request.available) {
        case Some(value) => jsonPutBool(json, "available", value)
        case None => ()
    }
    match (request.interface) {
        case Some(value) => jsonPutString(json, "interface", networkInterfaceKindWireValue(value))
        case None => ()
    }
    match (request.expensive) {
        case Some(value) => jsonPutBool(json, "expensive", value)
        case None => ()
    }
    match (request.metered) {
        case Some(value) => jsonPutBool(json, "metered", value)
        case None => ()
    }
    match (request.reason) {
        case Some(value) => jsonPutString(json, "reason", value)
        case None => ()
    }
    return json.toJsonString()
}

public func networkChangeRequestFromJson(raw: String): NetworkChangeRequest {
    return networkChangeRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func networkChangeRequestFromObject(object: CjJsonObject): NetworkChangeRequest {
    let available: ?Bool = jsonOptionalBoolField(object, "available")
    let interfaceValue: ?NetworkInterfaceKind = jsonOptionalNetworkInterfaceKindField(object, "interface")
    let expensive: ?Bool = jsonOptionalBoolField(object, "expensive")
    let metered: ?Bool = jsonOptionalBoolField(object, "metered")
    let reason: ?String = jsonOptionalStringField(object, "reason")
    return NetworkChangeRequest(
        available: available,
        interface: interfaceValue,
        expensive: expensive,
        metered: metered,
        reason: reason
    )
}

public func networkChangeResponseToJson(request: NetworkChangeResponse): String {
    let json = CjJsonObject()
    jsonPutBool(json, "reconnected", request.reconnected)
    return json.toJsonString()
}

public func networkChangeResponseFromJson(raw: String): NetworkChangeResponse {
    return networkChangeResponseFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func networkChangeResponseFromObject(object: CjJsonObject): NetworkChangeResponse {
    let reconnected = jsonBoolField(object, "reconnected", false)
    return NetworkChangeResponse(
        reconnected: reconnected
    )
}

public func serverClockResponseToJson(request: ServerClockResponse): String {
    let json = CjJsonObject()
    jsonPutInt(json, "offsetMs", request.offsetMs)
    jsonPutBool(json, "estimated", request.estimated)
    return json.toJsonString()
}

public func serverClockResponseFromJson(raw: String): ServerClockResponse {
    return serverClockResponseFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func serverClockResponseFromObject(object: CjJsonObject): ServerClockResponse {
    let offsetMs = jsonIntField(object, "offsetMs", 0)
    let estimated = jsonBoolField(object, "estimated", false)
    return ServerClockResponse(
        offsetMs: offsetMs,
        estimated: estimated
    )
}

public func updateConversationDraftRequestToJson(request: UpdateConversationDraftRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    match (request.draft) {
        case Some(value) => jsonPutString(json, "draft", value)
        case None => ()
    }
    return json.toJsonString()
}

public func updateConversationDraftRequestFromJson(raw: String): UpdateConversationDraftRequest {
    return updateConversationDraftRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func updateConversationDraftRequestFromObject(object: CjJsonObject): UpdateConversationDraftRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let draft: ?String = jsonOptionalStringField(object, "draft")
    return UpdateConversationDraftRequest(
        conversationId: conversationId,
        draft: draft
    )
}

public func heartbeatEffectiveIntervalResponseToJson(request: HeartbeatEffectiveIntervalResponse): String {
    let json = CjJsonObject()
    jsonPutBool(json, "connected", request.connected)
    match (request.intervalMs) {
        case Some(value) => jsonPutInt(json, "intervalMs", Int64(value))
        case None => ()
    }
    match (request.intervalSecs) {
        case Some(value) => jsonPutInt(json, "intervalSecs", Int64(value))
        case None => ()
    }
    return json.toJsonString()
}

public func heartbeatEffectiveIntervalResponseFromJson(raw: String): HeartbeatEffectiveIntervalResponse {
    return heartbeatEffectiveIntervalResponseFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func heartbeatEffectiveIntervalResponseFromObject(object: CjJsonObject): HeartbeatEffectiveIntervalResponse {
    let connected = jsonBoolField(object, "connected", false)
    let intervalMs: ?UInt64 = jsonOptionalUInt64Field(object, "intervalMs")
    let intervalSecs: ?UInt64 = jsonOptionalUInt64Field(object, "intervalSecs")
    return HeartbeatEffectiveIntervalResponse(
        connected: connected,
        intervalMs: intervalMs,
        intervalSecs: intervalSecs
    )
}

// A projected name: conversation `title` and `lastSender`, message `sender`, participant `name`. A value that is
// there never fails, by the rule the kits read names with: an unknown `entity` reads as `person`, an unknown `kind`, a
// `named` value without text or a value that is not an object reads as an unnamed person without an id tail, an
// unknown `source` is dropped. This decoder reads every field it does not find as that field's default, and an absent
// `title`, `sender` or `name` as an unnamed person without an id tail.
func unknownPersonName(): DisplayName {
    return DisplayName(
        kind: DisplayNameKind.Unnamed,
        text: None,
        source: None,
        names: None,
        total: None,
        entity: Some(NameEntity.Person),
        idTail: Some("")
    )
}

func jsonStringOrNone(object: CjJsonObject, key: String): ?String {
    match (object.get(key)) {
        case Some(value) =>
            try {
                return Some(value.asString().getValue())
            } catch (_) {
                return None
            }
        case None => return None
    }
}

func nameSourceFromWire(raw: String): ?NameSource {
    if (raw == "alias") { return Some(NameSource.Alias) }
    if (raw == "memberNickname") { return Some(NameSource.MemberNickname) }
    if (raw == "profile") { return Some(NameSource.Profile) }
    if (raw == "snapshot") { return Some(NameSource.Snapshot) }
    if (raw == "remark") { return Some(NameSource.Remark) }
    if (raw == "name") { return Some(NameSource.Name) }
    return None
}

func nameSourceWireValue(value: NameSource): String {
    return match (value) {
        case NameSource.Alias => "alias"
        case NameSource.MemberNickname => "memberNickname"
        case NameSource.Profile => "profile"
        case NameSource.Snapshot => "snapshot"
        case NameSource.Remark => "remark"
        case NameSource.Name => "name"
    }
}

func nameEntityFromWire(raw: String): NameEntity {
    if (raw == "group") { return NameEntity.Group }
    if (raw == "system") { return NameEntity.System }
    if (raw == "ai") { return NameEntity.Ai }
    if (raw == "customer") { return NameEntity.Customer }
    if (raw == "channel") { return NameEntity.Channel }
    if (raw == "broadcast") { return NameEntity.Broadcast }
    if (raw == "temp") { return NameEntity.Temp }
    return NameEntity.Person
}

func nameEntityWireValue(value: NameEntity): String {
    return match (value) {
        case NameEntity.Person => "person"
        case NameEntity.Group => "group"
        case NameEntity.System => "system"
        case NameEntity.Ai => "ai"
        case NameEntity.Customer => "customer"
        case NameEntity.Channel => "channel"
        case NameEntity.Broadcast => "broadcast"
        case NameEntity.Temp => "temp"
    }
}

func displayNameKindWireValue(value: DisplayNameKind): String {
    return match (value) {
        case DisplayNameKind.Named => "named"
        case DisplayNameKind.Members => "members"
        case DisplayNameKind.Unnamed => "unnamed"
    }
}

func displayNameTotalFromValue(value: CjJsonValue): UInt32 {
    try {
        let total = value.asInt().getValue()
        if (total < 0) {
            return 0
        }
        if (total > 4294967295) {
            return UInt32(4294967295)
        }
        return UInt32(total)
    } catch (_) {
        return 0
    }
}

func jsonObjectOrNone(value: CjJsonValue): ?CjJsonObject {
    try {
        return Some(value.asObject())
    } catch (_) {
        return None
    }
}

public func displayNameFromValue(value: CjJsonValue): DisplayName {
    let object = match (jsonObjectOrNone(value)) {
        case Some(found) => found
        case None => return unknownPersonName()
    }
    let kind = match (jsonStringOrNone(object, "kind")) {
        case Some(found) => found
        case None => ""
    }
    if (kind == "named") {
        return match (jsonStringOrNone(object, "text")) {
            case Some(text) =>
                let source: ?NameSource = match (jsonStringOrNone(object, "source")) {
                    case Some(raw) => nameSourceFromWire(raw)
                    case None => None
                }
                DisplayName(
                    kind: DisplayNameKind.Named,
                    text: Some(text),
                    source: source,
                    names: None,
                    total: None,
                    entity: None,
                    idTail: None
                )
            case None => unknownPersonName()
        }
    }
    if (kind == "members") {
        let names = match (object.get("names")) {
            case Some(found) => found.toJsonString()
            case None => "[]"
        }
        let total: UInt32 = match (object.get("total")) {
            case Some(found) => displayNameTotalFromValue(found)
            case None => 0
        }
        return DisplayName(
            kind: DisplayNameKind.Members,
            text: None,
            source: None,
            names: Some(names),
            total: Some(total),
            entity: None,
            idTail: None
        )
    }
    if (kind == "unnamed") {
        let entity = match (jsonStringOrNone(object, "entity")) {
            case Some(raw) => nameEntityFromWire(raw)
            case None => NameEntity.Person
        }
        let idTail = match (jsonStringOrNone(object, "idTail")) {
            case Some(found) => found
            case None => ""
        }
        return DisplayName(
            kind: DisplayNameKind.Unnamed,
            text: None,
            source: None,
            names: None,
            total: None,
            entity: Some(entity),
            idTail: Some(idTail)
        )
    }
    return unknownPersonName()
}

public func displayNameField(object: CjJsonObject, key: String): DisplayName {
    return match (object.get(key)) {
        case Some(value) => displayNameFromValue(value)
        case None => unknownPersonName()
    }
}

public func optionalDisplayNameField(object: CjJsonObject, key: String): ?DisplayName {
    return match (object.get(key)) {
        case Some(value) => Some(displayNameFromValue(value))
        case None => None
    }
}

public func displayNameToJson(request: DisplayName): String {
    let json = CjJsonObject()
    jsonPutString(json, "kind", displayNameKindWireValue(request.kind))
    match (request.text) {
        case Some(value) => jsonPutString(json, "text", value)
        case None => ()
    }
    match (request.source) {
        case Some(value) => jsonPutString(json, "source", nameSourceWireValue(value))
        case None => ()
    }
    match (request.names) {
        case Some(value) => jsonPutRaw(json, "names", value)
        case None => ()
    }
    match (request.total) {
        case Some(value) => jsonPutInt(json, "total", Int64(value))
        case None => ()
    }
    match (request.entity) {
        case Some(value) => jsonPutString(json, "entity", nameEntityWireValue(value))
        case None => ()
    }
    match (request.idTail) {
        case Some(value) => jsonPutString(json, "idTail", value)
        case None => ()
    }
    return json.toJsonString()
}

public func conversationParticipantToJson(request: ConversationParticipant): String {
    let json = CjJsonObject()
    jsonPutString(json, "userId", request.userId)
    jsonPutRaw(json, "roles", request.roles)
    jsonPutBool(json, "muted", request.muted)
    jsonPutBool(json, "pinned", request.pinned)
    jsonPutRaw(json, "attributes", request.attributes)
    jsonPutInt(json, "joinedAt", Int64(request.joinedAt))
    jsonPutString(json, "nickname", request.nickname)
    jsonPutRaw(json, "name", displayNameToJson(request.name))
    return json.toJsonString()
}

public func conversationParticipantFromJson(raw: String): ConversationParticipant {
    return conversationParticipantFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func conversationParticipantFromObject(object: CjJsonObject): ConversationParticipant {
    let userId = jsonStringField(object, "userId", "")
    let roles = jsonRawField(object, "roles", "[]")
    let muted = jsonBoolField(object, "muted", false)
    let pinned = jsonBoolField(object, "pinned", false)
    let attributes = jsonRawField(object, "attributes", "{}")
    let joinedAt = UInt64(jsonIntField(object, "joinedAt", 0))
    let nickname = jsonStringField(object, "nickname", "")
    let name = displayNameField(object, "name")
    return ConversationParticipant(
        userId: userId,
        roles: roles,
        muted: muted,
        pinned: pinned,
        attributes: attributes,
        joinedAt: joinedAt,
        nickname: nickname,
        name: name
    )
}

public func messagePreviewToJson(request: MessagePreview): String {
    let json = CjJsonObject()
    jsonPutString(json, "messageId", request.messageId)
    jsonPutString(json, "senderId", request.senderId)
    jsonPutInt(json, "type", Int64(request.type))
    jsonPutString(json, "text", request.text)
    jsonPutInt(json, "time", Int64(request.time))
    return json.toJsonString()
}

public func messagePreviewFromJson(raw: String): MessagePreview {
    return messagePreviewFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messagePreviewFromObject(object: CjJsonObject): MessagePreview {
    let messageId = jsonStringField(object, "messageId", "")
    let senderId = jsonStringField(object, "senderId", "")
    let type = Int32(jsonIntField(object, "type", 0))
    let text = jsonStringField(object, "text", "")
    let time = UInt64(jsonIntField(object, "time", 0))
    return MessagePreview(
        messageId: messageId,
        senderId: senderId,
        type: type,
        text: text,
        time: time
    )
}

public func conversationToJson(request: Conversation): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "conversationType", conversationTypeWireValue(request.conversationType))
    jsonPutString(json, "businessType", request.businessType)
    jsonPutString(json, "channelId", request.channelId)
    jsonPutInt(json, "membersCount", Int64(request.membersCount))
    jsonPutString(json, "displayName", request.displayName)
    jsonPutRaw(json, "title", displayNameToJson(request.title))
    jsonPutString(json, "avatarUrl", request.avatarUrl)
    match (request.remark) {
        case Some(value) => jsonPutString(json, "remark", value)
        case None => ()
    }
    match (request.description) {
        case Some(value) => jsonPutString(json, "description", value)
        case None => ()
    }
    match (request.lastMessageId) {
        case Some(value) => jsonPutString(json, "lastMessageId", value)
        case None => ()
    }
    match (request.lastSenderId) {
        case Some(value) => jsonPutString(json, "lastSenderId", value)
        case None => ()
    }
    match (request.lastMessageAt) {
        case Some(value) => jsonPutInt(json, "lastMessageAt", Int64(value))
        case None => ()
    }
    match (request.lastMessagePreview) {
        case Some(value) => jsonPutString(json, "lastMessagePreview", value)
        case None => ()
    }
    match (request.lastMessage) {
        case Some(value) => jsonPutRaw(json, "lastMessage", messagePreviewToJson(value))
        case None => ()
    }
    jsonPutString(json, "lastSenderNickname", request.lastSenderNickname)
    match (request.lastSender) {
        case Some(value) => jsonPutRaw(json, "lastSender", displayNameToJson(value))
        case None => ()
    }
    jsonPutString(json, "lastSenderAvatarUrl", request.lastSenderAvatarUrl)
    jsonPutInt(json, "unreadCount", Int64(request.unreadCount))
    jsonPutInt(json, "lastReadSeq", Int64(request.lastReadSeq))
    jsonPutInt(json, "peerReadSeq", Int64(request.peerReadSeq))
    jsonPutInt(json, "maxSeq", Int64(request.maxSeq))
    jsonPutInt(json, "visibleAfterSeq", Int64(request.visibleAfterSeq))
    jsonPutBool(json, "isPinned", request.isPinned)
    jsonPutBool(json, "isMuted", request.isMuted)
    jsonPutBool(json, "isArchived", request.isArchived)
    jsonPutInt(json, "version", Int64(request.version))
    jsonPutInt(json, "updatedAt", Int64(request.updatedAt))
    jsonPutInt(json, "createdAt", Int64(request.createdAt))
    match (request.updatedAtTs) {
        case Some(value) => jsonPutInt(json, "updatedAtTs", Int64(value))
        case None => ()
    }
    jsonPutRaw(json, "ext", request.ext)
    jsonPutInt(json, "participantVersion", Int64(request.participantVersion))
    jsonPutRaw(json, "memberPreview", request.memberPreview)
    match (request.draft) {
        case Some(value) => jsonPutString(json, "draft", value)
        case None => ()
    }
    jsonPutInt(json, "mentionCount", Int64(request.mentionCount))
    jsonPutBool(json, "mentionMe", request.mentionMe)
    match (request.badge) {
        case Some(value) => jsonPutString(json, "badge", value)
        case None => ()
    }
    match (request.role) {
        case Some(value) => jsonPutString(json, "role", value)
        case None => ()
    }
    jsonPutRaw(json, "participants", request.participants)
    return json.toJsonString()
}

public func conversationFromJson(raw: String): Conversation {
    return conversationFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func conversationFromObject(object: CjJsonObject): Conversation {
    let conversationId = jsonStringField(object, "conversationId", "")
    let conversationType = jsonConversationTypeField(object, "conversationType", ConversationType.Unspecified)
    let businessType = jsonStringField(object, "businessType", "")
    let channelId = jsonStringField(object, "channelId", "")
    let membersCount = UInt32(jsonIntField(object, "membersCount", 0))
    let displayName = jsonStringField(object, "displayName", "")
    let title = displayNameField(object, "title")
    let avatarUrl = jsonStringField(object, "avatarUrl", "")
    let remark: ?String = jsonOptionalStringField(object, "remark")
    let description: ?String = jsonOptionalStringField(object, "description")
    let lastMessageId: ?String = jsonOptionalStringField(object, "lastMessageId")
    let lastSenderId: ?String = jsonOptionalStringField(object, "lastSenderId")
    let lastMessageAt: ?UInt64 = jsonOptionalUInt64Field(object, "lastMessageAt")
    let lastMessagePreview: ?String = jsonOptionalStringField(object, "lastMessagePreview")
    let lastMessage: ?MessagePreview = match (object.get("lastMessage")) {
            case Some(value) => Some(messagePreviewFromObject(value.asObject()))
            case None => None
        }
    let lastSenderNickname = jsonStringField(object, "lastSenderNickname", "")
    let lastSender: ?DisplayName = optionalDisplayNameField(object, "lastSender")
    let lastSenderAvatarUrl = jsonStringField(object, "lastSenderAvatarUrl", "")
    let unreadCount = UInt32(jsonIntField(object, "unreadCount", 0))
    let lastReadSeq = UInt64(jsonIntField(object, "lastReadSeq", 0))
    let peerReadSeq = UInt64(jsonIntField(object, "peerReadSeq", 0))
    let maxSeq = UInt64(jsonIntField(object, "maxSeq", 0))
    let visibleAfterSeq = UInt64(jsonIntField(object, "visibleAfterSeq", 0))
    let isPinned = jsonBoolField(object, "isPinned", false)
    let isMuted = jsonBoolField(object, "isMuted", false)
    let isArchived = jsonBoolField(object, "isArchived", false)
    let version = UInt64(jsonIntField(object, "version", 0))
    let updatedAt = UInt64(jsonIntField(object, "updatedAt", 0))
    let createdAt = UInt64(jsonIntField(object, "createdAt", 0))
    let updatedAtTs: ?UInt64 = jsonOptionalUInt64Field(object, "updatedAtTs")
    let ext = jsonRawField(object, "ext", "{}")
    let participantVersion = UInt64(jsonIntField(object, "participantVersion", 0))
    let memberPreview = jsonRawField(object, "memberPreview", "[]")
    let draft: ?String = jsonOptionalStringField(object, "draft")
    let mentionCount = UInt32(jsonIntField(object, "mentionCount", 0))
    let mentionMe = jsonBoolField(object, "mentionMe", false)
    let badge: ?String = jsonOptionalStringField(object, "badge")
    let role: ?String = jsonOptionalStringField(object, "role")
    let participants = jsonRawField(object, "participants", "[]")
    return Conversation(
        conversationId: conversationId,
        conversationType: conversationType,
        businessType: businessType,
        channelId: channelId,
        membersCount: membersCount,
        displayName: displayName,
        title: title,
        avatarUrl: avatarUrl,
        remark: remark,
        description: description,
        lastMessageId: lastMessageId,
        lastSenderId: lastSenderId,
        lastMessageAt: lastMessageAt,
        lastMessagePreview: lastMessagePreview,
        lastMessage: lastMessage,
        lastSenderNickname: lastSenderNickname,
        lastSender: lastSender,
        lastSenderAvatarUrl: lastSenderAvatarUrl,
        unreadCount: unreadCount,
        lastReadSeq: lastReadSeq,
        peerReadSeq: peerReadSeq,
        maxSeq: maxSeq,
        visibleAfterSeq: visibleAfterSeq,
        isPinned: isPinned,
        isMuted: isMuted,
        isArchived: isArchived,
        version: version,
        updatedAt: updatedAt,
        createdAt: createdAt,
        updatedAtTs: updatedAtTs,
        ext: ext,
        participantVersion: participantVersion,
        memberPreview: memberPreview,
        draft: draft,
        mentionCount: mentionCount,
        mentionMe: mentionMe,
        badge: badge,
        role: role,
        participants: participants
    )
}

public func conversationListQueryToJson(request: ConversationListQuery): String {
    let json = CjJsonObject()
    match (request.keyword) {
        case Some(value) => jsonPutString(json, "keyword", value)
        case None => ()
    }
    jsonPutBool(json, "includeArchived", request.includeArchived)
    jsonPutBool(json, "unreadOnly", request.unreadOnly)
    jsonPutBool(json, "mentionMeOnly", request.mentionMeOnly)
    jsonPutBool(json, "pinnedOnly", request.pinnedOnly)
    match (request.mutedOnly) {
        case Some(value) => jsonPutBool(json, "mutedOnly", value)
        case None => ()
    }
    jsonPutBool(json, "hasDraftOnly", request.hasDraftOnly)
    jsonPutBool(json, "hasMarkedMessages", request.hasMarkedMessages)
    jsonPutRaw(json, "conversationTypes", request.conversationTypes)
    match (request.cursor) {
        case Some(value) => jsonPutString(json, "cursor", value)
        case None => ()
    }
    match (request.limit) {
        case Some(value) => jsonPutInt(json, "limit", Int64(value))
        case None => ()
    }
    return json.toJsonString()
}

public func conversationListQueryFromJson(raw: String): ConversationListQuery {
    return conversationListQueryFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func conversationListQueryFromObject(object: CjJsonObject): ConversationListQuery {
    let keyword: ?String = jsonOptionalStringField(object, "keyword")
    let includeArchived = jsonBoolField(object, "includeArchived", false)
    let unreadOnly = jsonBoolField(object, "unreadOnly", false)
    let mentionMeOnly = jsonBoolField(object, "mentionMeOnly", false)
    let pinnedOnly = jsonBoolField(object, "pinnedOnly", false)
    let mutedOnly: ?Bool = jsonOptionalBoolField(object, "mutedOnly")
    let hasDraftOnly = jsonBoolField(object, "hasDraftOnly", false)
    let hasMarkedMessages = jsonBoolField(object, "hasMarkedMessages", false)
    let conversationTypes = jsonRawField(object, "conversationTypes", "[]")
    let cursor: ?String = jsonOptionalStringField(object, "cursor")
    let limit: ?UInt32 = jsonOptionalUInt32Field(object, "limit")
    return ConversationListQuery(
        keyword: keyword,
        includeArchived: includeArchived,
        unreadOnly: unreadOnly,
        mentionMeOnly: mentionMeOnly,
        pinnedOnly: pinnedOnly,
        mutedOnly: mutedOnly,
        hasDraftOnly: hasDraftOnly,
        hasMarkedMessages: hasMarkedMessages,
        conversationTypes: conversationTypes,
        cursor: cursor,
        limit: limit
    )
}

public func listConversationsResponseToJson(request: ListConversationsResponse): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "conversations", request.conversations)
    return json.toJsonString()
}

public func listConversationsResponseFromJson(raw: String): ListConversationsResponse {
    return listConversationsResponseFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func listConversationsResponseFromObject(object: CjJsonObject): ListConversationsResponse {
    let conversations = jsonRawField(object, "conversations", "[]")
    return ListConversationsResponse(
        conversations: conversations
    )
}

public func bootstrapHomeTimelineRequestToJson(request: BootstrapHomeTimelineRequest): String {
    let json = CjJsonObject()
    jsonPutInt(json, "conversationLimit", Int64(request.conversationLimit))
    return json.toJsonString()
}

public func bootstrapHomeTimelineRequestFromJson(raw: String): BootstrapHomeTimelineRequest {
    return bootstrapHomeTimelineRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func bootstrapHomeTimelineRequestFromObject(object: CjJsonObject): BootstrapHomeTimelineRequest {
    let conversationLimit = UInt32(jsonIntField(object, "conversationLimit", 0))
    return BootstrapHomeTimelineRequest(
        conversationLimit: conversationLimit
    )
}

public func openConversationTimelineRequestToJson(request: OpenConversationTimelineRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutInt(json, "messageLimit", Int64(request.messageLimit))
    return json.toJsonString()
}

public func openConversationTimelineRequestFromJson(raw: String): OpenConversationTimelineRequest {
    return openConversationTimelineRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func openConversationTimelineRequestFromObject(object: CjJsonObject): OpenConversationTimelineRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let messageLimit = UInt32(jsonIntField(object, "messageLimit", 0))
    return OpenConversationTimelineRequest(
        conversationId: conversationId,
        messageLimit: messageLimit
    )
}

public func homeTimelineSnapshotToJson(request: HomeTimelineSnapshot): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "conversations", request.conversations)
    jsonPutInt(json, "totalUnread", Int64(request.totalUnread))
    jsonPutString(json, "syncState", timelineSyncStateWireValue(request.syncState))
    return json.toJsonString()
}

public func homeTimelineSnapshotFromJson(raw: String): HomeTimelineSnapshot {
    return homeTimelineSnapshotFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func homeTimelineSnapshotFromObject(object: CjJsonObject): HomeTimelineSnapshot {
    let conversations = jsonRawField(object, "conversations", "[]")
    let totalUnread = UInt64(jsonIntField(object, "totalUnread", 0))
    let syncState = jsonTimelineSyncStateField(object, "syncState", TimelineSyncState.LocalReady)
    return HomeTimelineSnapshot(
        conversations: conversations,
        totalUnread: totalUnread,
        syncState: syncState
    )
}

public func conversationTimelineSnapshotToJson(request: ConversationTimelineSnapshot): String {
    let json = CjJsonObject()
    match (request.conversation) {
        case Some(value) => jsonPutRaw(json, "conversation", conversationToJson(value))
        case None => ()
    }
    jsonPutRaw(json, "messages", request.messages)
    jsonPutBool(json, "hasMore", request.hasMore)
    return json.toJsonString()
}

public func conversationTimelineSnapshotFromJson(raw: String): ConversationTimelineSnapshot {
    return conversationTimelineSnapshotFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func conversationTimelineSnapshotFromObject(object: CjJsonObject): ConversationTimelineSnapshot {
    let conversation: ?Conversation = match (object.get("conversation")) {
            case Some(value) => Some(conversationFromObject(value.asObject()))
            case None => None
        }
    let messages = jsonRawField(object, "messages", "[]")
    let hasMore = jsonBoolField(object, "hasMore", false)
    return ConversationTimelineSnapshot(
        conversation: conversation,
        messages: messages,
        hasMore: hasMore
    )
}

public func conversationVersionToJson(request: ConversationVersion): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutInt(json, "version", Int64(request.version))
    return json.toJsonString()
}

public func conversationVersionFromJson(raw: String): ConversationVersion {
    return conversationVersionFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func conversationVersionFromObject(object: CjJsonObject): ConversationVersion {
    let conversationId = jsonStringField(object, "conversationId", "")
    let version = UInt64(jsonIntField(object, "version", 0))
    return ConversationVersion(
        conversationId: conversationId,
        version: version
    )
}

public func syncConversationSummariesRequestToJson(request: SyncConversationSummariesRequest): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "knownVersions", request.knownVersions)
    return json.toJsonString()
}

public func syncConversationSummariesRequestFromJson(raw: String): SyncConversationSummariesRequest {
    return syncConversationSummariesRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func syncConversationSummariesRequestFromObject(object: CjJsonObject): SyncConversationSummariesRequest {
    let knownVersions = jsonRawField(object, "knownVersions", "[]")
    return SyncConversationSummariesRequest(
        knownVersions: knownVersions
    )
}

public func syncConversationSummariesResponseToJson(request: SyncConversationSummariesResponse): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "changedConversations", request.changedConversations)
    return json.toJsonString()
}

public func syncConversationSummariesResponseFromJson(raw: String): SyncConversationSummariesResponse {
    return syncConversationSummariesResponseFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func syncConversationSummariesResponseFromObject(object: CjJsonObject): SyncConversationSummariesResponse {
    let changedConversations = jsonRawField(object, "changedConversations", "[]")
    return SyncConversationSummariesResponse(
        changedConversations: changedConversations
    )
}

public func reactionEntryToJson(request: ReactionEntry): String {
    let json = CjJsonObject()
    jsonPutString(json, "emoji", request.emoji)
    jsonPutRaw(json, "userIds", request.userIds)
    jsonPutInt(json, "count", Int64(request.count))
    return json.toJsonString()
}

public func reactionEntryFromJson(raw: String): ReactionEntry {
    return reactionEntryFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func reactionEntryFromObject(object: CjJsonObject): ReactionEntry {
    let emoji = jsonStringField(object, "emoji", "")
    let userIds = jsonRawField(object, "userIds", "[]")
    let count = UInt32(jsonIntField(object, "count", 0))
    return ReactionEntry(
        emoji: emoji,
        userIds: userIds,
        count: count
    )
}

public func messageLocalStateToJson(request: MessageLocalState): String {
    let json = CjJsonObject()
    jsonPutBool(json, "sending", request.sending)
    jsonPutBool(json, "failed", request.failed)
    jsonPutBool(json, "isLocal", request.isLocal)
    jsonPutInt(json, "sortTs", Int64(request.sortTs))
    return json.toJsonString()
}

public func messageLocalStateFromJson(raw: String): MessageLocalState {
    return messageLocalStateFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

// The core's receipt projection; None when absent or malformed (an older core).
public func messageReceiptViewFromValue(value: CjJsonValue): ?MessageReceiptView {
    try {
        let object = value.asObject()
        let raw = jsonStringField(object, "status", "")
        var status: ?MessageReceiptStatus = None
        if (raw == "pending") { status = Some(MessageReceiptStatus.Pending) }
        if (raw == "sending") { status = Some(MessageReceiptStatus.Sending) }
        if (raw == "sent") { status = Some(MessageReceiptStatus.Sent) }
        if (raw == "read") { status = Some(MessageReceiptStatus.Read) }
        if (raw == "failed") { status = Some(MessageReceiptStatus.Failed) }
        let readSummary: ?MessageReadSummary = match (object.get("readSummary")) {
            case Some(summary) => messageReadSummaryFromValue(summary)
            case None => None
        }
        return match (status) {
            case Some(found) => Some(MessageReceiptView(status: found, readSummary: readSummary))
            case None => None
        }
    } catch (_) {
        return None
    }
}

func messageReadSummaryFromValue(value: CjJsonValue): ?MessageReadSummary {
    try {
        let object = value.asObject()
        let readCount = jsonIntField(object, "readCount", -1)
        let unreadCount = jsonIntField(object, "unreadCount", -1)
        let total = jsonIntField(object, "total", -1)
        if (readCount < 0 || unreadCount < 0 || total <= 0 || total > 4294967295
            || readCount > total || unreadCount > total) {
            return None
        }
        return Some(MessageReadSummary(
            readCount: UInt32(readCount),
            unreadCount: UInt32(unreadCount),
            total: UInt32(total)
        ))
    } catch (_) {
        return None
    }
}

public func messageLocalStateFromObject(object: CjJsonObject): MessageLocalState {
    let sending = jsonBoolField(object, "sending", false)
    let failed = jsonBoolField(object, "failed", false)
    let isLocal = jsonBoolField(object, "isLocal", false)
    let sortTs = UInt64(jsonIntField(object, "sortTs", 0))
    return MessageLocalState(
        sending: sending,
        failed: failed,
        isLocal: isLocal,
        sortTs: sortTs
    )
}

public func messageContentToJson(request: MessageContent): String {
    let json = CjJsonObject()
    jsonPutString(json, "contentType", messageContentTypeWireValue(request.contentType))
    jsonPutRaw(json, "data", request.data)
    return json.toJsonString()
}

public func messageContentFromJson(raw: String): MessageContent {
    return messageContentFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messageContentFromObject(object: CjJsonObject): MessageContent {
    let contentType = jsonMessageContentTypeField(object, "contentType", MessageContentType.Custom)
    let data = jsonRawField(object, "data", "{}")
    return MessageContent(
        contentType: contentType,
        data: data
    )
}

public func messageToJson(request: Message): String {
    let json = CjJsonObject()
    jsonPutString(json, "serverId", request.serverId)
    jsonPutString(json, "clientMsgId", request.clientMsgId)
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutInt(json, "conversationType", Int64(request.conversationType))
    jsonPutString(json, "channelId", request.channelId)
    jsonPutString(json, "senderId", request.senderId)
    jsonPutInt(json, "source", Int64(request.source))
    jsonPutInt(json, "conversationSeq", Int64(request.conversationSeq))
    jsonPutInt(json, "createdAt", Int64(request.createdAt))
    jsonPutInt(json, "clientCreatedAt", Int64(request.clientCreatedAt))
    jsonPutInt(json, "messageType", Int64(request.messageType))
    match (request.content) {
        case Some(value) => jsonPutRaw(json, "content", messageContentToJson(value))
        case None => ()
    }
    jsonPutString(json, "senderName", request.senderName)
    jsonPutString(json, "senderAvatar", request.senderAvatar)
    jsonPutString(json, "senderDisplayName", request.senderDisplayName)
    match (request.replyTo) {
        case Some(value) => jsonPutString(json, "replyTo", value)
        case None => ()
    }
    match (request.quotePreview) {
        case Some(value) => jsonPutString(json, "quotePreview", value)
        case None => ()
    }
    match (request.threadId) {
        case Some(value) => jsonPutString(json, "threadId", value)
        case None => ()
    }
    jsonPutInt(json, "status", Int64(request.status))
    jsonPutBool(json, "isRead", request.isRead)
    jsonPutBool(json, "isRecalled", request.isRecalled)
    jsonPutBool(json, "isEdited", request.isEdited)
    jsonPutRaw(json, "mentionUsers", request.mentionUsers)
    jsonPutBool(json, "mentionAll", request.mentionAll)
    jsonPutRaw(json, "attributes", request.attributes)
    jsonPutRaw(json, "extensions", request.extensions)
    jsonPutRaw(json, "reactions", request.reactions)
    jsonPutString(json, "textPreview", request.textPreview)
    jsonPutInt(json, "version", Int64(request.version))
    jsonPutInt(json, "updatedAt", Int64(request.updatedAt))
    match (request.localState) {
        case Some(value) => jsonPutRaw(json, "localState", messageLocalStateToJson(value))
        case None => ()
    }
    jsonPutString(json, "timelineKey", request.timelineKey)
    jsonPutInt(json, "timelineSortTs", Int64(request.timelineSortTs))
    return json.toJsonString()
}

public func messageFromJson(raw: String): Message {
    return messageFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messageFromObject(object: CjJsonObject): Message {
    let serverId = jsonStringField(object, "serverId", "")
    let clientMsgId = jsonStringField(object, "clientMsgId", "")
    let conversationId = jsonStringField(object, "conversationId", "")
    let conversationType = Int32(jsonIntField(object, "conversationType", 0))
    let channelId = jsonStringField(object, "channelId", "")
    let senderId = jsonStringField(object, "senderId", "")
    let source = Int32(jsonIntField(object, "source", 0))
    let conversationSeq = UInt64(jsonIntField(object, "conversationSeq", 0))
    let createdAt = UInt64(jsonIntField(object, "createdAt", 0))
    let clientCreatedAt = UInt64(jsonIntField(object, "clientCreatedAt", 0))
    let messageType = Int32(jsonIntField(object, "messageType", 0))
    let content: ?MessageContent = match (object.get("content")) {
            case Some(value) => Some(messageContentFromObject(value.asObject()))
            case None => None
        }
    let senderName = jsonStringField(object, "senderName", "")
    let senderAvatar = jsonStringField(object, "senderAvatar", "")
    let senderDisplayName = jsonStringField(object, "senderDisplayName", "")
    let sender = displayNameField(object, "sender")
    let replyTo: ?String = jsonOptionalStringField(object, "replyTo")
    let quotePreview: ?String = jsonOptionalStringField(object, "quotePreview")
    let threadId: ?String = jsonOptionalStringField(object, "threadId")
    let status = Int32(jsonIntField(object, "status", 0))
    let isRead = jsonBoolField(object, "isRead", false)
    let isRecalled = jsonBoolField(object, "isRecalled", false)
    let isEdited = jsonBoolField(object, "isEdited", false)
    let mentionUsers = jsonRawField(object, "mentionUsers", "[]")
    let mentionAll = jsonBoolField(object, "mentionAll", false)
    let attributes = jsonRawField(object, "attributes", "{}")
    let extensions = jsonRawField(object, "extensions", "{}")
    let reactions = jsonRawField(object, "reactions", "[]")
    let textPreview = jsonStringField(object, "textPreview", "")
    let version = UInt64(jsonIntField(object, "version", 0))
    let updatedAt = UInt64(jsonIntField(object, "updatedAt", 0))
    let localState: ?MessageLocalState = match (object.get("localState")) {
            case Some(value) => Some(messageLocalStateFromObject(value.asObject()))
            case None => None
        }
    let timelineKey = jsonStringField(object, "timelineKey", "")
    let timelineSortTs = UInt64(jsonIntField(object, "timelineSortTs", 0))
    let receipt: ?MessageReceiptView = match (object.get("receipt")) {
            case Some(value) => messageReceiptViewFromValue(value)
            case None => None
        }
    return Message(
        serverId: serverId,
        clientMsgId: clientMsgId,
        conversationId: conversationId,
        conversationType: conversationType,
        channelId: channelId,
        senderId: senderId,
        source: source,
        conversationSeq: conversationSeq,
        createdAt: createdAt,
        clientCreatedAt: clientCreatedAt,
        messageType: messageType,
        content: content,
        senderName: senderName,
        senderAvatar: senderAvatar,
        senderDisplayName: senderDisplayName,
        sender: sender,
        replyTo: replyTo,
        quotePreview: quotePreview,
        threadId: threadId,
        status: status,
        isRead: isRead,
        isRecalled: isRecalled,
        isEdited: isEdited,
        mentionUsers: mentionUsers,
        mentionAll: mentionAll,
        attributes: attributes,
        extensions: extensions,
        reactions: reactions,
        textPreview: textPreview,
        version: version,
        updatedAt: updatedAt,
        localState: localState,
        timelineKey: timelineKey,
        timelineSortTs: timelineSortTs,
        receipt: receipt
    )
}

public func createTextMessageRequestToJson(request: CreateTextMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "text", request.text)
    return json.toJsonString()
}

public func createTextMessageRequestFromJson(raw: String): CreateTextMessageRequest {
    return createTextMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func createTextMessageRequestFromObject(object: CjJsonObject): CreateTextMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let text = jsonStringField(object, "text", "")
    return CreateTextMessageRequest(
        conversationId: conversationId,
        text: text
    )
}

public func sendMessageRequestToJson(request: SendMessageRequest): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "message", messageToJson(request.message))
    match (request.expectedUserId) {
        case Some(value) => jsonPutString(json, "expectedUserId", value)
        case None => ()
    }
    return json.toJsonString()
}

public func sendMessageRequestFromJson(raw: String): SendMessageRequest {
    return sendMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func sendMessageRequestFromObject(object: CjJsonObject): SendMessageRequest {
    let message = match (object.get("message")) {
            case Some(value) => messageFromObject(value.asObject())
            case None => messageFromJson("{}")
        }
    return SendMessageRequest(
        message: message
    )
}

public func sendMessageResponseToJson(request: SendMessageResponse): String {
    let json = CjJsonObject()
    jsonPutString(json, "serverId", request.serverId)
    jsonPutString(json, "clientMsgId", request.clientMsgId)
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutInt(json, "seq", Int64(request.seq))
    jsonPutInt(json, "timestamp", Int64(request.timestamp))
    return json.toJsonString()
}

public func sendMessageResponseFromJson(raw: String): SendMessageResponse {
    return sendMessageResponseFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func sendMessageResponseFromObject(object: CjJsonObject): SendMessageResponse {
    let serverId = jsonStringField(object, "serverId", "")
    let clientMsgId = jsonStringField(object, "clientMsgId", "")
    let conversationId = jsonStringField(object, "conversationId", "")
    let seq = UInt64(jsonIntField(object, "seq", 0))
    let timestamp = UInt64(jsonIntField(object, "timestamp", 0))
    return SendMessageResponse(
        serverId: serverId,
        clientMsgId: clientMsgId,
        conversationId: conversationId,
        seq: seq,
        timestamp: timestamp
    )
}

public func listMessagesRequestToJson(request: ListMessagesRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutInt(json, "beforeSeq", Int64(request.beforeSeq))
    jsonPutInt(json, "limit", Int64(request.limit))
    return json.toJsonString()
}

public func listMessagesRequestFromJson(raw: String): ListMessagesRequest {
    return listMessagesRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func listMessagesRequestFromObject(object: CjJsonObject): ListMessagesRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let beforeSeq = UInt64(jsonIntField(object, "beforeSeq", 0))
    let limit = UInt32(jsonIntField(object, "limit", 0))
    return ListMessagesRequest(
        conversationId: conversationId,
        beforeSeq: beforeSeq,
        limit: limit
    )
}

public func listMessagesResponseToJson(request: ListMessagesResponse): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "messages", request.messages)
    return json.toJsonString()
}

public func listMessagesResponseFromJson(raw: String): ListMessagesResponse {
    return listMessagesResponseFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func listMessagesResponseFromObject(object: CjJsonObject): ListMessagesResponse {
    let messages = jsonRawField(object, "messages", "[]")
    return ListMessagesResponse(
        messages: messages
    )
}

public func messageSearchQueryToJson(request: MessageSearchQuery): String {
    let json = CjJsonObject()
    match (request.keyword) {
        case Some(value) => jsonPutString(json, "keyword", value)
        case None => ()
    }
    match (request.conversationId) {
        case Some(value) => jsonPutString(json, "conversationId", value)
        case None => ()
    }
    match (request.senderId) {
        case Some(value) => jsonPutString(json, "senderId", value)
        case None => ()
    }
    match (request.fromTime) {
        case Some(value) => jsonPutInt(json, "fromTime", Int64(value))
        case None => ()
    }
    match (request.toTime) {
        case Some(value) => jsonPutInt(json, "toTime", Int64(value))
        case None => ()
    }
    jsonPutRaw(json, "kinds", request.kinds)
    jsonPutInt(json, "limit", Int64(request.limit))
    jsonPutBool(json, "includeRecalled", request.includeRecalled)
    return json.toJsonString()
}

public func messageSearchQueryFromJson(raw: String): MessageSearchQuery {
    return messageSearchQueryFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messageSearchQueryFromObject(object: CjJsonObject): MessageSearchQuery {
    let keyword: ?String = jsonOptionalStringField(object, "keyword")
    let conversationId: ?String = jsonOptionalStringField(object, "conversationId")
    let senderId: ?String = jsonOptionalStringField(object, "senderId")
    let fromTime: ?UInt64 = jsonOptionalUInt64Field(object, "fromTime")
    let toTime: ?UInt64 = jsonOptionalUInt64Field(object, "toTime")
    let kinds = jsonRawField(object, "kinds", "[]")
    let limit = UInt32(jsonIntField(object, "limit", 0))
    let includeRecalled = jsonBoolField(object, "includeRecalled", false)
    return MessageSearchQuery(
        keyword: keyword,
        conversationId: conversationId,
        senderId: senderId,
        fromTime: fromTime,
        toTime: toTime,
        kinds: kinds,
        limit: limit,
        includeRecalled: includeRecalled
    )
}

public func mediaSourceInfoToJson(request: MediaSourceInfo): String {
    let json = CjJsonObject()
    match (request.uuid) {
        case Some(value) => jsonPutString(json, "uuid", value)
        case None => ()
    }
    match (request.imageId) {
        case Some(value) => jsonPutString(json, "imageId", value)
        case None => ()
    }
    match (request.url) {
        case Some(value) => jsonPutString(json, "url", value)
        case None => ()
    }
    match (request.mimeType) {
        case Some(value) => jsonPutString(json, "mimeType", value)
        case None => ()
    }
    match (request.size) {
        case Some(value) => jsonPutInt(json, "size", Int64(value))
        case None => ()
    }
    match (request.width) {
        case Some(value) => jsonPutInt(json, "width", Int64(value))
        case None => ()
    }
    match (request.height) {
        case Some(value) => jsonPutInt(json, "height", Int64(value))
        case None => ()
    }
    match (request.durationMs) {
        case Some(value) => jsonPutInt(json, "durationMs", Int64(value))
        case None => ()
    }
    return json.toJsonString()
}

public func mediaSourceInfoFromJson(raw: String): MediaSourceInfo {
    return mediaSourceInfoFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func mediaSourceInfoFromObject(object: CjJsonObject): MediaSourceInfo {
    let uuid: ?String = jsonOptionalStringField(object, "uuid")
    let imageId: ?String = jsonOptionalStringField(object, "imageId")
    let url: ?String = jsonOptionalStringField(object, "url")
    let mimeType: ?String = jsonOptionalStringField(object, "mimeType")
    let size: ?UInt64 = jsonOptionalUInt64Field(object, "size")
    let width: ?Int32 = jsonOptionalInt32Field(object, "width")
    let height: ?Int32 = jsonOptionalInt32Field(object, "height")
    let durationMs: ?Int32 = jsonOptionalInt32Field(object, "durationMs")
    return MediaSourceInfo(
        uuid: uuid,
        imageId: imageId,
        url: url,
        mimeType: mimeType,
        size: size,
        width: width,
        height: height,
        durationMs: durationMs
    )
}

public func textContentPayloadToJson(request: TextContentPayload): String {
    let json = CjJsonObject()
    jsonPutString(json, "text", request.text)
    return json.toJsonString()
}

public func textContentPayloadFromJson(raw: String): TextContentPayload {
    return textContentPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func textContentPayloadFromObject(object: CjJsonObject): TextContentPayload {
    let text = jsonStringField(object, "text", "")
    return TextContentPayload(
        text: text
    )
}

public func imageContentPayloadToJson(request: ImageContentPayload): String {
    let json = CjJsonObject()
    match (request.imageId) {
        case Some(value) => jsonPutString(json, "imageId", value)
        case None => ()
    }
    match (request.source) {
        case Some(value) => jsonPutRaw(json, "source", mediaSourceInfoToJson(value))
        case None => ()
    }
    match (request.thumbnail) {
        case Some(value) => jsonPutRaw(json, "thumbnail", mediaSourceInfoToJson(value))
        case None => ()
    }
    match (request.description) {
        case Some(value) => jsonPutString(json, "description", value)
        case None => ()
    }
    return json.toJsonString()
}

public func imageContentPayloadFromJson(raw: String): ImageContentPayload {
    return imageContentPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func imageContentPayloadFromObject(object: CjJsonObject): ImageContentPayload {
    let imageId: ?String = jsonOptionalStringField(object, "imageId")
    let source: ?MediaSourceInfo = match (object.get("source")) {
            case Some(value) => Some(mediaSourceInfoFromObject(value.asObject()))
            case None => None
        }
    let thumbnail: ?MediaSourceInfo = match (object.get("thumbnail")) {
            case Some(value) => Some(mediaSourceInfoFromObject(value.asObject()))
            case None => None
        }
    let description: ?String = jsonOptionalStringField(object, "description")
    return ImageContentPayload(
        imageId: imageId,
        source: source,
        thumbnail: thumbnail,
        description: description
    )
}

public func imageGroupItemToJson(request: ImageGroupItem): String {
    let json = CjJsonObject()
    jsonPutString(json, "imageId", request.imageId)
    match (request.url) {
        case Some(value) => jsonPutString(json, "url", value)
        case None => ()
    }
    match (request.title) {
        case Some(value) => jsonPutString(json, "title", value)
        case None => ()
    }
    match (request.width) {
        case Some(value) => jsonPutInt(json, "width", Int64(value))
        case None => ()
    }
    match (request.height) {
        case Some(value) => jsonPutInt(json, "height", Int64(value))
        case None => ()
    }
    return json.toJsonString()
}

public func imageGroupItemFromJson(raw: String): ImageGroupItem {
    return imageGroupItemFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func imageGroupItemFromObject(object: CjJsonObject): ImageGroupItem {
    let imageId = jsonStringField(object, "imageId", "")
    let url: ?String = jsonOptionalStringField(object, "url")
    let title: ?String = jsonOptionalStringField(object, "title")
    let width: ?Int32 = jsonOptionalInt32Field(object, "width")
    let height: ?Int32 = jsonOptionalInt32Field(object, "height")
    return ImageGroupItem(
        imageId: imageId,
        url: url,
        title: title,
        width: width,
        height: height
    )
}

public func imageGroupContentPayloadToJson(request: ImageGroupContentPayload): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "images", request.images)
    match (request.title) {
        case Some(value) => jsonPutString(json, "title", value)
        case None => ()
    }
    return json.toJsonString()
}

public func imageGroupContentPayloadFromJson(raw: String): ImageGroupContentPayload {
    return imageGroupContentPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func imageGroupContentPayloadFromObject(object: CjJsonObject): ImageGroupContentPayload {
    let images = jsonRawField(object, "images", "[]")
    let title: ?String = jsonOptionalStringField(object, "title")
    return ImageGroupContentPayload(
        images: images,
        title: title
    )
}

public func videoContentPayloadToJson(request: VideoContentPayload): String {
    let json = CjJsonObject()
    match (request.videoId) {
        case Some(value) => jsonPutString(json, "videoId", value)
        case None => ()
    }
    match (request.source) {
        case Some(value) => jsonPutRaw(json, "source", mediaSourceInfoToJson(value))
        case None => ()
    }
    match (request.cover) {
        case Some(value) => jsonPutRaw(json, "cover", mediaSourceInfoToJson(value))
        case None => ()
    }
    match (request.description) {
        case Some(value) => jsonPutString(json, "description", value)
        case None => ()
    }
    return json.toJsonString()
}

public func videoContentPayloadFromJson(raw: String): VideoContentPayload {
    return videoContentPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func videoContentPayloadFromObject(object: CjJsonObject): VideoContentPayload {
    let videoId: ?String = jsonOptionalStringField(object, "videoId")
    let source: ?MediaSourceInfo = match (object.get("source")) {
            case Some(value) => Some(mediaSourceInfoFromObject(value.asObject()))
            case None => None
        }
    let cover: ?MediaSourceInfo = match (object.get("cover")) {
            case Some(value) => Some(mediaSourceInfoFromObject(value.asObject()))
            case None => None
        }
    let description: ?String = jsonOptionalStringField(object, "description")
    return VideoContentPayload(
        videoId: videoId,
        source: source,
        cover: cover,
        description: description
    )
}

public func audioContentPayloadToJson(request: AudioContentPayload): String {
    let json = CjJsonObject()
    match (request.audioId) {
        case Some(value) => jsonPutString(json, "audioId", value)
        case None => ()
    }
    match (request.source) {
        case Some(value) => jsonPutRaw(json, "source", mediaSourceInfoToJson(value))
        case None => ()
    }
    match (request.durationMs) {
        case Some(value) => jsonPutInt(json, "durationMs", Int64(value))
        case None => ()
    }
    return json.toJsonString()
}

public func audioContentPayloadFromJson(raw: String): AudioContentPayload {
    return audioContentPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func audioContentPayloadFromObject(object: CjJsonObject): AudioContentPayload {
    let audioId: ?String = jsonOptionalStringField(object, "audioId")
    let source: ?MediaSourceInfo = match (object.get("source")) {
            case Some(value) => Some(mediaSourceInfoFromObject(value.asObject()))
            case None => None
        }
    let durationMs: ?Int32 = jsonOptionalInt32Field(object, "durationMs")
    return AudioContentPayload(
        audioId: audioId,
        source: source,
        durationMs: durationMs
    )
}

public func fileContentPayloadToJson(request: FileContentPayload): String {
    let json = CjJsonObject()
    match (request.fileId) {
        case Some(value) => jsonPutString(json, "fileId", value)
        case None => ()
    }
    match (request.name) {
        case Some(value) => jsonPutString(json, "name", value)
        case None => ()
    }
    match (request.url) {
        case Some(value) => jsonPutString(json, "url", value)
        case None => ()
    }
    match (request.mimeType) {
        case Some(value) => jsonPutString(json, "mimeType", value)
        case None => ()
    }
    match (request.size) {
        case Some(value) => jsonPutInt(json, "size", Int64(value))
        case None => ()
    }
    return json.toJsonString()
}

public func fileContentPayloadFromJson(raw: String): FileContentPayload {
    return fileContentPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func fileContentPayloadFromObject(object: CjJsonObject): FileContentPayload {
    let fileId: ?String = jsonOptionalStringField(object, "fileId")
    let name: ?String = jsonOptionalStringField(object, "name")
    let url: ?String = jsonOptionalStringField(object, "url")
    let mimeType: ?String = jsonOptionalStringField(object, "mimeType")
    let size: ?UInt64 = jsonOptionalUInt64Field(object, "size")
    return FileContentPayload(
        fileId: fileId,
        name: name,
        url: url,
        mimeType: mimeType,
        size: size
    )
}

public func emojiContentPayloadToJson(request: EmojiContentPayload): String {
    let json = CjJsonObject()
    jsonPutString(json, "emoji", request.emoji)
    return json.toJsonString()
}

public func emojiContentPayloadFromJson(raw: String): EmojiContentPayload {
    return emojiContentPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func emojiContentPayloadFromObject(object: CjJsonObject): EmojiContentPayload {
    let emoji = jsonStringField(object, "emoji", "")
    return EmojiContentPayload(
        emoji: emoji
    )
}

public func stickerContentPayloadToJson(request: StickerContentPayload): String {
    let json = CjJsonObject()
    jsonPutString(json, "stickerId", request.stickerId)
    match (request.packageId) {
        case Some(value) => jsonPutString(json, "packageId", value)
        case None => ()
    }
    match (request.url) {
        case Some(value) => jsonPutString(json, "url", value)
        case None => ()
    }
    match (request.width) {
        case Some(value) => jsonPutInt(json, "width", Int64(value))
        case None => ()
    }
    match (request.height) {
        case Some(value) => jsonPutInt(json, "height", Int64(value))
        case None => ()
    }
    match (request.format) {
        case Some(value) => jsonPutString(json, "format", value)
        case None => ()
    }
    return json.toJsonString()
}

public func stickerContentPayloadFromJson(raw: String): StickerContentPayload {
    return stickerContentPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func stickerContentPayloadFromObject(object: CjJsonObject): StickerContentPayload {
    let stickerId = jsonStringField(object, "stickerId", "")
    let packageId: ?String = jsonOptionalStringField(object, "packageId")
    let url: ?String = jsonOptionalStringField(object, "url")
    let width: ?Int32 = jsonOptionalInt32Field(object, "width")
    let height: ?Int32 = jsonOptionalInt32Field(object, "height")
    let format: ?String = jsonOptionalStringField(object, "format")
    return StickerContentPayload(
        stickerId: stickerId,
        packageId: packageId,
        url: url,
        width: width,
        height: height,
        format: format
    )
}

public func forwardSourceMessageToJson(request: ForwardSourceMessage): String {
    let json = CjJsonObject()
    jsonPutString(json, "sourceMessageId", request.sourceMessageId)
    match (request.sourceConversationId) {
        case Some(value) => jsonPutString(json, "sourceConversationId", value)
        case None => ()
    }
    match (request.sourceSenderId) {
        case Some(value) => jsonPutString(json, "sourceSenderId", value)
        case None => ()
    }
    match (request.plainText) {
        case Some(value) => jsonPutString(json, "plainText", value)
        case None => ()
    }
    return json.toJsonString()
}

public func forwardSourceMessageFromJson(raw: String): ForwardSourceMessage {
    return forwardSourceMessageFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func forwardSourceMessageFromObject(object: CjJsonObject): ForwardSourceMessage {
    let sourceMessageId = jsonStringField(object, "sourceMessageId", "")
    let sourceConversationId: ?String = jsonOptionalStringField(object, "sourceConversationId")
    let sourceSenderId: ?String = jsonOptionalStringField(object, "sourceSenderId")
    let plainText: ?String = jsonOptionalStringField(object, "plainText")
    return ForwardSourceMessage(
        sourceMessageId: sourceMessageId,
        sourceConversationId: sourceConversationId,
        sourceSenderId: sourceSenderId,
        plainText: plainText
    )
}

public func forwardContentPayloadToJson(request: ForwardContentPayload): String {
    let json = CjJsonObject()
    match (request.merge) {
        case Some(value) => jsonPutBool(json, "merge", value)
        case None => ()
    }
    match (request.title) {
        case Some(value) => jsonPutString(json, "title", value)
        case None => ()
    }
    jsonPutRaw(json, "sourceMessages", request.sourceMessages)
    return json.toJsonString()
}

public func forwardContentPayloadFromJson(raw: String): ForwardContentPayload {
    return forwardContentPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func forwardContentPayloadFromObject(object: CjJsonObject): ForwardContentPayload {
    let merge: ?Bool = jsonOptionalBoolField(object, "merge")
    let title: ?String = jsonOptionalStringField(object, "title")
    let sourceMessages = jsonRawField(object, "sourceMessages", "[]")
    return ForwardContentPayload(
        merge: merge,
        title: title,
        sourceMessages: sourceMessages
    )
}

public func messageBuildCatalogEntryToJson(request: MessageBuildCatalogEntry): String {
    let json = CjJsonObject()
    jsonPutString(json, "op", messageBuildOpWireValue(request.op))
    jsonPutString(json, "method", request.method)
    jsonPutString(json, "requestType", request.requestType)
    jsonPutString(json, "contentType", messageContentTypeWireValue(request.contentType))
    jsonPutInt(json, "messageType", Int64(request.messageType))
    jsonPutString(json, "summary", request.summary)
    jsonPutString(json, "stability", request.stability)
    return json.toJsonString()
}

public func messageBuildCatalogEntryFromJson(raw: String): MessageBuildCatalogEntry {
    return messageBuildCatalogEntryFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messageBuildCatalogEntryFromObject(object: CjJsonObject): MessageBuildCatalogEntry {
    let op = jsonMessageBuildOpField(object, "op", MessageBuildOp.CreateText)
    let method = jsonStringField(object, "method", "")
    let requestType = jsonStringField(object, "requestType", "")
    let contentType = jsonMessageContentTypeField(object, "contentType", MessageContentType.Custom)
    let messageType = Int32(jsonIntField(object, "messageType", 0))
    let summary = jsonStringField(object, "summary", "")
    let stability = jsonStringField(object, "stability", "")
    return MessageBuildCatalogEntry(
        op: op,
        method: method,
        requestType: requestType,
        contentType: contentType,
        messageType: messageType,
        summary: summary,
        stability: stability
    )
}

public func listMessageBuildCatalogResponseToJson(request: ListMessageBuildCatalogResponse): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "entries", request.entries)
    return json.toJsonString()
}

public func listMessageBuildCatalogResponseFromJson(raw: String): ListMessageBuildCatalogResponse {
    return listMessageBuildCatalogResponseFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func listMessageBuildCatalogResponseFromObject(object: CjJsonObject): ListMessageBuildCatalogResponse {
    let entries = jsonRawField(object, "entries", "[]")
    return ListMessageBuildCatalogResponse(
        entries: entries
    )
}

public func buildTypedMessageRequestToJson(request: BuildTypedMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "op", messageBuildOpWireValue(request.op))
    match (request.data) {
        case Some(value) => jsonPutRaw(json, "data", value)
        case None => ()
    }
    return json.toJsonString()
}

public func buildTypedMessageRequestFromJson(raw: String): BuildTypedMessageRequest {
    return buildTypedMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildTypedMessageRequestFromObject(object: CjJsonObject): BuildTypedMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let op = jsonMessageBuildOpField(object, "op", MessageBuildOp.CreateText)
    let data: ?String = jsonOptionalRawField(object, "data")
    return BuildTypedMessageRequest(
        conversationId: conversationId,
        op: op,
        data: data
    )
}

public func buildTextMessageRequestToJson(request: BuildTextMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "text", request.text)
    return json.toJsonString()
}

public func buildTextMessageRequestFromJson(raw: String): BuildTextMessageRequest {
    return buildTextMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildTextMessageRequestFromObject(object: CjJsonObject): BuildTextMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let text = jsonStringField(object, "text", "")
    return BuildTextMessageRequest(
        conversationId: conversationId,
        text: text
    )
}

public func buildQuoteMessageRequestToJson(request: BuildQuoteMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "quotedMessageId", request.quotedMessageId)
    jsonPutString(json, "text", request.text)
    match (request.quotedSenderId) {
        case Some(value) => jsonPutString(json, "quotedSenderId", value)
        case None => ()
    }
    match (request.quotedTextPreview) {
        case Some(value) => jsonPutString(json, "quotedTextPreview", value)
        case None => ()
    }
    return json.toJsonString()
}

public func buildQuoteMessageRequestFromJson(raw: String): BuildQuoteMessageRequest {
    return buildQuoteMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildQuoteMessageRequestFromObject(object: CjJsonObject): BuildQuoteMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let quotedMessageId = jsonStringField(object, "quotedMessageId", "")
    let text = jsonStringField(object, "text", "")
    let quotedSenderId: ?String = jsonOptionalStringField(object, "quotedSenderId")
    let quotedTextPreview: ?String = jsonOptionalStringField(object, "quotedTextPreview")
    return BuildQuoteMessageRequest(
        conversationId: conversationId,
        quotedMessageId: quotedMessageId,
        text: text,
        quotedSenderId: quotedSenderId,
        quotedTextPreview: quotedTextPreview
    )
}

public func buildThreadReplyMessageRequestToJson(request: BuildThreadReplyMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "threadId", request.threadId)
    jsonPutString(json, "text", request.text)
    return json.toJsonString()
}

public func buildThreadReplyMessageRequestFromJson(raw: String): BuildThreadReplyMessageRequest {
    return buildThreadReplyMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildThreadReplyMessageRequestFromObject(object: CjJsonObject): BuildThreadReplyMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let threadId = jsonStringField(object, "threadId", "")
    let text = jsonStringField(object, "text", "")
    return BuildThreadReplyMessageRequest(
        conversationId: conversationId,
        threadId: threadId,
        text: text
    )
}

public func buildForwardMessageRequestToJson(request: BuildForwardMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    match (request.merge) {
        case Some(value) => jsonPutBool(json, "merge", value)
        case None => ()
    }
    jsonPutString(json, "title", request.title)
    jsonPutRaw(json, "sourceMessages", request.sourceMessages)
    return json.toJsonString()
}

public func buildForwardMessageRequestFromJson(raw: String): BuildForwardMessageRequest {
    return buildForwardMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildForwardMessageRequestFromObject(object: CjJsonObject): BuildForwardMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let merge: ?Bool = jsonOptionalBoolField(object, "merge")
    let title = jsonStringField(object, "title", "")
    let sourceMessages = jsonRawField(object, "sourceMessages", "[]")
    return BuildForwardMessageRequest(
        conversationId: conversationId,
        merge: merge,
        title: title,
        sourceMessages: sourceMessages
    )
}

public func buildImageMessageRequestToJson(request: BuildImageMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "imageId", request.imageId)
    match (request.payload) {
        case Some(value) => jsonPutRaw(json, "payload", imageContentPayloadToJson(value))
        case None => ()
    }
    return json.toJsonString()
}

public func buildImageMessageRequestFromJson(raw: String): BuildImageMessageRequest {
    return buildImageMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildImageMessageRequestFromObject(object: CjJsonObject): BuildImageMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let imageId = jsonStringField(object, "imageId", "")
    let payload: ?ImageContentPayload = match (object.get("payload")) {
            case Some(value) => Some(imageContentPayloadFromObject(value.asObject()))
            case None => None
        }
    return BuildImageMessageRequest(
        conversationId: conversationId,
        imageId: imageId,
        payload: payload
    )
}

public func buildImageGroupMessageRequestToJson(request: BuildImageGroupMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutRaw(json, "payload", imageGroupContentPayloadToJson(request.payload))
    return json.toJsonString()
}

public func buildImageGroupMessageRequestFromJson(raw: String): BuildImageGroupMessageRequest {
    return buildImageGroupMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildImageGroupMessageRequestFromObject(object: CjJsonObject): BuildImageGroupMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let payload = match (object.get("payload")) {
            case Some(value) => imageGroupContentPayloadFromObject(value.asObject())
            case None => imageGroupContentPayloadFromJson("{}")
        }
    return BuildImageGroupMessageRequest(
        conversationId: conversationId,
        payload: payload
    )
}

public func buildVideoMessageRequestToJson(request: BuildVideoMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "videoId", request.videoId)
    match (request.payload) {
        case Some(value) => jsonPutRaw(json, "payload", videoContentPayloadToJson(value))
        case None => ()
    }
    return json.toJsonString()
}

public func buildVideoMessageRequestFromJson(raw: String): BuildVideoMessageRequest {
    return buildVideoMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildVideoMessageRequestFromObject(object: CjJsonObject): BuildVideoMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let videoId = jsonStringField(object, "videoId", "")
    let payload: ?VideoContentPayload = match (object.get("payload")) {
            case Some(value) => Some(videoContentPayloadFromObject(value.asObject()))
            case None => None
        }
    return BuildVideoMessageRequest(
        conversationId: conversationId,
        videoId: videoId,
        payload: payload
    )
}

public func buildAudioMessageRequestToJson(request: BuildAudioMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "audioId", request.audioId)
    match (request.payload) {
        case Some(value) => jsonPutRaw(json, "payload", audioContentPayloadToJson(value))
        case None => ()
    }
    return json.toJsonString()
}

public func buildAudioMessageRequestFromJson(raw: String): BuildAudioMessageRequest {
    return buildAudioMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildAudioMessageRequestFromObject(object: CjJsonObject): BuildAudioMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let audioId = jsonStringField(object, "audioId", "")
    let payload: ?AudioContentPayload = match (object.get("payload")) {
            case Some(value) => Some(audioContentPayloadFromObject(value.asObject()))
            case None => None
        }
    return BuildAudioMessageRequest(
        conversationId: conversationId,
        audioId: audioId,
        payload: payload
    )
}

public func buildFileMessageRequestToJson(request: BuildFileMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "fileId", request.fileId)
    match (request.payload) {
        case Some(value) => jsonPutRaw(json, "payload", fileContentPayloadToJson(value))
        case None => ()
    }
    return json.toJsonString()
}

public func buildFileMessageRequestFromJson(raw: String): BuildFileMessageRequest {
    return buildFileMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildFileMessageRequestFromObject(object: CjJsonObject): BuildFileMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let fileId = jsonStringField(object, "fileId", "")
    let payload: ?FileContentPayload = match (object.get("payload")) {
            case Some(value) => Some(fileContentPayloadFromObject(value.asObject()))
            case None => None
        }
    return BuildFileMessageRequest(
        conversationId: conversationId,
        fileId: fileId,
        payload: payload
    )
}

public func buildEmojiMessageRequestToJson(request: BuildEmojiMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "emoji", request.emoji)
    return json.toJsonString()
}

public func buildEmojiMessageRequestFromJson(raw: String): BuildEmojiMessageRequest {
    return buildEmojiMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildEmojiMessageRequestFromObject(object: CjJsonObject): BuildEmojiMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let emoji = jsonStringField(object, "emoji", "")
    return BuildEmojiMessageRequest(
        conversationId: conversationId,
        emoji: emoji
    )
}

public func buildLocationMessageRequestToJson(request: BuildLocationMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutFloat(json, "latitude", request.latitude)
    jsonPutFloat(json, "longitude", request.longitude)
    match (request.title) {
        case Some(value) => jsonPutString(json, "title", value)
        case None => ()
    }
    match (request.address) {
        case Some(value) => jsonPutString(json, "address", value)
        case None => ()
    }
    return json.toJsonString()
}

public func buildLocationMessageRequestFromJson(raw: String): BuildLocationMessageRequest {
    return buildLocationMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildLocationMessageRequestFromObject(object: CjJsonObject): BuildLocationMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let latitude = jsonFloatField(object, "latitude", 0.0)
    let longitude = jsonFloatField(object, "longitude", 0.0)
    let title: ?String = jsonOptionalStringField(object, "title")
    let address: ?String = jsonOptionalStringField(object, "address")
    return BuildLocationMessageRequest(
        conversationId: conversationId,
        latitude: latitude,
        longitude: longitude,
        title: title,
        address: address
    )
}

public func buildStickerMessageRequestToJson(request: BuildStickerMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "stickerId", request.stickerId)
    match (request.packageId) {
        case Some(value) => jsonPutString(json, "packageId", value)
        case None => ()
    }
    match (request.payload) {
        case Some(value) => jsonPutRaw(json, "payload", stickerContentPayloadToJson(value))
        case None => ()
    }
    return json.toJsonString()
}

public func buildStickerMessageRequestFromJson(raw: String): BuildStickerMessageRequest {
    return buildStickerMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildStickerMessageRequestFromObject(object: CjJsonObject): BuildStickerMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let stickerId = jsonStringField(object, "stickerId", "")
    let packageId: ?String = jsonOptionalStringField(object, "packageId")
    let payload: ?StickerContentPayload = match (object.get("payload")) {
            case Some(value) => Some(stickerContentPayloadFromObject(value.asObject()))
            case None => None
        }
    return BuildStickerMessageRequest(
        conversationId: conversationId,
        stickerId: stickerId,
        packageId: packageId,
        payload: payload
    )
}

public func buildLinkCardMessageRequestToJson(request: BuildLinkCardMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "url", request.url)
    match (request.title) {
        case Some(value) => jsonPutString(json, "title", value)
        case None => ()
    }
    match (request.description) {
        case Some(value) => jsonPutString(json, "description", value)
        case None => ()
    }
    return json.toJsonString()
}

public func buildLinkCardMessageRequestFromJson(raw: String): BuildLinkCardMessageRequest {
    return buildLinkCardMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildLinkCardMessageRequestFromObject(object: CjJsonObject): BuildLinkCardMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let url = jsonStringField(object, "url", "")
    let title: ?String = jsonOptionalStringField(object, "title")
    let description: ?String = jsonOptionalStringField(object, "description")
    return BuildLinkCardMessageRequest(
        conversationId: conversationId,
        url: url,
        title: title,
        description: description
    )
}

public func buildCardMessageRequestToJson(request: BuildCardMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "id", request.id)
    match (request.cardType) {
        case Some(value) => jsonPutString(json, "cardType", value)
        case None => ()
    }
    match (request.title) {
        case Some(value) => jsonPutString(json, "title", value)
        case None => ()
    }
    match (request.subtitle) {
        case Some(value) => jsonPutString(json, "subtitle", value)
        case None => ()
    }
    match (request.avatar) {
        case Some(value) => jsonPutString(json, "avatar", value)
        case None => ()
    }
    match (request.attributes) {
        case Some(value) => jsonPutRaw(json, "attributes", value)
        case None => ()
    }
    return json.toJsonString()
}

public func buildCardMessageRequestFromJson(raw: String): BuildCardMessageRequest {
    return buildCardMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildCardMessageRequestFromObject(object: CjJsonObject): BuildCardMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let id = jsonStringField(object, "id", "")
    let cardType: ?String = jsonOptionalStringField(object, "cardType")
    let title: ?String = jsonOptionalStringField(object, "title")
    let subtitle: ?String = jsonOptionalStringField(object, "subtitle")
    let avatar: ?String = jsonOptionalStringField(object, "avatar")
    let attributes: ?String = jsonOptionalRawField(object, "attributes")
    return BuildCardMessageRequest(
        conversationId: conversationId,
        id: id,
        cardType: cardType,
        title: title,
        subtitle: subtitle,
        avatar: avatar,
        attributes: attributes
    )
}

public func buildMiniProgramMessageRequestToJson(request: BuildMiniProgramMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "appId", request.appId)
    match (request.path) {
        case Some(value) => jsonPutString(json, "path", value)
        case None => ()
    }
    match (request.title) {
        case Some(value) => jsonPutString(json, "title", value)
        case None => ()
    }
    return json.toJsonString()
}

public func buildMiniProgramMessageRequestFromJson(raw: String): BuildMiniProgramMessageRequest {
    return buildMiniProgramMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildMiniProgramMessageRequestFromObject(object: CjJsonObject): BuildMiniProgramMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let appId = jsonStringField(object, "appId", "")
    let path: ?String = jsonOptionalStringField(object, "path")
    let title: ?String = jsonOptionalStringField(object, "title")
    return BuildMiniProgramMessageRequest(
        conversationId: conversationId,
        appId: appId,
        path: path,
        title: title
    )
}

public func buildRichDocMessageRequestToJson(request: BuildRichDocMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    match (request.docJson) {
        case Some(value) => jsonPutString(json, "docJson", value)
        case None => ()
    }
    match (request.markdown) {
        case Some(value) => jsonPutString(json, "markdown", value)
        case None => ()
    }
    match (request.html) {
        case Some(value) => jsonPutString(json, "html", value)
        case None => ()
    }
    return json.toJsonString()
}

public func buildRichDocMessageRequestFromJson(raw: String): BuildRichDocMessageRequest {
    return buildRichDocMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildRichDocMessageRequestFromObject(object: CjJsonObject): BuildRichDocMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let docJson: ?String = jsonOptionalStringField(object, "docJson")
    let markdown: ?String = jsonOptionalStringField(object, "markdown")
    let html: ?String = jsonOptionalStringField(object, "html")
    return BuildRichDocMessageRequest(
        conversationId: conversationId,
        docJson: docJson,
        markdown: markdown,
        html: html
    )
}

public func normalizeRichDocFromMarkdownRequestToJson(request: NormalizeRichDocFromMarkdownRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "markdown", request.markdown)
    return json.toJsonString()
}

public func normalizeRichDocFromMarkdownRequestFromJson(raw: String): NormalizeRichDocFromMarkdownRequest {
    return normalizeRichDocFromMarkdownRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func normalizeRichDocFromMarkdownRequestFromObject(object: CjJsonObject): NormalizeRichDocFromMarkdownRequest {
    let markdown = jsonStringField(object, "markdown", "")
    return NormalizeRichDocFromMarkdownRequest(
        markdown: markdown
    )
}

public func normalizeRichDocFromHtmlRequestToJson(request: NormalizeRichDocFromHtmlRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "html", request.html)
    return json.toJsonString()
}

public func normalizeRichDocFromHtmlRequestFromJson(raw: String): NormalizeRichDocFromHtmlRequest {
    return normalizeRichDocFromHtmlRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func normalizeRichDocFromHtmlRequestFromObject(object: CjJsonObject): NormalizeRichDocFromHtmlRequest {
    let html = jsonStringField(object, "html", "")
    return NormalizeRichDocFromHtmlRequest(
        html: html
    )
}

public func normalizeRichDocFromDocJsonRequestToJson(request: NormalizeRichDocFromDocJsonRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "docJson", request.docJson)
    return json.toJsonString()
}

public func normalizeRichDocFromDocJsonRequestFromJson(raw: String): NormalizeRichDocFromDocJsonRequest {
    return normalizeRichDocFromDocJsonRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func normalizeRichDocFromDocJsonRequestFromObject(object: CjJsonObject): NormalizeRichDocFromDocJsonRequest {
    let docJson = jsonStringField(object, "docJson", "")
    return NormalizeRichDocFromDocJsonRequest(
        docJson: docJson
    )
}

public func richDocV2NormalizedToJson(request: RichDocV2Normalized): String {
    let json = CjJsonObject()
    jsonPutString(json, "docJson", request.docJson)
    jsonPutString(json, "contentSchema", request.contentSchema)
    jsonPutInt(json, "version", Int64(request.version))
    jsonPutString(json, "plainText", request.plainText)
    jsonPutString(json, "searchText", request.searchText)
    jsonPutRaw(json, "renderHints", request.renderHints)
    match (request.inputFormat) {
        case Some(value) => jsonPutString(json, "inputFormat", value)
        case None => ()
    }
    match (request.sourcePayload) {
        case Some(value) => jsonPutRaw(json, "sourcePayload", value)
        case None => ()
    }
    return json.toJsonString()
}

public func richDocV2NormalizedFromJson(raw: String): RichDocV2Normalized {
    return richDocV2NormalizedFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func richDocV2NormalizedFromObject(object: CjJsonObject): RichDocV2Normalized {
    let docJson = jsonStringField(object, "docJson", "")
    let contentSchema = jsonStringField(object, "contentSchema", "")
    let version = UInt32(jsonIntField(object, "version", 0))
    let plainText = jsonStringField(object, "plainText", "")
    let searchText = jsonStringField(object, "searchText", "")
    let renderHints = jsonRawField(object, "renderHints", "{}")
    let inputFormat: ?String = jsonOptionalStringField(object, "inputFormat")
    let sourcePayload: ?String = jsonOptionalRawField(object, "sourcePayload")
    return RichDocV2Normalized(
        docJson: docJson,
        contentSchema: contentSchema,
        version: version,
        plainText: plainText,
        searchText: searchText,
        renderHints: renderHints,
        inputFormat: inputFormat,
        sourcePayload: sourcePayload
    )
}

public func buildSystemMessageRequestToJson(request: BuildSystemMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "text", request.text)
    return json.toJsonString()
}

public func buildSystemMessageRequestFromJson(raw: String): BuildSystemMessageRequest {
    return buildSystemMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildSystemMessageRequestFromObject(object: CjJsonObject): BuildSystemMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let text = jsonStringField(object, "text", "")
    return BuildSystemMessageRequest(
        conversationId: conversationId,
        text: text
    )
}

public func buildNotificationMessageRequestToJson(request: BuildNotificationMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "text", request.text)
    return json.toJsonString()
}

public func buildNotificationMessageRequestFromJson(raw: String): BuildNotificationMessageRequest {
    return buildNotificationMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildNotificationMessageRequestFromObject(object: CjJsonObject): BuildNotificationMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let text = jsonStringField(object, "text", "")
    return BuildNotificationMessageRequest(
        conversationId: conversationId,
        text: text
    )
}

public func buildVoteMessageRequestToJson(request: BuildVoteMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutRaw(json, "data", request.data)
    return json.toJsonString()
}

public func buildVoteMessageRequestFromJson(raw: String): BuildVoteMessageRequest {
    return buildVoteMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildVoteMessageRequestFromObject(object: CjJsonObject): BuildVoteMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let data = jsonRawField(object, "data", "{}")
    return BuildVoteMessageRequest(
        conversationId: conversationId,
        data: data
    )
}

public func buildTaskMessageRequestToJson(request: BuildTaskMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutRaw(json, "data", request.data)
    return json.toJsonString()
}

public func buildTaskMessageRequestFromJson(raw: String): BuildTaskMessageRequest {
    return buildTaskMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildTaskMessageRequestFromObject(object: CjJsonObject): BuildTaskMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let data = jsonRawField(object, "data", "{}")
    return BuildTaskMessageRequest(
        conversationId: conversationId,
        data: data
    )
}

public func buildScheduleMessageRequestToJson(request: BuildScheduleMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutRaw(json, "data", request.data)
    return json.toJsonString()
}

public func buildScheduleMessageRequestFromJson(raw: String): BuildScheduleMessageRequest {
    return buildScheduleMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildScheduleMessageRequestFromObject(object: CjJsonObject): BuildScheduleMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let data = jsonRawField(object, "data", "{}")
    return BuildScheduleMessageRequest(
        conversationId: conversationId,
        data: data
    )
}

public func buildAnnouncementMessageRequestToJson(request: BuildAnnouncementMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "text", request.text)
    return json.toJsonString()
}

public func buildAnnouncementMessageRequestFromJson(raw: String): BuildAnnouncementMessageRequest {
    return buildAnnouncementMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildAnnouncementMessageRequestFromObject(object: CjJsonObject): BuildAnnouncementMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let text = jsonStringField(object, "text", "")
    return BuildAnnouncementMessageRequest(
        conversationId: conversationId,
        text: text
    )
}

public func buildCustomMessageRequestToJson(request: BuildCustomMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "typeKey", request.typeKey)
    jsonPutRaw(json, "data", request.data)
    return json.toJsonString()
}

public func buildCustomMessageRequestFromJson(raw: String): BuildCustomMessageRequest {
    return buildCustomMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildCustomMessageRequestFromObject(object: CjJsonObject): BuildCustomMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let typeKey = jsonStringField(object, "typeKey", "")
    let data = jsonRawField(object, "data", "{}")
    return BuildCustomMessageRequest(
        conversationId: conversationId,
        typeKey: typeKey,
        data: data
    )
}

public func buildPlaceholderMessageRequestToJson(request: BuildPlaceholderMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    match (request.hint) {
        case Some(value) => jsonPutString(json, "hint", value)
        case None => ()
    }
    return json.toJsonString()
}

public func buildPlaceholderMessageRequestFromJson(raw: String): BuildPlaceholderMessageRequest {
    return buildPlaceholderMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildPlaceholderMessageRequestFromObject(object: CjJsonObject): BuildPlaceholderMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let hint: ?String = jsonOptionalStringField(object, "hint")
    return BuildPlaceholderMessageRequest(
        conversationId: conversationId,
        hint: hint
    )
}

public func buildWithContentMessageRequestToJson(request: BuildWithContentMessageRequest): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutRaw(json, "content", messageContentToJson(request.content))
    return json.toJsonString()
}

public func buildWithContentMessageRequestFromJson(raw: String): BuildWithContentMessageRequest {
    return buildWithContentMessageRequestFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func buildWithContentMessageRequestFromObject(object: CjJsonObject): BuildWithContentMessageRequest {
    let conversationId = jsonStringField(object, "conversationId", "")
    let content = match (object.get("content")) {
            case Some(value) => messageContentFromObject(value.asObject())
            case None => messageContentFromJson("{}")
        }
    return BuildWithContentMessageRequest(
        conversationId: conversationId,
        content: content
    )
}

public func sdkErrorPayloadToJson(request: SdkErrorPayload): String {
    let json = CjJsonObject()
    jsonPutString(json, "code", request.code)
    jsonPutString(json, "message", request.message)
    match (request.operation) {
        case Some(value) => jsonPutString(json, "operation", value)
        case None => ()
    }
    match (request.retryable) {
        case Some(value) => jsonPutBool(json, "retryable", value)
        case None => ()
    }
    jsonPutRaw(json, "details", request.details)
    return json.toJsonString()
}

public func sdkErrorPayloadFromJson(raw: String): SdkErrorPayload {
    return sdkErrorPayloadFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func sdkErrorPayloadFromObject(object: CjJsonObject): SdkErrorPayload {
    let code = jsonStringField(object, "code", "")
    let message = jsonStringField(object, "message", "")
    let operation: ?String = jsonOptionalStringField(object, "operation")
    let retryable: ?Bool = jsonOptionalBoolField(object, "retryable")
    let details = jsonRawField(object, "details", "{}")
    return SdkErrorPayload(
        code: code,
        message: message,
        operation: operation,
        retryable: retryable,
        details: details
    )
}

public func sdkEventEnvelopeToJson(request: SdkEventEnvelope): String {
    let json = CjJsonObject()
    jsonPutString(json, "eventId", request.eventId)
    jsonPutInt(json, "kind", enumWireIndex(SDK_EVENT_KIND_WIRE_ORDER, request.kind))
    jsonPutString(json, "name", request.name)
    jsonPutInt(json, "occurredAt", Int64(request.occurredAt))
    match (request.traceId) {
        case Some(value) => jsonPutString(json, "traceId", value)
        case None => ()
    }
    jsonPutRaw(json, "payload", request.payload)
    return json.toJsonString()
}

public func sdkEventEnvelopeFromJson(raw: String): SdkEventEnvelope {
    return sdkEventEnvelopeFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func sdkEventEnvelopeFromObject(object: CjJsonObject): SdkEventEnvelope {
    let eventId = jsonStringField(object, "eventId", "")
    let kind = jsonEnumField(SDK_EVENT_KIND_WIRE_ORDER, object, "kind", SdkEventKind.Lifecycle)
    let name = jsonStringField(object, "name", "")
    let occurredAt = UInt64(jsonIntField(object, "occurredAt", 0))
    let traceId: ?String = jsonOptionalStringField(object, "traceId")
    let payload = jsonRawField(object, "payload", "{}")
    return SdkEventEnvelope(
        eventId: eventId,
        kind: kind,
        name: name,
        occurredAt: occurredAt,
        traceId: traceId,
        payload: payload
    )
}

public func lifecycleEventToJson(request: LifecycleEvent): String {
    let json = CjJsonObject()
    jsonPutInt(json, "name", enumWireIndex(LIFECYCLE_EVENT_NAME_WIRE_ORDER, request.name))
    jsonPutString(json, "operation", request.operation)
    match (request.userId) {
        case Some(value) => jsonPutString(json, "userId", value)
        case None => ()
    }
    match (request.sessionId) {
        case Some(value) => jsonPutString(json, "sessionId", value)
        case None => ()
    }
    match (request.error) {
        case Some(value) => jsonPutRaw(json, "error", sdkErrorPayloadToJson(value))
        case None => ()
    }
    return json.toJsonString()
}

public func lifecycleEventFromJson(raw: String): LifecycleEvent {
    return lifecycleEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func lifecycleEventFromObject(object: CjJsonObject): LifecycleEvent {
    let name = jsonEnumField(LIFECYCLE_EVENT_NAME_WIRE_ORDER, object, "name", LifecycleEventName.Initializing)
    let operation = jsonStringField(object, "operation", "")
    let userId: ?String = jsonOptionalStringField(object, "userId")
    let sessionId: ?String = jsonOptionalStringField(object, "sessionId")
    let error: ?SdkErrorPayload = match (object.get("error")) {
            case Some(value) => Some(sdkErrorPayloadFromObject(value.asObject()))
            case None => None
        }
    return LifecycleEvent(
        name: name,
        operation: operation,
        userId: userId,
        sessionId: sessionId,
        error: error
    )
}

public func connectionEventToJson(request: ConnectionEvent): String {
    let json = CjJsonObject()
    jsonPutInt(json, "name", enumWireIndex(CONNECTION_EVENT_NAME_WIRE_ORDER, request.name))
    jsonPutInt(json, "state", enumWireIndex(SDK_CONNECTION_STATE_WIRE_ORDER, request.state))
    match (request.reason) {
        case Some(value) => jsonPutString(json, "reason", value)
        case None => ()
    }
    match (request.attempt) {
        case Some(value) => jsonPutInt(json, "attempt", Int64(value))
        case None => ()
    }
    match (request.error) {
        case Some(value) => jsonPutRaw(json, "error", sdkErrorPayloadToJson(value))
        case None => ()
    }
    return json.toJsonString()
}

public func connectionEventFromJson(raw: String): ConnectionEvent {
    return connectionEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func connectionEventFromObject(object: CjJsonObject): ConnectionEvent {
    let name = jsonEnumField(CONNECTION_EVENT_NAME_WIRE_ORDER, object, "name", ConnectionEventName.Connecting)
    let state = jsonEnumField(SDK_CONNECTION_STATE_WIRE_ORDER, object, "state", SdkConnectionState.Disconnected)
    let reason: ?String = jsonOptionalStringField(object, "reason")
    let attempt: ?UInt32 = jsonOptionalUInt32Field(object, "attempt")
    let error: ?SdkErrorPayload = match (object.get("error")) {
            case Some(value) => Some(sdkErrorPayloadFromObject(value.asObject()))
            case None => None
        }
    return ConnectionEvent(
        name: name,
        state: state,
        reason: reason,
        attempt: attempt,
        error: error
    )
}

public func messageReceivedEventToJson(request: MessageReceivedEvent): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "message", messageToJson(request.message))
    return json.toJsonString()
}

public func messageReceivedEventFromJson(raw: String): MessageReceivedEvent {
    return messageReceivedEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messageReceivedEventFromObject(object: CjJsonObject): MessageReceivedEvent {
    let message = match (object.get("message")) {
            case Some(value) => messageFromObject(value.asObject())
            case None => messageFromJson("{}")
        }
    return MessageReceivedEvent(
        message: message
    )
}

public func messageReceivedBatchEventToJson(request: MessageReceivedBatchEvent): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "messages", request.messages)
    return json.toJsonString()
}

public func messageReceivedBatchEventFromJson(raw: String): MessageReceivedBatchEvent {
    return messageReceivedBatchEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messageReceivedBatchEventFromObject(object: CjJsonObject): MessageReceivedBatchEvent {
    let messages = jsonRawField(object, "messages", "[]")
    return MessageReceivedBatchEvent(
        messages: messages
    )
}

public func messageSendAckEventToJson(request: MessageSendAckEvent): String {
    let json = CjJsonObject()
    jsonPutRaw(json, "ack", sendMessageResponseToJson(request.ack))
    return json.toJsonString()
}

public func messageSendAckEventFromJson(raw: String): MessageSendAckEvent {
    return messageSendAckEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messageSendAckEventFromObject(object: CjJsonObject): MessageSendAckEvent {
    let ack = match (object.get("ack")) {
            case Some(value) => sendMessageResponseFromObject(value.asObject())
            case None => sendMessageResponseFromJson("{}")
        }
    return MessageSendAckEvent(
        ack: ack
    )
}

public func messageSendFailedEventToJson(request: MessageSendFailedEvent): String {
    let json = CjJsonObject()
    jsonPutString(json, "clientMsgId", request.clientMsgId)
    jsonPutString(json, "reason", request.reason)
    match (request.error) {
        case Some(value) => jsonPutRaw(json, "error", sdkErrorPayloadToJson(value))
        case None => ()
    }
    return json.toJsonString()
}

public func messageSendFailedEventFromJson(raw: String): MessageSendFailedEvent {
    return messageSendFailedEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messageSendFailedEventFromObject(object: CjJsonObject): MessageSendFailedEvent {
    let clientMsgId = jsonStringField(object, "clientMsgId", "")
    let reason = jsonStringField(object, "reason", "")
    let error: ?SdkErrorPayload = match (object.get("error")) {
            case Some(value) => Some(sdkErrorPayloadFromObject(value.asObject()))
            case None => None
        }
    return MessageSendFailedEvent(
        clientMsgId: clientMsgId,
        reason: reason,
        error: error
    )
}

public func messageMutationEventToJson(request: MessageMutationEvent): String {
    let json = CjJsonObject()
    jsonPutInt(json, "name", enumWireIndex(MESSAGE_EVENT_NAME_WIRE_ORDER, request.name))
    jsonPutString(json, "conversationId", request.conversationId)
    match (request.messageId) {
        case Some(value) => jsonPutString(json, "messageId", value)
        case None => ()
    }
    match (request.serverMsgId) {
        case Some(value) => jsonPutString(json, "serverMsgId", value)
        case None => ()
    }
    match (request.userId) {
        case Some(value) => jsonPutString(json, "userId", value)
        case None => ()
    }
    match (request.reason) {
        case Some(value) => jsonPutString(json, "reason", value)
        case None => ()
    }
    return json.toJsonString()
}

public func messageMutationEventFromJson(raw: String): MessageMutationEvent {
    return messageMutationEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func messageMutationEventFromObject(object: CjJsonObject): MessageMutationEvent {
    let name = jsonEnumField(MESSAGE_EVENT_NAME_WIRE_ORDER, object, "name", MessageEventName.Received)
    let conversationId = jsonStringField(object, "conversationId", "")
    let messageId: ?String = jsonOptionalStringField(object, "messageId")
    let serverMsgId: ?String = jsonOptionalStringField(object, "serverMsgId")
    let userId: ?String = jsonOptionalStringField(object, "userId")
    let reason: ?String = jsonOptionalStringField(object, "reason")
    return MessageMutationEvent(
        name: name,
        conversationId: conversationId,
        messageId: messageId,
        serverMsgId: serverMsgId,
        userId: userId,
        reason: reason
    )
}

public func typingEventToJson(request: TypingEvent): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "userId", request.userId)
    jsonPutBool(json, "typing", request.typing)
    return json.toJsonString()
}

public func typingEventFromJson(raw: String): TypingEvent {
    return typingEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func typingEventFromObject(object: CjJsonObject): TypingEvent {
    let conversationId = jsonStringField(object, "conversationId", "")
    let userId = jsonStringField(object, "userId", "")
    let typing = jsonBoolField(object, "typing", false)
    return TypingEvent(
        conversationId: conversationId,
        userId: userId,
        typing: typing
    )
}

public func readReceiptEventToJson(request: ReadReceiptEvent): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "userId", request.userId)
    jsonPutInt(json, "readSeq", Int64(request.readSeq))
    return json.toJsonString()
}

public func readReceiptEventFromJson(raw: String): ReadReceiptEvent {
    return readReceiptEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func readReceiptEventFromObject(object: CjJsonObject): ReadReceiptEvent {
    let conversationId = jsonStringField(object, "conversationId", "")
    let userId = jsonStringField(object, "userId", "")
    let readSeq = UInt64(jsonIntField(object, "readSeq", 0))
    return ReadReceiptEvent(
        conversationId: conversationId,
        userId: userId,
        readSeq: readSeq
    )
}

public func reactionChangedEventToJson(request: ReactionChangedEvent): String {
    let json = CjJsonObject()
    jsonPutString(json, "conversationId", request.conversationId)
    jsonPutString(json, "serverMsgId", request.serverMsgId)
    jsonPutString(json, "userId", request.userId)
    jsonPutString(json, "emoji", request.emoji)
    jsonPutInt(json, "action", Int64(request.action))
    return json.toJsonString()
}

public func reactionChangedEventFromJson(raw: String): ReactionChangedEvent {
    return reactionChangedEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func reactionChangedEventFromObject(object: CjJsonObject): ReactionChangedEvent {
    let conversationId = jsonStringField(object, "conversationId", "")
    let serverMsgId = jsonStringField(object, "serverMsgId", "")
    let userId = jsonStringField(object, "userId", "")
    let emoji = jsonStringField(object, "emoji", "")
    let action = Int32(jsonIntField(object, "action", 0))
    return ReactionChangedEvent(
        conversationId: conversationId,
        serverMsgId: serverMsgId,
        userId: userId,
        emoji: emoji,
        action: action
    )
}

public func conversationEventToJson(request: ConversationEvent): String {
    let json = CjJsonObject()
    jsonPutInt(json, "name", enumWireIndex(CONVERSATION_EVENT_NAME_WIRE_ORDER, request.name))
    match (request.conversationId) {
        case Some(value) => jsonPutString(json, "conversationId", value)
        case None => ()
    }
    jsonPutRaw(json, "conversationIds", request.conversationIds)
    match (request.unreadCount) {
        case Some(value) => jsonPutInt(json, "unreadCount", Int64(value))
        case None => ()
    }
    return json.toJsonString()
}

public func conversationEventFromJson(raw: String): ConversationEvent {
    return conversationEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func conversationEventFromObject(object: CjJsonObject): ConversationEvent {
    let name = jsonEnumField(CONVERSATION_EVENT_NAME_WIRE_ORDER, object, "name", ConversationEventName.Synced)
    let conversationId: ?String = jsonOptionalStringField(object, "conversationId")
    let conversationIds = jsonRawField(object, "conversationIds", "[]")
    let unreadCount: ?UInt32 = jsonOptionalUInt32Field(object, "unreadCount")
    return ConversationEvent(
        name: name,
        conversationId: conversationId,
        conversationIds: conversationIds,
        unreadCount: unreadCount
    )
}

public func presenceChangedEventToJson(request: PresenceChangedEvent): String {
    let json = CjJsonObject()
    match (request.conversationId) {
        case Some(value) => jsonPutString(json, "conversationId", value)
        case None => ()
    }
    jsonPutString(json, "userId", request.userId)
    jsonPutString(json, "status", request.status)
    jsonPutRaw(json, "extra", request.extra)
    return json.toJsonString()
}

public func presenceChangedEventFromJson(raw: String): PresenceChangedEvent {
    return presenceChangedEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func presenceChangedEventFromObject(object: CjJsonObject): PresenceChangedEvent {
    let conversationId: ?String = jsonOptionalStringField(object, "conversationId")
    let userId = jsonStringField(object, "userId", "")
    let status = jsonStringField(object, "status", "")
    let extra = jsonRawField(object, "extra", "{}")
    return PresenceChangedEvent(
        conversationId: conversationId,
        userId: userId,
        status: status,
        extra: extra
    )
}

public func progressEventToJson(request: ProgressEvent): String {
    let json = CjJsonObject()
    jsonPutInt(json, "name", enumWireIndex(PROGRESS_EVENT_NAME_WIRE_ORDER, request.name))
    jsonPutString(json, "operation", request.operation)
    jsonPutInt(json, "current", Int64(request.current))
    jsonPutInt(json, "total", Int64(request.total))
    match (request.taskId) {
        case Some(value) => jsonPutString(json, "taskId", value)
        case None => ()
    }
    match (request.detail) {
        case Some(value) => jsonPutString(json, "detail", value)
        case None => ()
    }
    return json.toJsonString()
}

public func progressEventFromJson(raw: String): ProgressEvent {
    return progressEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func progressEventFromObject(object: CjJsonObject): ProgressEvent {
    let name = jsonEnumField(PROGRESS_EVENT_NAME_WIRE_ORDER, object, "name", ProgressEventName.SyncProgress)
    let operation = jsonStringField(object, "operation", "")
    let current = UInt64(jsonIntField(object, "current", 0))
    let total = UInt64(jsonIntField(object, "total", 0))
    let taskId: ?String = jsonOptionalStringField(object, "taskId")
    let detail: ?String = jsonOptionalStringField(object, "detail")
    return ProgressEvent(
        name: name,
        operation: operation,
        current: current,
        total: total,
        taskId: taskId,
        detail: detail
    )
}

public func syncEventToJson(request: SyncEvent): String {
    let json = CjJsonObject()
    jsonPutInt(json, "name", enumWireIndex(SYNC_EVENT_NAME_WIRE_ORDER, request.name))
    match (request.trigger) {
        case Some(value) => jsonPutString(json, "trigger", value)
        case None => ()
    }
    match (request.phase) {
        case Some(value) => jsonPutString(json, "phase", value)
        case None => ()
    }
    match (request.task) {
        case Some(value) => jsonPutString(json, "task", value)
        case None => ()
    }
    match (request.progress) {
        case Some(value) => jsonPutInt(json, "progress", Int64(value))
        case None => ()
    }
    match (request.error) {
        case Some(value) => jsonPutRaw(json, "error", sdkErrorPayloadToJson(value))
        case None => ()
    }
    return json.toJsonString()
}

public func syncEventFromJson(raw: String): SyncEvent {
    return syncEventFromObject(wireDecodeResponseObject(jsonObjectFromRaw(raw)))
}

public func syncEventFromObject(object: CjJsonObject): SyncEvent {
    let name = jsonEnumField(SYNC_EVENT_NAME_WIRE_ORDER, object, "name", SyncEventName.StateChanged)
    let trigger: ?String = jsonOptionalStringField(object, "trigger")
    let phase: ?String = jsonOptionalStringField(object, "phase")
    let task: ?String = jsonOptionalStringField(object, "task")
    let progress: ?UInt32 = jsonOptionalUInt32Field(object, "progress")
    let error: ?SdkErrorPayload = match (object.get("error")) {
            case Some(value) => Some(sdkErrorPayloadFromObject(value.asObject()))
            case None => None
        }
    return SyncEvent(
        name: name,
        trigger: trigger,
        phase: phase,
        task: task,
        progress: progress,
        error: error
    )
}

public func messageFromEnvelopeJson(raw: String): Message {
    let object = wireDecodeResponseObject(jsonObjectFromRaw(raw))
    return match (object.get("message")) {
        case Some(value) => messageFromObject(value.asObject())
        case None => messageFromObject(object)
    }
}

// RUST-OWNED WIRE BOUNDARY: BEGIN
public func wireEncodeRequest(raw: String): String {
    return raw
}

public func wireDecodeResponse(raw: String): String {
    return raw
}

public func wireDecodeResponseObject(object: CjJsonObject): CjJsonObject {
    return object
}
// RUST-OWNED WIRE BOUNDARY: END
