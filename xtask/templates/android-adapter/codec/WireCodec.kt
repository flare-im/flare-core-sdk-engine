package com.flare.im.adapter.codec

/** GENERATED. Do not edit by hand. */

import com.flare.im.bridge.FlareSdkException
import com.flare.im.contract.SdkErrorCodes
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

fun pluginUiEvaluationRequestToMap(request: PluginUiEvaluationRequest): Map<String, Any?> =
    mapOf("payload" to request.payload)

fun pluginCommandRequestToMap(request: PluginCommandRequest): Map<String, Any?> =
    mapOf("payload" to request.payload)

fun messageReadReceiptMembersRequestToMap(request: MessageReadReceiptMembersRequest): Map<String, Any?> =
    mapOf("payload" to request.payload)

@Suppress("UNCHECKED_CAST")


fun setHeartbeatAppStateRequestToMap(request: SetHeartbeatAppStateRequest): Map<String, Any?> = buildMap {
    put("appState", request.appState.ordinal)
}

fun setHeartbeatNatTimeoutRequestToMap(request: SetHeartbeatNatTimeoutRequest): Map<String, Any?> = buildMap {
    request.natTimeoutSecs?.let { put("natTimeoutSecs", it) }
}

fun networkInterfaceKindWireValue(value: NetworkInterfaceKind): String = when (value) {
    NetworkInterfaceKind.UNKNOWN -> "unknown"
    NetworkInterfaceKind.WIFI -> "wifi"
    NetworkInterfaceKind.CELLULAR -> "cellular"
    NetworkInterfaceKind.ETHERNET -> "ethernet"
    NetworkInterfaceKind.OTHER -> "other"
}

fun networkChangeRequestToMap(request: NetworkChangeRequest): Map<String, Any?> = buildMap {
    request.available?.let { put("available", it) }
    request.`interface`?.let { put("interface", networkInterfaceKindWireValue(it)) }
    request.expensive?.let { put("expensive", it) }
    request.metered?.let { put("metered", it) }
    request.reason?.let { put("reason", it) }
}

fun updateConversationDraftRequestToMap(request: UpdateConversationDraftRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    request.draft?.let { put("draft", it) }
}

fun heartbeatEffectiveIntervalResponseToMap(request: HeartbeatEffectiveIntervalResponse): Map<String, Any?> = buildMap {
    put("connected", request.connected)
    request.intervalMs?.let { put("intervalMs", it) }
    request.intervalSecs?.let { put("intervalSecs", it) }
}

fun conversationParticipantToMap(request: ConversationParticipant): Map<String, Any?> = buildMap {
    put("userId", request.userId)
    if (request.roles.isNotEmpty()) { put("roles", request.roles) }
    put("muted", request.muted)
    put("pinned", request.pinned)
    put("attributes", request.attributes)
    put("joinedAt", request.joinedAt)
    put("nickname", request.nickname)
    request.name?.let { put("name", displayNameToMap(it)) }
}

fun nameSourceWireValue(value: NameSource): String = when (value) {
    NameSource.ALIAS -> "alias"
    NameSource.MEMBER_NICKNAME -> "memberNickname"
    NameSource.PROFILE -> "profile"
    NameSource.SNAPSHOT -> "snapshot"
    NameSource.REMARK -> "remark"
    NameSource.NAME -> "name"
}

fun displayNameToMap(request: DisplayName): Map<String, Any?> = buildMap {
    put("kind", request.kind.name.lowercase())
    request.text?.let { put("text", it) }
    request.source?.let { put("source", nameSourceWireValue(it)) }
    request.names?.let { put("names", it) }
    request.total?.let { put("total", it) }
    request.entity?.let { put("entity", it.name.lowercase()) }
    request.idTail?.let { put("idTail", it) }
}

fun messagePreviewToMap(request: MessagePreview): Map<String, Any?> = buildMap {
    put("messageId", request.messageId)
    put("senderId", request.senderId)
    put("type", request.type)
    put("text", request.text)
    put("time", request.time)
}

fun conversationToMap(request: Conversation): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("conversationType", conversationTypeWireValue(request.conversationType))
    put("businessType", request.businessType)
    put("channelId", request.channelId)
    put("membersCount", request.membersCount)
    put("displayName", request.displayName)
    request.title?.let { put("title", displayNameToMap(it)) }
    put("avatarUrl", request.avatarUrl)
    request.remark?.let { put("remark", it) }
    request.description?.let { put("description", it) }
    request.lastMessageId?.let { put("lastMessageId", it) }
    request.lastSenderId?.let { put("lastSenderId", it) }
    request.lastMessageAt?.let { put("lastMessageAt", it) }
    request.lastMessagePreview?.let { put("lastMessagePreview", it) }
    request.lastMessage?.let { put("lastMessage", messagePreviewToMap(it)) }
    put("lastSenderNickname", request.lastSenderNickname)
    request.lastSender?.let { put("lastSender", displayNameToMap(it)) }
    put("lastSenderAvatarUrl", request.lastSenderAvatarUrl)
    put("unreadCount", request.unreadCount)
    put("lastReadSeq", request.lastReadSeq)
    put("peerReadSeq", request.peerReadSeq)
    put("maxSeq", request.maxSeq)
    put("visibleAfterSeq", request.visibleAfterSeq)
    put("isPinned", request.isPinned)
    put("isMuted", request.isMuted)
    put("isArchived", request.isArchived)
    put("version", request.version)
    put("updatedAt", request.updatedAt)
    put("createdAt", request.createdAt)
    request.updatedAtTs?.let { put("updatedAtTs", it) }
    put("ext", request.ext)
    put("participantVersion", request.participantVersion)
    if (request.memberPreview.isNotEmpty()) {
        put("memberPreview", request.memberPreview.map { conversationParticipantToMap(it) })
    }
    request.draft?.let { put("draft", it) }
    put("mentionCount", request.mentionCount)
    put("mentionMe", request.mentionMe)
    request.badge?.let { put("badge", it) }
    request.role?.let { put("role", it) }
    if (request.participants.isNotEmpty()) {
        put("participants", request.participants.map { conversationParticipantToMap(it) })
    }
}

fun conversationListQueryToMap(request: ConversationListQuery): Map<String, Any?> = buildMap {
    request.keyword?.let { put("keyword", it) }
    put("includeArchived", request.includeArchived)
    put("unreadOnly", request.unreadOnly)
    put("mentionMeOnly", request.mentionMeOnly)
    put("pinnedOnly", request.pinnedOnly)
    put("mutedOnly", request.mutedOnly)
    put("hasDraftOnly", request.hasDraftOnly)
    put("hasMarkedMessages", request.hasMarkedMessages)
    if (request.conversationTypes.isNotEmpty()) {
        put("conversationTypes", request.conversationTypes.map(::conversationTypeWireValue))
    }
    request.cursor?.let { put("cursor", it) }
    request.limit?.let { put("limit", it) }
}

fun conversationTypeWireValue(type: ConversationType): String = type.name.lowercase()

fun listConversationsResponseToMap(request: ListConversationsResponse): Map<String, Any?> = buildMap {
    if (request.conversations.isNotEmpty()) {
        put("conversations", request.conversations.map { conversationToMap(it) })
    }
}

fun bootstrapHomeTimelineRequestToMap(request: BootstrapHomeTimelineRequest): Map<String, Any?> = buildMap {
    put("conversationLimit", request.conversationLimit)
}

fun openConversationTimelineRequestToMap(request: OpenConversationTimelineRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("messageLimit", request.messageLimit)
}

fun openTimelineViewRequestToMap(request: OpenTimelineViewRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("messageLimit", request.messageLimit)
}

fun loadOlderTimelineViewRequestToMap(request: LoadOlderTimelineViewRequest): Map<String, Any?> = buildMap {
    put("viewId", request.viewId)
    put("messageLimit", request.messageLimit)
}

fun openConversationListViewRequestToMap(request: OpenConversationListViewRequest): Map<String, Any?> = buildMap {
    put("conversationLimit", request.conversationLimit)
}

fun closeViewRequestToMap(request: CloseViewRequest): Map<String, Any?> = buildMap {
    put("viewId", request.viewId)
}

fun homeTimelineSnapshotToMap(request: HomeTimelineSnapshot): Map<String, Any?> = buildMap {
    if (request.conversations.isNotEmpty()) {
        put("conversations", request.conversations.map { conversationToMap(it) })
    }
    put("totalUnread", request.totalUnread)
    put("syncState", request.syncState.ordinal)
}

fun conversationTimelineSnapshotToMap(request: ConversationTimelineSnapshot): Map<String, Any?> = buildMap {
    request.conversation?.let { put("conversation", conversationToMap(it)) }
    if (request.messages.isNotEmpty()) {
        put("messages", request.messages.map { messageToWireMap(it) })
    }
    put("hasMore", request.hasMore)
}

fun conversationVersionToMap(request: ConversationVersion): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("version", request.version)
}

fun syncConversationSummariesRequestToMap(request: SyncConversationSummariesRequest): Map<String, Any?> = buildMap {
    if (request.knownVersions.isNotEmpty()) {
        put("knownVersions", request.knownVersions.map { conversationVersionToMap(it) })
    }
}

fun syncConversationSummariesResponseToMap(request: SyncConversationSummariesResponse): Map<String, Any?> = buildMap {
    if (request.changedConversations.isNotEmpty()) {
        put("changedConversations", request.changedConversations.map { conversationVersionToMap(it) })
    }
}

fun startupHomeSyncRequestToMap(request: StartupHomeSyncRequest): Map<String, Any?> = mapOf(
    "backfillVisibleHistories" to request.backfillVisibleHistories,
    "conversationLimit" to request.conversationLimit,
    "historyBackfillLimit" to request.historyBackfillLimit,
    "historyBackfillMaxConversations" to request.historyBackfillMaxConversations,
    "historyBackfillMaxPagesPerConversation" to request.historyBackfillMaxPagesPerConversation,
    "startBackgroundConvergence" to request.startBackgroundConvergence,
)

fun startupHomeSyncResponseFromJson(value: Any?): StartupHomeSyncResponse {
    val json = mapValue(value)
    val degradedReason = field(json, "degradedReason") as? String
    return StartupHomeSyncResponse(
        backgroundConvergenceStarted = field(json, "backgroundConvergenceStarted") as? Boolean ?: false,
        coldSyncPerformed = field(json, "coldSyncPerformed") as? Boolean ?: false,
        degradedReason = degradedReason?.takeIf { it.isNotEmpty() },
        servedFromLocal = field(json, "servedFromLocal") as? Boolean ?: false,
        snapshot = homeTimelineSnapshotFromJson(field(json, "snapshot")),
    )
}

fun conversationHistoryBackfillRequestToMap(request: ConversationHistoryBackfillRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    request.limit?.let { put("limit", it) }
    request.maxPages?.let { put("maxPages", it) }
}

fun conversationHistoryBackfillResponseFromJson(value: Any?): ConversationHistoryBackfillResponse {
    val json = mapValue(value)
    return ConversationHistoryBackfillResponse(
        conversationId = requiredStringField(json, "conversationId", "ConversationHistoryBackfillResponse"),
        pagesLoaded = requiredLongField(json, "pagesLoaded", "ConversationHistoryBackfillResponse").toInt(),
        oldestSeqBefore = requiredLongField(json, "oldestSeqBefore", "ConversationHistoryBackfillResponse"),
        oldestSeqAfter = requiredLongField(json, "oldestSeqAfter", "ConversationHistoryBackfillResponse"),
        hasMore = requiredBooleanField(json, "hasMore", "ConversationHistoryBackfillResponse"),
        completed = requiredBooleanField(json, "completed", "ConversationHistoryBackfillResponse"),
    )
}

fun reactionEntryToMap(request: ReactionEntry): Map<String, Any?> = buildMap {
    put("emoji", request.emoji)
    if (request.userIds.isNotEmpty()) { put("userIds", request.userIds) }
    put("count", request.count)
}

fun messageLocalStateToMap(request: MessageLocalState): Map<String, Any?> = buildMap {
    put("sending", request.sending)
    put("failed", request.failed)
    put("isLocal", request.isLocal)
    put("sortTs", request.sortTs)
}

fun messageContentToMap(request: MessageContent): Map<String, Any?> = buildMap {
    put("contentType", request.contentType.name.lowercase())
    put("data", request.data)
}

fun createTextMessageRequestToMap(request: CreateTextMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("text", request.text)
}

fun sendMessageResponseToMap(request: SendMessageResponse): Map<String, Any?> = buildMap {
    put("serverId", request.serverId)
    put("clientMsgId", request.clientMsgId)
    put("conversationId", request.conversationId)
    put("seq", request.seq)
    put("timestamp", request.timestamp)
}

fun listMessagesRequestToMap(request: ListMessagesRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("beforeSeq", request.beforeSeq)
    put("limit", request.limit)
}

fun listMessagesResponseToMap(request: ListMessagesResponse): Map<String, Any?> = buildMap {
    if (request.messages.isNotEmpty()) {
        put("messages", request.messages.map { messageToWireMap(it) })
    }
}

fun messageSearchQueryToMap(request: MessageSearchQuery): Map<String, Any?> = buildMap {
    request.keyword?.let { put("keyword", it) }
    request.conversationId?.let { put("conversationId", it) }
    request.senderId?.let { put("senderId", it) }
    request.fromTime?.let { put("fromTime", it) }
    request.toTime?.let { put("toTime", it) }
    if (request.kinds.isNotEmpty()) { put("kinds", request.kinds.map { it.name.lowercase() }) }
    put("limit", request.limit)
    put("includeRecalled", request.includeRecalled)
}

fun mediaSourceInfoToMap(request: MediaSourceInfo): Map<String, Any?> = buildMap {
    request.uuid?.let { put("uuid", it) }
    request.imageId?.let { put("imageId", it) }
    request.url?.let { put("url", it) }
    request.mimeType?.let { put("mimeType", it) }
    request.size?.let { put("size", it) }
    request.width?.let { put("width", it) }
    request.height?.let { put("height", it) }
    request.durationMs?.let { put("durationMs", it) }
}

fun textContentPayloadToMap(request: TextContentPayload): Map<String, Any?> = buildMap {
    put("text", request.text)
}

fun imageContentPayloadToMap(request: ImageContentPayload): Map<String, Any?> = buildMap {
    request.imageId?.let { put("imageId", it) }
    request.source?.let { put("source", mediaSourceInfoToMap(it)) }
    request.thumbnail?.let { put("thumbnail", mediaSourceInfoToMap(it)) }
    request.description?.let { put("description", it) }
}

fun imageGroupItemToMap(request: ImageGroupItem): Map<String, Any?> = buildMap {
    put("imageId", request.imageId)
    request.url?.let { put("url", it) }
    request.title?.let { put("title", it) }
    request.width?.let { put("width", it) }
    request.height?.let { put("height", it) }
}

fun imageGroupContentPayloadToMap(request: ImageGroupContentPayload): Map<String, Any?> = buildMap {
    if (request.images.isNotEmpty()) {
        put("images", request.images.map { imageGroupItemToMap(it) })
    }
    request.title?.let { put("title", it) }
}

fun videoContentPayloadToMap(request: VideoContentPayload): Map<String, Any?> = buildMap {
    request.videoId?.let { put("videoId", it) }
    request.source?.let { put("source", mediaSourceInfoToMap(it)) }
    request.cover?.let { put("cover", mediaSourceInfoToMap(it)) }
    request.description?.let { put("description", it) }
}

fun audioContentPayloadToMap(request: AudioContentPayload): Map<String, Any?> = buildMap {
    request.audioId?.let { put("audioId", it) }
    request.source?.let { put("source", mediaSourceInfoToMap(it)) }
    request.durationMs?.let { put("durationMs", it) }
}

fun fileContentPayloadToMap(request: FileContentPayload): Map<String, Any?> = buildMap {
    request.fileId?.let { put("fileId", it) }
    request.name?.let { put("name", it) }
    request.url?.let { put("url", it) }
    request.mimeType?.let { put("mimeType", it) }
    request.size?.let { put("size", it) }
}

fun emojiContentPayloadToMap(request: EmojiContentPayload): Map<String, Any?> = buildMap {
    put("emoji", request.emoji)
}

fun stickerContentPayloadToMap(request: StickerContentPayload): Map<String, Any?> = buildMap {
    put("stickerId", request.stickerId)
    request.packageId?.let { put("packageId", it) }
    request.url?.let { put("url", it) }
    request.width?.let { put("width", it) }
    request.height?.let { put("height", it) }
    request.format?.let { put("format", it) }
}

fun forwardSourceMessageToMap(request: ForwardSourceMessage): Map<String, Any?> = buildMap {
    put("sourceMessageId", request.sourceMessageId)
    request.sourceConversationId?.let { put("sourceConversationId", it) }
    request.sourceSenderId?.let { put("sourceSenderId", it) }
    request.plainText?.let { put("plainText", it) }
}

fun forwardContentPayloadToMap(request: ForwardContentPayload): Map<String, Any?> = buildMap {
    put("merge", request.merge)
    request.title?.let { put("title", it) }
    if (request.sourceMessages.isNotEmpty()) {
        put("sourceMessages", request.sourceMessages.map { forwardSourceMessageToMap(it) })
    }
}

fun messageBuildCatalogEntryToMap(request: MessageBuildCatalogEntry): Map<String, Any?> = buildMap {
    put("op", request.op.name.lowercase())
    put("method", request.method)
    put("requestType", request.requestType)
    put("contentType", request.contentType.name.lowercase())
    put("messageType", request.messageType)
    put("summary", request.summary)
    put("stability", request.stability)
}

fun listMessageBuildCatalogResponseToMap(request: ListMessageBuildCatalogResponse): Map<String, Any?> = buildMap {
    if (request.entries.isNotEmpty()) {
        put("entries", request.entries.map { messageBuildCatalogEntryToMap(it) })
    }
}

fun buildTypedMessageRequestToMap(request: BuildTypedMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("op", request.op.name.lowercase())
    put("data", request.data)
}

fun buildTextMessageRequestToMap(request: BuildTextMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("text", request.text)
}

fun buildQuoteMessageRequestToMap(request: BuildQuoteMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("quotedMessageId", request.quotedMessageId)
    put("text", request.text)
    request.quotedSenderId?.let { put("quotedSenderId", it) }
    request.quotedTextPreview?.let { put("quotedTextPreview", it) }
}

fun buildThreadReplyMessageRequestToMap(request: BuildThreadReplyMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("threadId", request.threadId)
    put("text", request.text)
}

fun buildForwardMessageRequestToMap(request: BuildForwardMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("merge", request.merge)
    put("title", request.title)
    if (request.sourceMessages.isNotEmpty()) {
        // 这里的 sourceMessages 是**完整消息**（BuildForwardMessageRequest），
        // 不同于 ForwardContentPayload 里的 id 存根 —— 两者代码长得一样，别改错。
        // 转发载荷要把原文嵌进去，核心侧 forward_item_from_source 会读
        // content / senderId / conversationId。
        put("sourceMessages", request.sourceMessages.map { messageToWireMap(it) })
    }
}

fun buildImageMessageRequestToMap(request: BuildImageMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("imageId", request.imageId)
    request.payload?.let { put("payload", imageContentPayloadToMap(it)) }
}

fun buildImageGroupMessageRequestToMap(request: BuildImageGroupMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("payload", imageGroupContentPayloadToMap(request.payload))
}

fun buildVideoMessageRequestToMap(request: BuildVideoMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("videoId", request.videoId)
    request.payload?.let { put("payload", videoContentPayloadToMap(it)) }
}

fun buildAudioMessageRequestToMap(request: BuildAudioMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("audioId", request.audioId)
    request.payload?.let { put("payload", audioContentPayloadToMap(it)) }
}

fun buildFileMessageRequestToMap(request: BuildFileMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("fileId", request.fileId)
    request.payload?.let { put("payload", fileContentPayloadToMap(it)) }
}

fun buildEmojiMessageRequestToMap(request: BuildEmojiMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("emoji", request.emoji)
}

fun buildLocationMessageRequestToMap(request: BuildLocationMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("latitude", request.latitude)
    put("longitude", request.longitude)
    request.title?.let { put("title", it) }
    request.address?.let { put("address", it) }
}

fun buildStickerMessageRequestToMap(request: BuildStickerMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("stickerId", request.stickerId)
    request.packageId?.let { put("packageId", it) }
    request.payload?.let { put("payload", stickerContentPayloadToMap(it)) }
}

fun buildLinkCardMessageRequestToMap(request: BuildLinkCardMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("url", request.url)
    request.title?.let { put("title", it) }
    request.description?.let { put("description", it) }
}

fun buildCardMessageRequestToMap(request: BuildCardMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("id", request.id)
    request.cardType?.let { put("cardType", it) }
    request.title?.let { put("title", it) }
    request.subtitle?.let { put("subtitle", it) }
    request.avatar?.let { put("avatar", it) }
    request.attributes?.let { put("attributes", it) }
}

fun buildMiniProgramMessageRequestToMap(request: BuildMiniProgramMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("appId", request.appId)
    request.pagePath?.let { put("pagePath", it) }
    request.title?.let { put("title", it) }
    request.thumbnailUrl?.let { put("thumbnailUrl", it) }
    request.extra?.let { put("extra", it) }
}

fun buildRichDocMessageRequestToMap(request: BuildRichDocMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("docJson", request.docJson)
    put("contentSchema", request.contentSchema)
    put("plainText", request.plainText)
    request.inputFormat?.let { put("inputFormat", it) }
    request.inputFormatVersion?.let { put("inputFormatVersion", it) }
    request.sourcePayload?.let { put("sourcePayload", it) }
    request.title?.let { put("title", it) }
    request.searchText?.let { put("searchText", it) }
    request.renderHintsJson?.let { put("renderHintsJson", it) }
}

fun normalizeRichDocFromMarkdownRequestToMap(request: NormalizeRichDocFromMarkdownRequest): Map<String, Any?> = buildMap {
    put("markdown", request.markdown)
}

fun normalizeRichDocFromHtmlRequestToMap(request: NormalizeRichDocFromHtmlRequest): Map<String, Any?> = buildMap {
    put("html", request.html)
}

fun normalizeRichDocFromDocJsonRequestToMap(request: NormalizeRichDocFromDocJsonRequest): Map<String, Any?> = buildMap {
    put("docJson", request.docJson)
}

fun richDocV2NormalizedToMap(request: RichDocV2Normalized): Map<String, Any?> = buildMap {
    put("docJson", request.docJson)
    put("contentSchema", request.contentSchema)
    put("version", request.version)
    put("plainText", request.plainText)
    put("searchText", request.searchText)
    put("renderHints", request.renderHints)
    request.inputFormat?.let { put("inputFormat", it) }
    put("sourcePayload", request.sourcePayload)
}

fun buildSystemMessageRequestToMap(request: BuildSystemMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("eventKind", request.eventKind)
    put("body", request.body)
}

fun buildNotificationMessageRequestToMap(request: BuildNotificationMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("title", request.title)
    put("body", request.body)
}

fun buildVoteMessageRequestToMap(request: BuildVoteMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("voteId", request.voteId)
    put("title", request.title)
    if (request.options.isNotEmpty()) put("options", request.options)
    if (request.participantUserIds.isNotEmpty()) put("participantUserIds", request.participantUserIds)
}

fun buildTaskMessageRequestToMap(request: BuildTaskMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("taskId", request.taskId)
    put("title", request.title)
    request.status?.let { put("status", it) }
    if (request.participantUserIds.isNotEmpty()) put("participantUserIds", request.participantUserIds)
}

fun buildScheduleMessageRequestToMap(request: BuildScheduleMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("scheduleId", request.scheduleId)
    put("title", request.title)
    put("startTimeMs", request.startTimeMs)
    put("endTimeMs", request.endTimeMs)
    if (request.participantUserIds.isNotEmpty()) put("participantUserIds", request.participantUserIds)
}

fun buildAnnouncementMessageRequestToMap(request: BuildAnnouncementMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("title", request.title)
    put("body", request.body)
}

fun buildCustomMessageRequestToMap(request: BuildCustomMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("type", request.type)
}

fun buildPlaceholderMessageRequestToMap(request: BuildPlaceholderMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("reason", request.reason)
}

fun buildWithContentMessageRequestToMap(request: BuildWithContentMessageRequest): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("content", messageContentToMap(request.content))
}

fun sdkErrorPayloadToMap(request: SdkErrorPayload): Map<String, Any?> = buildMap {
    put("code", request.code)
    request.machineReason?.let { put("machineReason", it) }
    put("message", request.message)
    request.operation?.let { put("operation", it) }
    put("retryable", request.retryable)
    put("details", request.details)
}

fun sdkEventEnvelopeToMap(request: SdkEventEnvelope): Map<String, Any?> = buildMap {
    put("eventId", request.eventId)
    put("kind", request.kind.ordinal)
    put("name", request.name)
    put("occurredAt", request.occurredAt)
    request.traceId?.let { put("traceId", it) }
    put("payload", request.payload)
}

fun lifecycleEventToMap(request: LifecycleEvent): Map<String, Any?> = buildMap {
    put("name", request.name.ordinal)
    put("operation", request.operation)
    request.userId?.let { put("userId", it) }
    request.sessionId?.let { put("sessionId", it) }
    request.error?.let { put("error", sdkErrorPayloadToMap(it)) }
}

fun connectionEventToMap(request: ConnectionEvent): Map<String, Any?> = buildMap {
    put("name", request.name.ordinal)
    put("state", request.state.ordinal)
    request.reason?.let { put("reason", it) }
    request.attempt?.let { put("attempt", it) }
    request.error?.let { put("error", sdkErrorPayloadToMap(it)) }
}

fun messageReceivedEventToMap(request: MessageReceivedEvent): Map<String, Any?> = buildMap {
    put("message", messageToWireMap(request.message))
}

fun messageReceivedBatchEventToMap(request: MessageReceivedBatchEvent): Map<String, Any?> = buildMap {
    if (request.messages.isNotEmpty()) {
        put("messages", request.messages.map { messageToWireMap(it) })
    }
}

fun messageSendAckEventToMap(request: MessageSendAckEvent): Map<String, Any?> = buildMap {
    put("ack", sendMessageResponseToMap(request.ack))
}

fun messageSendFailedEventToMap(request: MessageSendFailedEvent): Map<String, Any?> = buildMap {
    put("clientMsgId", request.clientMsgId)
    put("reason", request.reason)
    request.error?.let { put("error", sdkErrorPayloadToMap(it)) }
}

fun messageMutationEventToMap(request: MessageMutationEvent): Map<String, Any?> = buildMap {
    put("name", request.name.ordinal)
    put("conversationId", request.conversationId)
    request.messageId?.let { put("messageId", it) }
    request.serverMsgId?.let { put("serverMsgId", it) }
    request.userId?.let { put("userId", it) }
    request.reason?.let { put("reason", it) }
}

fun typingEventToMap(request: TypingEvent): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("userId", request.userId)
    put("typing", request.typing)
}

fun readReceiptEventToMap(request: ReadReceiptEvent): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("userId", request.userId)
    put("readSeq", request.readSeq)
}

fun reactionChangedEventToMap(request: ReactionChangedEvent): Map<String, Any?> = buildMap {
    put("conversationId", request.conversationId)
    put("serverMsgId", request.serverMsgId)
    put("userId", request.userId)
    put("emoji", request.emoji)
    put("action", request.action)
}

fun conversationEventToMap(request: ConversationEvent): Map<String, Any?> = buildMap {
    put("name", request.name.ordinal)
    request.conversationId?.let { put("conversationId", it) }
    if (request.conversationIds.isNotEmpty()) { put("conversationIds", request.conversationIds) }
    request.unreadCount?.let { put("unreadCount", it) }
}

fun presenceChangedEventToMap(request: PresenceChangedEvent): Map<String, Any?> = buildMap {
    request.conversationId?.let { put("conversationId", it) }
    put("userId", request.userId)
    put("status", request.status)
    put("extra", request.extra)
}

fun progressEventToMap(request: ProgressEvent): Map<String, Any?> = buildMap {
    put("name", request.name.ordinal)
    put("operation", request.operation)
    put("current", request.current)
    put("total", request.total)
    request.taskId?.let { put("taskId", it) }
    request.detail?.let { put("detail", it) }
}

fun syncEventToMap(request: SyncEvent): Map<String, Any?> = buildMap {
    put("name", request.name.ordinal)
    request.trigger?.let { put("trigger", it) }
    request.phase?.let { put("phase", it) }
    request.task?.let { put("task", it) }
    request.progress?.let { put("progress", it) }
    request.error?.let { put("error", sdkErrorPayloadToMap(it)) }
}



fun sendMessageRequestToMap(request: SendMessageRequest): Map<String, Any?> =
    buildMap { put("message", messageToWireMap(request.message)); request.expectedUserId?.let { put("expectedUserId", it) } }

fun messageToWireMap(message: Message): Map<String, Any?> = buildMap {
    put("serverId", message.serverId)
    put("clientMsgId", message.clientMsgId)
    put("conversationId", message.conversationId)
    put("conversationType", message.conversationType)
    put("channelId", message.channelId)
    put("senderId", message.senderId)
    put("source", message.source)
    put("conversationSeq", message.conversationSeq)
    put("createdAt", message.createdAt)
    put("clientCreatedAt", message.clientCreatedAt)
    put("messageType", message.messageType)
    message.content?.let {
        put("content", buildMap {
            put("contentType", it.contentType.name.lowercase())
            putAll(it.data)
        })
    }
    put("senderName", message.senderName)
    put("senderAvatar", message.senderAvatar)
    put("senderDisplayName", message.senderDisplayName)
    message.replyTo?.let { put("replyTo", it) }
    message.quotePreview?.let { put("quotePreview", it) }
    message.threadId?.let { put("threadId", it) }
    put("status", message.status)
    put("isRead", message.isRead)
    put("isRecalled", message.isRecalled)
    put("isEdited", message.isEdited)
    put("mentionUsers", message.mentionUsers)
    put("mentionAll", message.mentionAll)
    put("attributes", message.attributes)
    put("extensions", message.extensions)
    put("reactions", message.reactions.map { reactionEntryToMap(it) })
    message.localState?.let { put("localState", messageLocalStateToMap(it)) }
    put("textPreview", message.textPreview)
    put("timelineKey", message.timelineKey)
    put("timelineSortTs", message.timelineSortTs)
    put("version", message.version)
    put("updatedAt", message.updatedAt)
}

fun listOfMaps(value: Any?): List<Map<String, Any?>> {
    if (value !is List<*>) return emptyList()
    return value.mapNotNull { item ->
        when (item) {
            is Map<*, *> -> item.entries.associate { (k, v) -> k.toString() to v }
            else -> null
        }
    }
}

fun requiredListOfMaps(value: Any?, field: String, context: String): List<Map<String, Any?>> {
    if (value !is List<*>) {
        throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "$context.$field must be an array",
            operation = "wire.decode",
            details = mapOf("field" to "$context.$field", "expected" to "array"),
        )
    }
    return value.mapIndexed { index, item ->
        if (item !is Map<*, *>) {
            throw FlareSdkException(
                SdkErrorCodes.INVALIDPARAMETER,
                "$context.$field[$index] must be an object",
                operation = "wire.decode",
                details = mapOf("field" to "$context.$field[$index]", "expected" to "object"),
            )
        }
        item.entries.associate { (k, v) -> k.toString() to v }
    }
}

fun intValue(value: Any?): Long {
    val number = when (value) {
        is Byte -> value.toLong()
        is Short -> value.toLong()
        is Int -> value.toLong()
        is Long -> value
        is Float -> if (value.isFinite() && value >= 0 && value % 1.0f == 0.0f) value.toLong() else null
        is Double -> if (value.isFinite() && value >= 0 && value % 1.0 == 0.0) value.toLong() else null
        else -> null
    }
    if (number != null && number >= 0) return number
    throw FlareSdkException(
        SdkErrorCodes.INVALIDPARAMETER,
        "wire field must be an unsigned integer",
        operation = "wire.decode",
        details = mapOf("expected" to "unsigned integer"),
    )
}

private fun field(json: Map<String, Any?>, key: String): Any? = json[key]

private fun stringValue(value: Any?): String =
    when (value) {
        is String -> value
        else -> throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "wire field must be a string",
            operation = "wire.decode",
            details = mapOf("expected" to "string"),
        )
    }

/**
 * 「必填」= 字段**存在且是字符串**，空串是合法值。
 *
 * 曾经额外要求 isNotBlank，于是同一条服务端数据在 web/iOS 上正常、在
 * Android/Flutter 上直接抛异常：真实事件里 clientMsgId 常常是空串（别人发来的
 * 消息没有我方的客户端去重 id），protobuf3 又会把未设置的字符串序列化成 ""。
 * 结果一收到实时消息批就整批解码失败，等于收不到消息。
 *
 * TypeScript 与 Swift 一直是「存在即可」，四端必须一致，
 * 否则同一份 wire 数据在不同端有不同结果。
 */
private fun requiredStringField(json: Map<String, Any?>, key: String, context: String): String {
    val value = field(json, key)
    if (value is String) return value
    throw FlareSdkException(
        SdkErrorCodes.INVALIDPARAMETER,
        "$context.$key is required",
        operation = "wire.decode",
        details = mapOf("field" to "$context.$key", "expected" to "string"),
    )
}

private fun requiredLongField(json: Map<String, Any?>, key: String, context: String): Long {
    val value = field(json, key)
    val number = when (value) {
        is Byte -> value.toLong()
        is Short -> value.toLong()
        is Int -> value.toLong()
        is Long -> value
        is Float -> if (value.isFinite() && value >= 0 && value % 1.0f == 0.0f) value.toLong() else null
        is Double -> if (value.isFinite() && value >= 0 && value % 1.0 == 0.0) value.toLong() else null
        else -> null
    }
    if (number != null && number >= 0) return number
    val message = if (value == null) {
        "$context.$key is required"
    } else {
        "$context.$key must be an unsigned integer"
    }
    throw FlareSdkException(
        SdkErrorCodes.INVALIDPARAMETER,
        message,
        operation = "wire.decode",
        details = mapOf("field" to "$context.$key", "expected" to "unsigned integer"),
    )
}

private fun requiredSignedLongField(json: Map<String, Any?>, key: String, context: String): Long {
    val value = field(json, key)
    val number = when (value) {
        is Byte -> value.toLong()
        is Short -> value.toLong()
        is Int -> value.toLong()
        is Long -> value
        is Float -> if (value.isFinite() && value % 1.0f == 0.0f) value.toLong() else null
        is Double -> if (value.isFinite() && value % 1.0 == 0.0) value.toLong() else null
        else -> null
    }
    if (number != null) return number
    val message = if (value == null) {
        "$context.$key is required"
    } else {
        "$context.$key must be an integer"
    }
    throw FlareSdkException(
        SdkErrorCodes.INVALIDPARAMETER,
        message,
        operation = "wire.decode",
        details = mapOf("field" to "$context.$key", "expected" to "integer"),
    )
}

private fun requiredBooleanField(json: Map<String, Any?>, key: String, context: String): Boolean {
    val value = field(json, key)
    if (value is Boolean) return value
    val message = if (value == null) {
        "$context.$key is required"
    } else {
        "$context.$key must be a boolean"
    }
    throw FlareSdkException(
        SdkErrorCodes.INVALIDPARAMETER,
        message,
        operation = "wire.decode",
        details = mapOf("field" to "$context.$key", "expected" to "boolean"),
    )
}

private fun requiredStringList(value: Any?, field: String, context: String): List<String> {
    if (value !is List<*>) {
        throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "$context.$field must be an array",
            operation = "wire.decode",
            details = mapOf("field" to "$context.$field", "expected" to "array"),
        )
    }
    return value.mapIndexed { index, item ->
        if (item !is String) {
            throw FlareSdkException(
                SdkErrorCodes.INVALIDPARAMETER,
                "$context.$field[$index] must be a string",
                operation = "wire.decode",
                details = mapOf("field" to "$context.$field[$index]", "expected" to "string"),
            )
        }
        item
    }
}

private fun requiredStringMap(value: Any?, field: String, context: String): Map<String, String> {
    if (value !is Map<*, *>) {
        throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "$context.$field must be an object",
            operation = "wire.decode",
            details = mapOf("field" to "$context.$field", "expected" to "object"),
        )
    }
    return value.entries.associate { (k, v) ->
        if (k !is String || v !is String) {
            val suffix = if (k is String) ".$k" else ""
            throw FlareSdkException(
                SdkErrorCodes.INVALIDPARAMETER,
                "$context.$field$suffix must be a string",
                operation = "wire.decode",
                details = mapOf("field" to "$context.$field$suffix", "expected" to "string"),
            )
        }
        k to v
    }
}

private fun stringList(value: Any?): List<String> =
    when (value) {
        is List<*> -> value.mapIndexed { index, item ->
            if (item !is String) {
                throw FlareSdkException(
                    SdkErrorCodes.INVALIDPARAMETER,
                    "wire string array item $index must be a string",
                    operation = "wire.decode",
                    details = mapOf("expected" to "string"),
                )
            }
            item
        }
        else -> throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "wire field must be a string array",
            operation = "wire.decode",
            details = mapOf("expected" to "string array"),
        )
    }

private fun stringMap(value: Any?): Map<String, String> =
    when (value) {
        is Map<*, *> -> value.entries.associate { (k, v) ->
            if (k !is String || v !is String) {
                throw FlareSdkException(
                    SdkErrorCodes.INVALIDPARAMETER,
                    "wire field must be a string map",
                    operation = "wire.decode",
                    details = mapOf("expected" to "string map"),
                )
            }
            k to v
        }
        else -> throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "wire field must be a string map",
            operation = "wire.decode",
            details = mapOf("expected" to "string map"),
        )
    }

private fun bytesMap(value: Any?): Map<String, ByteArray> =
    when (value) {
        is Map<*, *> -> value.entries.mapNotNull { (k, v) ->
            val bytes = when (v) {
                is ByteArray -> v
                is List<*> -> v.mapNotNull { item ->
                    when (item) {
                        is Number -> item.toInt().takeIf { it in 0..255 }?.toByte()
                        else -> null
                    }
                }.toByteArray()
                else -> null
            }
            bytes?.let { k.toString() to it }
        }.toMap()
        else -> emptyMap()
    }

fun mapValue(value: Any?): Map<String, Any?> =
    when (value) {
        is Map<*, *> -> value.entries.associate { (k, v) -> k.toString() to v }
        else -> emptyMap()
    }

fun networkChangeResponseFromJson(value: Any?): NetworkChangeResponse {
    val json = mapValue(value)
    return NetworkChangeResponse(
        reconnected = requiredBooleanField(json, "reconnected", "NetworkChangeResponse"),
    )
}

fun serverClockResponseFromJson(value: Any?): ServerClockResponse {
    val json = mapValue(value)
    return ServerClockResponse(
        offsetMs = requiredSignedLongField(json, "offsetMs", "ServerClockResponse"),
        estimated = requiredBooleanField(json, "estimated", "ServerClockResponse"),
    )
}

fun heartbeatEffectiveIntervalResponseFromJson(value: Any?): HeartbeatEffectiveIntervalResponse {
    val json = mapValue(value)
    return HeartbeatEffectiveIntervalResponse(
        connected = requiredBooleanField(json, "connected", "HeartbeatEffectiveIntervalResponse"),
        intervalMs = field(json, "intervalMs")?.let(::intValue),
        intervalSecs = field(json, "intervalSecs")?.let(::intValue),
    )
}

fun runtimeHealthResponseFromJson(value: Any?): RuntimeHealthResponse {
    val json = mapValue(value)
    return RuntimeHealthResponse(
        metricsEnabled = requiredBooleanField(json, "metricsEnabled", "RuntimeHealthResponse"),
        metricsJson = requiredStringField(json, "metricsJson", "RuntimeHealthResponse"),
        rawSubscriberDroppedTotal = requiredLongField(json, "rawSubscriberDroppedTotal", "RuntimeHealthResponse"),
        sessionGeneration = requiredLongField(json, "sessionGeneration", "RuntimeHealthResponse"),
        state = requiredStringField(json, "state", "RuntimeHealthResponse"),
        stateCode = requiredLongField(json, "stateCode", "RuntimeHealthResponse").toInt(),
    )
}

fun timelineSyncStateFromJson(value: Any?): TimelineSyncState {
    val raw = value?.toString()?.trim().orEmpty()
    return when (raw) {
        "localReady" -> TimelineSyncState.LOCAL_READY
        "synced" -> TimelineSyncState.SYNCED
        "partial" -> TimelineSyncState.PARTIAL
        else -> throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "invalid timeline sync state: ${raw.ifEmpty { "<empty>" }}",
            operation = "wire.timeline.decode",
            details = mapOf("field" to "syncState"),
        )
    }
}

fun reactionEntryFromJson(value: Any?): ReactionEntry {
    val json = mapValue(value)
    return ReactionEntry(
        count = requiredLongField(json, "count", "ReactionEntry").toInt(),
        emoji = requiredStringField(json, "emoji", "ReactionEntry"),
        userIds = requiredStringList(field(json, "userIds"), "userIds", "ReactionEntry"),
    )
}

fun messageLocalStateFromJson(value: Any?): MessageLocalState? {
    val json = mapValue(value)
    if (json.isEmpty()) return null
    return MessageLocalState(
        failed = requiredBooleanField(json, "failed", "MessageLocalState"),
        isLocal = requiredBooleanField(json, "isLocal", "MessageLocalState"),
        sending = requiredBooleanField(json, "sending", "MessageLocalState"),
        sortTs = intValue(field(json, "sortTs")),
    )
}

/** The core's receipt projection; null when absent or malformed (an older core). */
fun messageReceiptViewFromJson(value: Any?): MessageReceiptView? {
    val json = mapValue(value)
    if (json.isEmpty()) return null
    val status = when (field(json, "status")?.toString()) {
        "pending" -> MessageReceiptStatus.PENDING
        "sending" -> MessageReceiptStatus.SENDING
        "sent" -> MessageReceiptStatus.SENT
        "read" -> MessageReceiptStatus.READ
        "failed" -> MessageReceiptStatus.FAILED
        else -> return null
    }
    val summary = mapValue(field(json, "readSummary"))
    fun count(key: String): Int? =
        (field(summary, key) as? Number)?.toLong()?.takeIf { it in 0..Int.MAX_VALUE.toLong() }?.toInt()
    val readCount = count("readCount")
    val unreadCount = count("unreadCount")
    val total = count("total")
    val readSummary =
        if (readCount != null && unreadCount != null && total != null && total > 0) {
            MessageReadSummary(readCount = readCount, unreadCount = unreadCount, total = total)
        } else {
            null
        }
    return MessageReceiptView(status = status, readSummary = readSummary)
}

private val UNKNOWN_PERSON_NAME = DisplayName(kind = DisplayNameKind.UNNAMED, entity = NameEntity.PERSON, idTail = "")

/**
 * A projected name: conversation `title` and `lastSender`, message `sender`, participant `name`. Every value the SDK
 * hands over carries `title`, `sender` and `name`, and `lastSender` when there is a latest message; the fields stay
 * optional while hosts move to them, so a value without one (from an older core) reads as null. It never fails, so a
 * core newer than this package still yields a name: an unknown `entity` reads as `person`, an unknown `kind`, a
 * `named` value without text or a value that is not an object reads as an unnamed person without an id tail, an
 * unknown `source` is dropped. The kits read names by the same rule.
 */
fun displayNameFromJson(value: Any?): DisplayName? {
    if (value == null) return null
    if (value !is Map<*, *>) return UNKNOWN_PERSON_NAME
    val json = mapValue(value)
    return when (field(json, "kind")) {
        "named" -> {
            val text = field(json, "text") as? String ?: return UNKNOWN_PERSON_NAME
            DisplayName(
                kind = DisplayNameKind.NAMED,
                text = text,
                source = NameSource.entries.firstOrNull { nameSourceWireValue(it) == field(json, "source") },
            )
        }
        "members" -> DisplayName(
            kind = DisplayNameKind.MEMBERS,
            names = (field(json, "names") as? List<*>)?.filterIsInstance<String>() ?: emptyList(),
            total = displayNameTotal(field(json, "total")),
        )
        "unnamed" -> DisplayName(
            kind = DisplayNameKind.UNNAMED,
            entity = NameEntity.entries.firstOrNull { it.name.lowercase() == field(json, "entity") } ?: NameEntity.PERSON,
            idTail = field(json, "idTail") as? String ?: "",
        )
        else -> UNKNOWN_PERSON_NAME
    }
}

private fun displayNameTotal(value: Any?): Int {
    val total = (value as? Number)?.toDouble() ?: return 0
    if (!total.isFinite() || total < 0) return 0
    return if (total >= Int.MAX_VALUE) Int.MAX_VALUE else total.toInt()
}

fun messagePreviewFromJson(value: Any?): MessagePreview? {
    val json = mapValue(value)
    if (json.isEmpty()) return null
    return MessagePreview(
        messageId = requiredStringField(json, "messageId", "MessagePreview"),
        senderId = requiredStringField(json, "senderId", "MessagePreview"),
        text = requiredStringField(json, "text", "MessagePreview"),
        time = requiredLongField(json, "time", "MessagePreview"),
        type = requiredLongField(json, "type", "MessagePreview").toInt(),
    )
}

fun conversationParticipantFromJson(value: Any?): ConversationParticipant {
    val json = mapValue(value)
    return ConversationParticipant(
        attributes = requiredStringMap(field(json, "attributes"), "attributes", "ConversationParticipant"),
        joinedAt = requiredLongField(json, "joinedAt", "ConversationParticipant"),
        muted = requiredBooleanField(json, "muted", "ConversationParticipant"),
        name = displayNameFromJson(field(json, "name")),
        nickname = requiredStringField(json, "nickname", "ConversationParticipant"),
        pinned = requiredBooleanField(json, "pinned", "ConversationParticipant"),
        roles = requiredStringList(field(json, "roles"), "roles", "ConversationParticipant"),
        userId = requiredStringField(json, "userId", "ConversationParticipant"),
    )
}

fun richDocV2NormalizedFromJson(value: Any?): RichDocV2Normalized {
    val json = mapValue(value)
    val sourcePayload = field(json, "sourcePayload")
    return RichDocV2Normalized(
        docJson = stringValue(field(json, "docJson")),
        contentSchema = stringValue(field(json, "contentSchema")),
        version = intValue(field(json, "version")).toInt(),
        plainText = stringValue(field(json, "plainText")),
        searchText = stringValue(field(json, "searchText")),
        renderHints = mapValue(field(json, "renderHints")),
        inputFormat = field(json, "inputFormat")?.toString(),
        sourcePayload = sourcePayload?.let(::mapValue),
    )
}

fun messageContentTypeFromJson(value: Any?): MessageContentType = when (value) {
    is String -> MessageContentType.entries.firstOrNull { it.name.lowercase() == value.lowercase() }
        ?: throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "invalid message content type: ${value.ifBlank { "<empty>" }}",
            operation = "wire.message.decode",
            details = mapOf("field" to "content.contentType"),
        )
    else -> throw FlareSdkException(
        SdkErrorCodes.INVALIDPARAMETER,
        "invalid message content type: <empty>",
        operation = "wire.message.decode",
        details = mapOf("field" to "content.contentType"),
    )
}

fun messageContentDataFromJson(rawContent: Map<String, Any?>): Map<String, Any?> {
    return rawContent.filterKeys { key -> key != "contentType" && key != "messageType" }
}

fun messageFromJson(value: Any?): Message {
    val json = value as? Map<String, Any?> ?: emptyMap()
    val rawContent = field(json, "content") as? Map<String, Any?>
    val content = rawContent?.let {
        MessageContent(
            contentType = messageContentTypeFromJson(it["contentType"]),
            data = messageContentDataFromJson(it),
        )
    }
    return Message(
        attributes = requiredStringMap(field(json, "attributes"), "attributes", "Message"),
        channelId = requiredStringField(json, "channelId", "Message"),
        clientCreatedAt = requiredLongField(json, "clientCreatedAt", "Message"),
        clientMsgId = requiredStringField(json, "clientMsgId", "Message"),
        content = content,
        conversationId = requiredStringField(json, "conversationId", "Message"),
        conversationSeq = requiredLongField(json, "conversationSeq", "Message"),
        conversationType = requiredLongField(json, "conversationType", "Message").toInt(),
        createdAt = requiredLongField(json, "createdAt", "Message"),
        extensions = bytesMap(field(json, "extensions")),
        isEdited = requiredBooleanField(json, "isEdited", "Message"),
        isRead = requiredBooleanField(json, "isRead", "Message"),
        isRecalled = requiredBooleanField(json, "isRecalled", "Message"),
        localState = messageLocalStateFromJson(field(json, "localState")),
        mentionAll = requiredBooleanField(json, "mentionAll", "Message"),
        mentionUsers = requiredStringList(field(json, "mentionUsers"), "mentionUsers", "Message"),
        messageType = requiredLongField(json, "messageType", "Message").toInt(),
        quotePreview = field(json, "quotePreview")?.toString(),
        reactions = requiredListOfMaps(field(json, "reactions"), "reactions", "Message").map(::reactionEntryFromJson),
        replyTo = field(json, "replyTo")?.toString(),
        threadId = field(json, "threadId")?.toString(),
        sender = displayNameFromJson(field(json, "sender")),
        senderAvatar = field(json, "senderAvatar")?.let(::stringValue).orEmpty(),
        senderDisplayName = field(json, "senderDisplayName")?.let(::stringValue).orEmpty(),
        senderId = requiredStringField(json, "senderId", "Message"),
        senderName = field(json, "senderName")?.let(::stringValue).orEmpty(),
        serverId = field(json, "serverId")?.let(::stringValue).orEmpty(),
        source = requiredLongField(json, "source", "Message").toInt(),
        status = requiredLongField(json, "status", "Message").toInt(),
        textPreview = field(json, "textPreview")?.let(::stringValue).orEmpty(),
        updatedAt = requiredLongField(json, "updatedAt", "Message"),
        version = requiredLongField(json, "version", "Message"),
        timelineKey = field(json, "timelineKey")?.let(::stringValue).orEmpty(),
        timelineSortTs = requiredLongField(json, "timelineSortTs", "Message"),
        receipt = messageReceiptViewFromJson(field(json, "receipt")),
    )
}

fun sendAckFromJson(value: Any?): SendMessageResponse {
    val json = value as? Map<String, Any?> ?: emptyMap()
    return SendMessageResponse(
        ackId = requiredStringField(json, "ackId", "SendMessageResponse"),
        serverId = field(json, "serverId")?.let(::stringValue).orEmpty(),
        clientMsgId = requiredStringField(json, "clientMsgId", "SendMessageResponse"),
        conversationId = requiredStringField(json, "conversationId", "SendMessageResponse"),
        seq = requiredLongField(json, "seq", "SendMessageResponse"),
        timestamp = requiredLongField(json, "timestamp", "SendMessageResponse"),
        success = requiredBooleanField(json, "success", "SendMessageResponse"),
        errorCode = requiredLongField(json, "errorCode", "SendMessageResponse").toInt(),
        errorMessage = stringValue(field(json, "errorMessage")),
    )
}

fun conversationVersionFromJson(value: Any?): ConversationVersion {
    val json = mapValue(value)
    return ConversationVersion(
        conversationId = requiredStringField(json, "conversationId", "ConversationVersion"),
        version = requiredLongField(json, "version", "ConversationVersion"),
    )
}

fun syncConversationSummariesResponseFromJson(value: Any?): SyncConversationSummariesResponse {
    val json = mapValue(value)
    return SyncConversationSummariesResponse(
        changedConversations = requiredListOfMaps(
            field(json, "changedConversations"),
            "changedConversations",
            "SyncConversationSummariesResponse",
        ).map(::conversationVersionFromJson),
    )
}

fun listConversationsResponseFromJson(value: Any?): ListConversationsResponse {
    val json = value as? Map<String, Any?> ?: emptyMap()
    return ListConversationsResponse(
        conversations = requiredListOfMaps(
            field(json, "conversations"),
            "conversations",
            "ListConversationsResponse",
        ).map(::conversationFromJson),
    )
}

fun homeTimelineSnapshotFromJson(value: Any?): HomeTimelineSnapshot {
    val json = mapValue(value)
    return HomeTimelineSnapshot(
        conversations = requiredListOfMaps(
            field(json, "conversations"),
            "conversations",
            "HomeTimelineSnapshot",
        ).map(::conversationFromJson),
        syncState = timelineSyncStateFromJson(field(json, "syncState")),
        totalUnread = requiredLongField(json, "totalUnread", "HomeTimelineSnapshot"),
    )
}

fun conversationTimelineSnapshotFromJson(value: Any?): ConversationTimelineSnapshot {
    val json = mapValue(value)
    val conversationRaw = field(json, "conversation")
    return ConversationTimelineSnapshot(
        conversation = if (conversationRaw is Map<*, *>) conversationFromJson(mapValue(conversationRaw)) else null,
        hasMore = requiredBooleanField(json, "hasMore", "ConversationTimelineSnapshot"),
        messages = requiredListOfMaps(
            field(json, "messages"),
            "messages",
            "ConversationTimelineSnapshot",
        ).map(::messageFromJson),
    )
}

fun conversationFromJson(json: Map<String, Any?>): Conversation {
    val typeRaw = field(json, "conversationType")
    val rawType = if (typeRaw is String) typeRaw.trim() else ""
    val conversationType = when (rawType) {
        "unspecified" -> ConversationType.UNSPECIFIED
        "single" -> ConversationType.SINGLE
        "group" -> ConversationType.GROUP
        "ai" -> ConversationType.AI
        "system" -> ConversationType.SYSTEM
        "customer" -> ConversationType.CUSTOMER
        "temp" -> ConversationType.TEMP
        "channel" -> ConversationType.CHANNEL
        "broadcast" -> ConversationType.BROADCAST
        else -> throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "invalid conversation type: ${rawType.ifEmpty { "<empty>" }}",
            operation = "wire.conversation.decode",
            details = mapOf("field" to "conversationType"),
        )
    }
    return Conversation(
        avatarUrl = field(json, "avatarUrl")?.let(::stringValue).orEmpty(),
        badge = field(json, "badge")?.toString(),
        businessType = field(json, "businessType")?.let(::stringValue).orEmpty(),
        channelId = requiredStringField(json, "channelId", "Conversation"),
        conversationId = requiredStringField(json, "conversationId", "Conversation"),
        conversationType = conversationType,
        createdAt = requiredLongField(json, "createdAt", "Conversation"),
        description = field(json, "description")?.toString(),
        displayName = field(json, "displayName")?.let(::stringValue).orEmpty(),
        draft = field(json, "draft")?.toString(),
        ext = requiredStringMap(field(json, "ext"), "ext", "Conversation"),
        isArchived = requiredBooleanField(json, "isArchived", "Conversation"),
        isMuted = requiredBooleanField(json, "isMuted", "Conversation"),
        isPinned = requiredBooleanField(json, "isPinned", "Conversation"),
        lastMessage = messagePreviewFromJson(field(json, "lastMessage")),
        lastMessageAt = field(json, "lastMessageAt")?.let { intValue(it) },
        lastMessageId = field(json, "lastMessageId")?.toString(),
        lastMessagePreview = field(json, "lastMessagePreview")?.toString(),
        lastReadSeq = requiredLongField(json, "lastReadSeq", "Conversation"),
        lastSenderAvatarUrl = field(json, "lastSenderAvatarUrl")?.let(::stringValue).orEmpty(),
        lastSenderId = field(json, "lastSenderId")?.toString(),
        lastSenderNickname = field(json, "lastSenderNickname")?.let(::stringValue).orEmpty(),
        lastSender = displayNameFromJson(field(json, "lastSender")),
        maxSeq = requiredLongField(json, "maxSeq", "Conversation"),
        memberPreview = requiredListOfMaps(field(json, "memberPreview"), "memberPreview", "Conversation").map(::conversationParticipantFromJson),
        membersCount = requiredLongField(json, "membersCount", "Conversation").toInt(),
        mentionCount = requiredLongField(json, "mentionCount", "Conversation").toInt(),
        mentionMe = requiredBooleanField(json, "mentionMe", "Conversation"),
        participantVersion = requiredLongField(json, "participantVersion", "Conversation"),
        participants = requiredListOfMaps(field(json, "participants"), "participants", "Conversation").map(::conversationParticipantFromJson),
        peerReadSeq = requiredLongField(json, "peerReadSeq", "Conversation"),
        remark = field(json, "remark")?.toString(),
        role = field(json, "role")?.toString(),
        title = displayNameFromJson(field(json, "title")),
        unreadCount = requiredLongField(json, "unreadCount", "Conversation").toInt(),
        updatedAt = requiredLongField(json, "updatedAt", "Conversation"),
        updatedAtTs = field(json, "updatedAtTs")?.let { intValue(it) },
        version = requiredLongField(json, "version", "Conversation"),
        visibleAfterSeq = requiredLongField(json, "visibleAfterSeq", "Conversation"),
    )
}

fun listMessagesResponseFromJson(value: Any?): ListMessagesResponse {
    val json = value as? Map<String, Any?> ?: emptyMap()
    return ListMessagesResponse(
        messages = requiredListOfMaps(
            field(json, "messages"),
            "messages",
            "ListMessagesResponse",
        ).map(::messageFromJson),
    )
}

fun viewSnapshotFromJson(value: Any?): ViewSnapshot {
    val json = mapValue(value)
    return ViewSnapshot(
        viewType = viewTypeFromJson(field(json, "viewType"), "ViewSnapshot"),
        data = mapValue(field(json, "data")),
    )
}

fun viewTypeFromJson(value: Any?, context: String): String {
    val raw = if (value is String) value.trim() else ""
    return when (raw) {
        "timeline", "conversationList" -> raw
        else -> throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "invalid view type: ${raw.ifEmpty { "<empty>" }}",
            operation = "wire.view.decode",
            details = mapOf("field" to "$context.viewType"),
        )
    }
}

fun viewOpenResponseFromJson(value: Any?): ViewOpenResponse {
    val json = mapValue(value)
    return ViewOpenResponse(
        viewId = requiredStringField(json, "viewId", "ViewOpenResponse"),
        snapshot = viewSnapshotFromJson(field(json, "snapshot")),
    )
}

fun viewDeltaOpKindFromJson(value: Any?): String {
    val raw = if (value is String) value.trim() else ""
    return when (raw) {
        "insert", "update", "remove", "move" -> raw
        else -> throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "invalid view delta op: ${raw.ifEmpty { "<empty>" }}",
            operation = "wire.view.decode",
            details = mapOf("field" to "ViewDeltaOp.op"),
        )
    }
}

fun viewDeltaOpFromJson(value: Any?): ViewDeltaOp {
    val json = mapValue(value)
    val fromIndex = field(json, "fromIndex")
    val item = field(json, "item")
    if (item != null && item !is Map<*, *>) {
        throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "ViewDeltaOp.item must be an object",
            operation = "wire.view.decode",
            details = mapOf("field" to "ViewDeltaOp.item", "expected" to "object"),
        )
    }
    return ViewDeltaOp(
        op = viewDeltaOpKindFromJson(field(json, "op")),
        key = requiredStringField(json, "key", "ViewDeltaOp"),
        index = requiredLongField(json, "index", "ViewDeltaOp").toInt(),
        fromIndex = fromIndex?.let { requiredLongField(json, "fromIndex", "ViewDeltaOp").toInt() },
        item = item?.let(::mapValue),
    )
}

fun viewDeltaFromJson(value: Any?): ViewDelta {
    val json = mapValue(value)
    val conversation = field(json, "conversation")
    val hasMore = field(json, "hasMore")
    val totalUnread = field(json, "totalUnread")
    val syncState = field(json, "syncState")
    return ViewDelta(
        viewType = viewTypeFromJson(field(json, "viewType"), "ViewDelta"),
        ops = requiredListOfMaps(
            field(json, "ops"),
            "ops",
            "ViewDelta",
        ).map(::viewDeltaOpFromJson),
        conversation = conversation?.let { conversationFromJson(mapValue(it)) },
        hasMore = hasMore as? Boolean,
        totalUnread = totalUnread?.let(::intValue),
        syncState = syncState?.let(::stringValue),
    )
}

fun viewUpdateFromJson(value: Any?): ViewUpdate {
    val json = mapValue(value)
    val kind = when (val raw = stringValue(field(json, "kind"))) {
        "snapshot", "delta" -> raw
        else -> throw FlareSdkException(
            SdkErrorCodes.INVALIDPARAMETER,
            "invalid view update kind: ${if (raw.isEmpty()) "<empty>" else raw}",
            operation = "event.decode",
            details = mapOf("field" to "kind"),
        )
    }
    return ViewUpdate(
        viewId = requiredStringField(json, "viewId", "ViewUpdate"),
        kind = kind,
        snapshot = if (kind == "delta") null else viewSnapshotFromJson(field(json, "snapshot")),
        delta = if (kind == "delta") viewDeltaFromJson(field(json, "delta")) else null,
    )
}

fun viewLoadOlderResponseFromJson(value: Any?): ViewLoadOlderResponse {
    val json = mapValue(value)
    val update = field(json, "update")
    return ViewLoadOlderResponse(
        viewId = requiredStringField(json, "viewId", "ViewLoadOlderResponse"),
        loadedCount = requiredLongField(json, "loadedCount", "ViewLoadOlderResponse").toInt(),
        hasMore = requiredBooleanField(json, "hasMore", "ViewLoadOlderResponse"),
        update = update?.let(::viewUpdateFromJson),
    )
}

fun closeViewResponseFromJson(value: Any?): CloseViewResponse {
    val json = mapValue(value)
    return CloseViewResponse(closed = requiredBooleanField(json, "closed", "CloseViewResponse"))
}

// RUST-OWNED WIRE BOUNDARY: BEGIN
/** The FFI wire contract is canonical camelCase SDK JSON. */
fun wireEncodeRequest(value: Any?): Any? = value

/** The FFI wire contract is canonical camelCase SDK JSON. */
fun wireDecodeResponse(value: Any?): Any? = value
// RUST-OWNED WIRE BOUNDARY: END
