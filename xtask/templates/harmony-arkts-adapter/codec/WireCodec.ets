// GENERATED. Do not edit by hand.
import {
  PluginUiEvaluationRequest,
  PluginCommandRequest,
  CloseViewRequest,
  CloseViewResponse,
  ConversationVersion,
  ConversationParticipant,
  Conversation,
  ConversationType,
  ListConversationsResponse,
  ListMessagesResponse,
  LoadNewerTimelineViewRequest,
  LoadOlderTimelineViewRequest,
  Message,
  MessageContent,
  MessageContentType,
  MessageLocalState,
  MessagePreview,
  MessageReadReceiptMembersRequest,
  MessageReadSummary,
  MessageReceiptStatus,
  MessageReceiptView,
  NetworkChangeRequest,
  NetworkChangeResponse,
  OpenConversationListViewRequest,
  OpenTimelineViewRequest,
  ReactionEntry,
  SendMessageRequest,
  SendMessageResponse,
  ServerClockResponse,
  SyncConversationSummariesResponse,
  UpdateConversationDraftRequest,
  TimelineViewPageResponse,
  TimelineViewRequest,
  TimelineViewSnapshot,
  ViewDelta,
  ViewDeltaOp,
  ViewOpenResponse,
  ViewSnapshot,
  ViewUpdate,
} from '../model';

export function pluginUiEvaluationRequestToMap(request: PluginUiEvaluationRequest): Record<string, Object> {
  return { payload: request.payload };
}

export function messageReadReceiptMembersRequestToMap(request: MessageReadReceiptMembersRequest): Record<string, Object> {
  return { payload: request.payload };
}

export function pluginCommandRequestToMap(request: PluginCommandRequest): Record<string, Object> {
  return { payload: request.payload };
}

export function enumWireIndex<T>(order: ReadonlyArray<T>, value: T): number {
  const index = order.indexOf(value);
  return index >= 0 ? index : 0;
}

export function conversationTypeWireValue(value: ConversationType): string {
  return value;
}

export function timelineSyncStateWireValue(value: TimelineSyncState): string {
  return value;
}

export function networkChangeRequestToMap(request: NetworkChangeRequest): Record<string, Object> {
  return {
    ...(request.available !== undefined ? { available: request.available } : {}),
    ...(request.interface !== undefined ? { interface: request.interface } : {}),
    ...(request.expensive !== undefined ? { expensive: request.expensive } : {}),
    ...(request.metered !== undefined ? { metered: request.metered } : {}),
    ...(request.reason !== undefined ? { reason: request.reason } : {}),
  };
}

export function updateConversationDraftRequestToMap(request: UpdateConversationDraftRequest): Record<string, Object> {
  return {
    conversationId: request.conversationId,
    ...(request.draft !== undefined ? { draft: request.draft } : {}),
  };
}



const HEARTBEAT_APP_STATE_WIRE_ORDER: HeartbeatAppState[] = [HeartbeatAppState.Foreground, HeartbeatAppState.Background];
const CONVERSATION_TYPE_WIRE_VALUES: ConversationType[] = [ConversationType.Unspecified, ConversationType.Single, ConversationType.Group, ConversationType.Ai, ConversationType.System, ConversationType.Customer, ConversationType.Temp, ConversationType.Channel, ConversationType.Broadcast];
const MESSAGE_CONTENT_TYPE_WIRE_VALUES: MessageContentType[] = [MessageContentType.Text, MessageContentType.Image, MessageContentType.Video, MessageContentType.Audio, MessageContentType.File, MessageContentType.Location, MessageContentType.Card, MessageContentType.Sticker, MessageContentType.Emoji, MessageContentType.Quote, MessageContentType.LinkCard, MessageContentType.Forward, MessageContentType.Thread, MessageContentType.MiniProgram, MessageContentType.RichText, MessageContentType.ImageGroup, MessageContentType.System, MessageContentType.Notification, MessageContentType.Vote, MessageContentType.Task, MessageContentType.Schedule, MessageContentType.Announcement, MessageContentType.Custom, MessageContentType.Placeholder];
const MESSAGE_SEARCH_KIND_WIRE_ORDER: MessageSearchKind[] = [MessageSearchKind.Message, MessageSearchKind.Text, MessageSearchKind.Media, MessageSearchKind.Image, MessageSearchKind.Video, MessageSearchKind.Audio, MessageSearchKind.File];
const SDK_EVENT_KIND_WIRE_ORDER: SdkEventKind[] = [SdkEventKind.Lifecycle, SdkEventKind.Connection, SdkEventKind.Message, SdkEventKind.Notification, SdkEventKind.Conversation, SdkEventKind.Sync, SdkEventKind.Extension, SdkEventKind.ExtensionEvent, SdkEventKind.Presence, SdkEventKind.Media, SdkEventKind.Plugin];
const LIFECYCLE_EVENT_NAME_WIRE_ORDER: LifecycleEventName[] = [LifecycleEventName.Initializing, LifecycleEventName.Initialized, LifecycleEventName.InitFailed, LifecycleEventName.LoginSucceeded, LifecycleEventName.LoginFailed, LifecycleEventName.LoggedOut, LifecycleEventName.Disposed];
const SDK_CONNECTION_STATE_WIRE_ORDER: SdkConnectionState[] = [SdkConnectionState.Disconnected, SdkConnectionState.Connecting, SdkConnectionState.Connected, SdkConnectionState.Ready, SdkConnectionState.Reconnecting];
const CONNECTION_EVENT_NAME_WIRE_ORDER: ConnectionEventName[] = [ConnectionEventName.Connecting, ConnectionEventName.Connected, ConnectionEventName.Ready, ConnectionEventName.Disconnected, ConnectionEventName.Reconnecting, ConnectionEventName.ReconnectFailed, ConnectionEventName.StateChanged, ConnectionEventName.SyncStateChanged, ConnectionEventName.ServerError, ConnectionEventName.KickedOff, ConnectionEventName.TokenExpired];
const MESSAGE_EVENT_NAME_WIRE_ORDER: MessageEventName[] = [MessageEventName.Received, MessageEventName.ReceivedBatch, MessageEventName.SendAck, MessageEventName.SendFailed, MessageEventName.Recalled, MessageEventName.Typing, MessageEventName.Edited, MessageEventName.ReactionChanged, MessageEventName.Deleted, MessageEventName.ReadReceipt, MessageEventName.BurnScheduled, MessageEventName.Burned, MessageEventName.HardDeleted, MessageEventName.Pinned, MessageEventName.Unpinned, MessageEventName.Marked, MessageEventName.Unmarked, MessageEventName.RetentionScheduled, MessageEventName.RetentionExpired, MessageEventName.RetentionPurged, MessageEventName.PresenceChanged, MessageEventName.Custom, MessageEventName.PluginStateChanged, MessageEventName.ReadSummariesChanged];
const CONVERSATION_EVENT_NAME_WIRE_ORDER: ConversationEventName[] = [ConversationEventName.Synced, ConversationEventName.Created, ConversationEventName.Updated, ConversationEventName.UnreadCountChanged, ConversationEventName.Deleted];
const SYNC_EVENT_NAME_WIRE_ORDER: SyncEventName[] = [SyncEventName.StateChanged, SyncEventName.Started, SyncEventName.Finished, SyncEventName.Failed, SyncEventName.Progress, SyncEventName.TaskCompleted, SyncEventName.ResyncNeeded];
const PROGRESS_EVENT_NAME_WIRE_ORDER: ProgressEventName[] = [ProgressEventName.SyncProgress, ProgressEventName.UploadProgress, ProgressEventName.DownloadProgress];



export function setHeartbeatAppStateRequestToMap(request: SetHeartbeatAppStateRequest): Record<string, Object> {
  return {
  appState: enumWireIndex(HEARTBEAT_APP_STATE_WIRE_ORDER, request.appState),
  };
}

export function setHeartbeatNatTimeoutRequestToMap(request: SetHeartbeatNatTimeoutRequest): Record<string, Object> {
  return {
  ...(request.natTimeoutSecs !== undefined ? { natTimeoutSecs: request.natTimeoutSecs } : {}),
  };
}

export function heartbeatEffectiveIntervalResponseToMap(request: HeartbeatEffectiveIntervalResponse): Record<string, Object> {
  return {
  connected: request.connected,
  ...(request.intervalMs !== undefined ? { intervalMs: request.intervalMs } : {}),
  ...(request.intervalSecs !== undefined ? { intervalSecs: request.intervalSecs } : {}),
  };
}

export function conversationParticipantToMap(request: ConversationParticipant): Record<string, Object> {
  return {
  userId: request.userId,
  ...(request.roles.length > 0 ? { roles: request.roles } : {}),
  muted: request.muted,
  pinned: request.pinned,
  attributes: request.attributes,
  joinedAt: request.joinedAt,
  nickname: request.nickname,
  };
}

export function messagePreviewToMap(request: MessagePreview): Record<string, Object> {
  return {
  messageId: request.messageId,
  senderId: request.senderId,
  type: request.type,
  text: request.text,
  time: request.time,
  };
}

export function conversationToMap(request: Conversation): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  conversationType: conversationTypeWireValue(request.conversationType),
  businessType: request.businessType,
  channelId: request.channelId,
  membersCount: request.membersCount,
  displayName: request.displayName,
  avatarUrl: request.avatarUrl,
  ...(request.remark !== undefined ? { remark: request.remark } : {}),
  ...(request.description !== undefined ? { description: request.description } : {}),
  ...(request.lastMessageId !== undefined ? { lastMessageId: request.lastMessageId } : {}),
  ...(request.lastSenderId !== undefined ? { lastSenderId: request.lastSenderId } : {}),
  ...(request.lastMessageAt !== undefined ? { lastMessageAt: request.lastMessageAt } : {}),
  ...(request.lastMessagePreview !== undefined ? { lastMessagePreview: request.lastMessagePreview } : {}),
  ...(request.lastMessage !== undefined ? { lastMessage: messagePreviewToMap(request.lastMessage) } : {}),
  lastSenderNickname: request.lastSenderNickname,
  lastSenderAvatarUrl: request.lastSenderAvatarUrl,
  unreadCount: request.unreadCount,
  lastReadSeq: request.lastReadSeq,
  peerReadSeq: request.peerReadSeq,
  maxSeq: request.maxSeq,
  visibleAfterSeq: request.visibleAfterSeq,
  isPinned: request.isPinned,
  isMuted: request.isMuted,
  isArchived: request.isArchived,
  version: request.version,
  updatedAt: request.updatedAt,
  createdAt: request.createdAt,
  ...(request.updatedAtTs !== undefined ? { updatedAtTs: request.updatedAtTs } : {}),
  ext: request.ext,
  participantVersion: request.participantVersion,
  ...(request.memberPreview.length > 0 ? { memberPreview: request.memberPreview.map((item) => conversationParticipantToMap(item)) } : {}),
  ...(request.draft !== undefined ? { draft: request.draft } : {}),
  mentionCount: request.mentionCount,
  mentionMe: request.mentionMe,
  ...(request.badge !== undefined ? { badge: request.badge } : {}),
  ...(request.role !== undefined ? { role: request.role } : {}),
  ...(request.participants.length > 0 ? { participants: request.participants.map((item) => conversationParticipantToMap(item)) } : {}),
  };
}

export function conversationListQueryToMap(request: ConversationListQuery): Record<string, Object> {
  return {
  ...(request.keyword !== undefined ? { keyword: request.keyword } : {}),
  includeArchived: request.includeArchived,
  unreadOnly: request.unreadOnly,
  mentionMeOnly: request.mentionMeOnly,
  pinnedOnly: request.pinnedOnly,
  mutedOnly: request.mutedOnly,
  hasDraftOnly: request.hasDraftOnly,
  hasMarkedMessages: request.hasMarkedMessages,
  ...(request.conversationTypes.length > 0 ? { conversationTypes: request.conversationTypes.map((v) => conversationTypeWireValue(v)) } : {}),
  ...(request.cursor !== undefined ? { cursor: request.cursor } : {}),
  ...(request.limit !== undefined ? { limit: request.limit } : {}),
  };
}

export function listConversationsResponseToMap(request: ListConversationsResponse): Record<string, Object> {
  return {
  ...(request.conversations.length > 0 ? { conversations: request.conversations.map((item) => conversationToMap(item)) } : {}),
  };
}

export function bootstrapHomeTimelineRequestToMap(request: BootstrapHomeTimelineRequest): Record<string, Object> {
  return {
  conversationLimit: request.conversationLimit,
  };
}

export function openConversationTimelineRequestToMap(request: OpenConversationTimelineRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  messageLimit: request.messageLimit,
  };
}

export function openTimelineViewRequestToMap(request: OpenTimelineViewRequest): Record<string, Object> {
  const out: Record<string, Object> = {
  conversationId: request.conversationId,
  messageLimit: request.messageLimit,
  };
  if (request.anchor !== undefined) {
    const anchor: Record<string, Object> = {};
    if (request.anchor.messageId !== undefined) {
      anchor['messageId'] = request.anchor.messageId;
    }
    if (request.anchor.seq !== undefined) {
      anchor['seq'] = request.anchor.seq;
    }
    out['anchor'] = anchor;
  }
  return out;
}

export function loadOlderTimelineViewRequestToMap(request: LoadOlderTimelineViewRequest): Record<string, Object> {
  return {
  viewId: request.viewId,
  messageLimit: request.messageLimit,
  };
}

export function loadNewerTimelineViewRequestToMap(request: LoadNewerTimelineViewRequest): Record<string, Object> {
  return {
  viewId: request.viewId,
  messageLimit: request.messageLimit,
  };
}

export function timelineViewRequestToMap(request: TimelineViewRequest): Record<string, Object> {
  return {
  viewId: request.viewId,
  };
}

export function openConversationListViewRequestToMap(request: OpenConversationListViewRequest): Record<string, Object> {
  return {
  conversationLimit: request.conversationLimit,
  };
}

export function closeViewRequestToMap(request: CloseViewRequest): Record<string, Object> {
  return {
  viewId: request.viewId,
  };
}

export function homeTimelineSnapshotToMap(request: HomeTimelineSnapshot): Record<string, Object> {
  return {
  ...(request.conversations.length > 0 ? { conversations: request.conversations.map((item) => conversationToMap(item)) } : {}),
  totalUnread: request.totalUnread,
  syncState: timelineSyncStateWireValue(request.syncState),
  };
}

export function conversationTimelineSnapshotToMap(request: ConversationTimelineSnapshot): Record<string, Object> {
  return {
  ...(request.conversation !== undefined ? { conversation: conversationToMap(request.conversation) } : {}),
  ...(request.messages.length > 0 ? { messages: request.messages.map((item) => messageToMap(item)) } : {}),
  hasMore: request.hasMore,
  };
}

export function conversationVersionToMap(request: ConversationVersion): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  version: request.version,
  };
}

export function syncConversationSummariesRequestToMap(request: SyncConversationSummariesRequest): Record<string, Object> {
  return {
  ...(request.knownVersions.length > 0 ? { knownVersions: request.knownVersions.map((item) => conversationVersionToMap(item)) } : {}),
  };
}

export function syncConversationSummariesResponseToMap(request: SyncConversationSummariesResponse): Record<string, Object> {
  return {
  ...(request.changedConversations.length > 0 ? { changedConversations: request.changedConversations.map((item) => conversationVersionToMap(item)) } : {}),
  };
}

export function reactionEntryToMap(request: ReactionEntry): Record<string, Object> {
  return {
  emoji: request.emoji,
  ...(request.userIds.length > 0 ? { userIds: request.userIds } : {}),
  count: request.count,
  };
}

export function messageLocalStateToMap(request: MessageLocalState): Record<string, Object> {
  return {
  sending: request.sending,
  failed: request.failed,
  isLocal: request.isLocal,
  sortTs: request.sortTs,
  };
}

export function messageContentToMap(request: MessageContent): Record<string, Object> {
  return {
  contentType: request.contentType,
  data: request.data,
  };
}

export function createTextMessageRequestToMap(request: CreateTextMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  text: request.text,
  };
}

export function sendMessageRequestToMap(request: SendMessageRequest): Record<string, Object> {
  return {
  message: messageToMap(request.message),
  };
}

export function sendMessageResponseToMap(request: SendMessageResponse): Record<string, Object> {
  return {
  serverId: request.serverId,
  clientMsgId: request.clientMsgId,
  conversationId: request.conversationId,
  seq: request.seq,
  timestamp: request.timestamp,
  };
}

export function listMessagesRequestToMap(request: ListMessagesRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  beforeSeq: request.beforeSeq,
  limit: request.limit,
  };
}

export function listMessagesResponseToMap(request: ListMessagesResponse): Record<string, Object> {
  return {
  ...(request.messages.length > 0 ? { messages: request.messages.map((item) => messageToMap(item)) } : {}),
  };
}

export function messageSearchQueryToMap(request: MessageSearchQuery): Record<string, Object> {
  return {
  ...(request.keyword !== undefined ? { keyword: request.keyword } : {}),
  ...(request.conversationId !== undefined ? { conversationId: request.conversationId } : {}),
  ...(request.senderId !== undefined ? { senderId: request.senderId } : {}),
  ...(request.fromTime !== undefined ? { fromTime: request.fromTime } : {}),
  ...(request.toTime !== undefined ? { toTime: request.toTime } : {}),
  ...(request.kinds.length > 0 ? { kinds: request.kinds.map((v) => enumWireIndex(MESSAGE_SEARCH_KIND_WIRE_ORDER, v)) } : {}),
  limit: request.limit,
  includeRecalled: request.includeRecalled,
  };
}

export function mediaSourceInfoToMap(request: MediaSourceInfo): Record<string, Object> {
  return {
  ...(request.uuid !== undefined ? { uuid: request.uuid } : {}),
  ...(request.imageId !== undefined ? { imageId: request.imageId } : {}),
  ...(request.url !== undefined ? { url: request.url } : {}),
  ...(request.mimeType !== undefined ? { mimeType: request.mimeType } : {}),
  ...(request.size !== undefined ? { size: request.size } : {}),
  ...(request.width !== undefined ? { width: request.width } : {}),
  ...(request.height !== undefined ? { height: request.height } : {}),
  ...(request.durationMs !== undefined ? { durationMs: request.durationMs } : {}),
  };
}

export function textContentPayloadToMap(request: TextContentPayload): Record<string, Object> {
  return {
  text: request.text,
  };
}

export function imageContentPayloadToMap(request: ImageContentPayload): Record<string, Object> {
  return {
  ...(request.imageId !== undefined ? { imageId: request.imageId } : {}),
  ...(request.source !== undefined ? { source: mediaSourceInfoToMap(request.source) } : {}),
  ...(request.thumbnail !== undefined ? { thumbnail: mediaSourceInfoToMap(request.thumbnail) } : {}),
  ...(request.description !== undefined ? { description: request.description } : {}),
  };
}

export function imageGroupItemToMap(request: ImageGroupItem): Record<string, Object> {
  return {
  imageId: request.imageId,
  ...(request.url !== undefined ? { url: request.url } : {}),
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.width !== undefined ? { width: request.width } : {}),
  ...(request.height !== undefined ? { height: request.height } : {}),
  };
}

export function imageGroupContentPayloadToMap(request: ImageGroupContentPayload): Record<string, Object> {
  return {
  ...(request.images.length > 0 ? { images: request.images.map((item) => imageGroupItemToMap(item)) } : {}),
  ...(request.title !== undefined ? { title: request.title } : {}),
  };
}

export function videoContentPayloadToMap(request: VideoContentPayload): Record<string, Object> {
  return {
  ...(request.videoId !== undefined ? { videoId: request.videoId } : {}),
  ...(request.source !== undefined ? { source: mediaSourceInfoToMap(request.source) } : {}),
  ...(request.cover !== undefined ? { cover: mediaSourceInfoToMap(request.cover) } : {}),
  ...(request.description !== undefined ? { description: request.description } : {}),
  };
}

export function audioContentPayloadToMap(request: AudioContentPayload): Record<string, Object> {
  return {
  ...(request.audioId !== undefined ? { audioId: request.audioId } : {}),
  ...(request.source !== undefined ? { source: mediaSourceInfoToMap(request.source) } : {}),
  ...(request.durationMs !== undefined ? { durationMs: request.durationMs } : {}),
  };
}

export function fileContentPayloadToMap(request: FileContentPayload): Record<string, Object> {
  return {
  ...(request.fileId !== undefined ? { fileId: request.fileId } : {}),
  ...(request.name !== undefined ? { name: request.name } : {}),
  ...(request.url !== undefined ? { url: request.url } : {}),
  ...(request.mimeType !== undefined ? { mimeType: request.mimeType } : {}),
  ...(request.size !== undefined ? { size: request.size } : {}),
  };
}

export function emojiContentPayloadToMap(request: EmojiContentPayload): Record<string, Object> {
  return {
  emoji: request.emoji,
  };
}

export function stickerContentPayloadToMap(request: StickerContentPayload): Record<string, Object> {
  return {
  stickerId: request.stickerId,
  ...(request.packageId !== undefined ? { packageId: request.packageId } : {}),
  ...(request.url !== undefined ? { url: request.url } : {}),
  ...(request.width !== undefined ? { width: request.width } : {}),
  ...(request.height !== undefined ? { height: request.height } : {}),
  ...(request.format !== undefined ? { format: request.format } : {}),
  };
}

export function forwardSourceMessageToMap(request: ForwardSourceMessage): Record<string, Object> {
  return {
  sourceMessageId: request.sourceMessageId,
  ...(request.sourceConversationId !== undefined ? { sourceConversationId: request.sourceConversationId } : {}),
  ...(request.sourceSenderId !== undefined ? { sourceSenderId: request.sourceSenderId } : {}),
  ...(request.plainText !== undefined ? { plainText: request.plainText } : {}),
  };
}

export function forwardContentPayloadToMap(request: ForwardContentPayload): Record<string, Object> {
  return {
  merge: request.merge,
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.sourceMessages.length > 0 ? { sourceMessages: request.sourceMessages.map((item) => forwardSourceMessageToMap(item)) } : {}),
  };
}

export function messageBuildCatalogEntryToMap(request: MessageBuildCatalogEntry): Record<string, Object> {
  return {
  op: request.op,
  method: request.method,
  requestType: request.requestType,
  contentType: request.contentType,
  messageType: request.messageType,
  summary: request.summary,
  stability: request.stability,
  };
}

export function listMessageBuildCatalogResponseToMap(request: ListMessageBuildCatalogResponse): Record<string, Object> {
  return {
  ...(request.entries.length > 0 ? { entries: request.entries.map((item) => messageBuildCatalogEntryToMap(item)) } : {}),
  };
}

export function buildTypedMessageRequestToMap(request: BuildTypedMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  op: request.op,
  data: request.data,
  };
}

export function buildTextMessageRequestToMap(request: BuildTextMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  text: request.text,
  };
}

export function buildQuoteMessageRequestToMap(request: BuildQuoteMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  quotedMessageId: request.quotedMessageId,
  text: request.text,
  ...(request.quotedSenderId !== undefined ? { quotedSenderId: request.quotedSenderId } : {}),
  ...(request.quotedTextPreview !== undefined ? { quotedTextPreview: request.quotedTextPreview } : {}),
  };
}

export function buildThreadReplyMessageRequestToMap(request: BuildThreadReplyMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  threadId: request.threadId,
  text: request.text,
  };
}

export function buildForwardMessageRequestToMap(request: BuildForwardMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  merge: request.merge,
  title: request.title,
  ...(request.sourceMessages.length > 0 ? { sourceMessages: request.sourceMessages.map((item) => messageToMap(item)) } : {}),
  };
}

export function buildImageMessageRequestToMap(request: BuildImageMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  imageId: request.imageId,
  ...(request.payload !== undefined ? { payload: imageContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildImageGroupMessageRequestToMap(request: BuildImageGroupMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  payload: imageGroupContentPayloadToMap(request.payload),
  };
}

export function buildVideoMessageRequestToMap(request: BuildVideoMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  videoId: request.videoId,
  ...(request.payload !== undefined ? { payload: videoContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildAudioMessageRequestToMap(request: BuildAudioMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  audioId: request.audioId,
  ...(request.payload !== undefined ? { payload: audioContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildFileMessageRequestToMap(request: BuildFileMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  fileId: request.fileId,
  ...(request.payload !== undefined ? { payload: fileContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildEmojiMessageRequestToMap(request: BuildEmojiMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  emoji: request.emoji,
  };
}

export function buildLocationMessageRequestToMap(request: BuildLocationMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  latitude: request.latitude,
  longitude: request.longitude,
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.address !== undefined ? { address: request.address } : {}),
  };
}

export function buildStickerMessageRequestToMap(request: BuildStickerMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  stickerId: request.stickerId,
  ...(request.packageId !== undefined ? { packageId: request.packageId } : {}),
  ...(request.payload !== undefined ? { payload: stickerContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildLinkCardMessageRequestToMap(request: BuildLinkCardMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  url: request.url,
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.description !== undefined ? { description: request.description } : {}),
  };
}

export function buildCardMessageRequestToMap(request: BuildCardMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  id: request.id,
  ...(request.cardType !== undefined ? { cardType: request.cardType } : {}),
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.subtitle !== undefined ? { subtitle: request.subtitle } : {}),
  ...(request.avatar !== undefined ? { avatar: request.avatar } : {}),
  ...(request.attributes !== undefined ? { attributes: request.attributes } : {}),
  };
}

export function buildMiniProgramMessageRequestToMap(request: BuildMiniProgramMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  appId: request.appId,
  ...(request.path !== undefined ? { path: request.path } : {}),
  ...(request.title !== undefined ? { title: request.title } : {}),
  };
}

export function buildRichDocMessageRequestToMap(request: BuildRichDocMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  ...(request.docJson !== undefined ? { docJson: request.docJson } : {}),
  ...(request.markdown !== undefined ? { markdown: request.markdown } : {}),
  ...(request.html !== undefined ? { html: request.html } : {}),
  };
}

export function normalizeRichDocFromMarkdownRequestToMap(request: NormalizeRichDocFromMarkdownRequest): Record<string, Object> {
  return {
  markdown: request.markdown,
  };
}

export function normalizeRichDocFromHtmlRequestToMap(request: NormalizeRichDocFromHtmlRequest): Record<string, Object> {
  return {
  html: request.html,
  };
}

export function normalizeRichDocFromDocJsonRequestToMap(request: NormalizeRichDocFromDocJsonRequest): Record<string, Object> {
  return {
  docJson: request.docJson,
  };
}

export function richDocV2NormalizedToMap(request: RichDocV2Normalized): Record<string, Object> {
  return {
  docJson: request.docJson,
  contentSchema: request.contentSchema,
  version: request.version,
  plainText: request.plainText,
  searchText: request.searchText,
  renderHints: request.renderHints,
  ...(request.inputFormat !== undefined ? { inputFormat: request.inputFormat } : {}),
  sourcePayload: request.sourcePayload,
  };
}

export function buildSystemMessageRequestToMap(request: BuildSystemMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  text: request.text,
  };
}

export function buildNotificationMessageRequestToMap(request: BuildNotificationMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  text: request.text,
  };
}

export function buildVoteMessageRequestToMap(request: BuildVoteMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  data: request.data,
  };
}

export function buildTaskMessageRequestToMap(request: BuildTaskMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  data: request.data,
  };
}

export function buildScheduleMessageRequestToMap(request: BuildScheduleMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  data: request.data,
  };
}

export function buildAnnouncementMessageRequestToMap(request: BuildAnnouncementMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  text: request.text,
  };
}

export function buildCustomMessageRequestToMap(request: BuildCustomMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  typeKey: request.typeKey,
  data: request.data,
  };
}

export function buildPlaceholderMessageRequestToMap(request: BuildPlaceholderMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  ...(request.hint !== undefined ? { hint: request.hint } : {}),
  };
}

export function buildWithContentMessageRequestToMap(request: BuildWithContentMessageRequest): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  content: messageContentToMap(request.content),
  };
}

export function sdkErrorPayloadToMap(request: SdkErrorPayload): Record<string, Object> {
  return {
  code: request.code,
  message: request.message,
  ...(request.operation !== undefined ? { operation: request.operation } : {}),
  retryable: request.retryable,
  details: request.details,
  };
}

export function sdkEventEnvelopeToMap(request: SdkEventEnvelope): Record<string, Object> {
  return {
  eventId: request.eventId,
  kind: enumWireIndex(SDK_EVENT_KIND_WIRE_ORDER, request.kind),
  name: request.name,
  occurredAt: request.occurredAt,
  ...(request.traceId !== undefined ? { traceId: request.traceId } : {}),
  payload: request.payload,
  };
}

export function lifecycleEventToMap(request: LifecycleEvent): Record<string, Object> {
  return {
  name: enumWireIndex(LIFECYCLE_EVENT_NAME_WIRE_ORDER, request.name),
  operation: request.operation,
  ...(request.userId !== undefined ? { userId: request.userId } : {}),
  ...(request.sessionId !== undefined ? { sessionId: request.sessionId } : {}),
  ...(request.error !== undefined ? { error: sdkErrorPayloadToMap(request.error) } : {}),
  };
}

export function connectionEventToMap(request: ConnectionEvent): Record<string, Object> {
  return {
  name: enumWireIndex(CONNECTION_EVENT_NAME_WIRE_ORDER, request.name),
  state: enumWireIndex(SDK_CONNECTION_STATE_WIRE_ORDER, request.state),
  ...(request.reason !== undefined ? { reason: request.reason } : {}),
  ...(request.attempt !== undefined ? { attempt: request.attempt } : {}),
  ...(request.error !== undefined ? { error: sdkErrorPayloadToMap(request.error) } : {}),
  };
}

export function messageReceivedEventToMap(request: MessageReceivedEvent): Record<string, Object> {
  return {
  message: messageToMap(request.message),
  };
}

export function messageReceivedBatchEventToMap(request: MessageReceivedBatchEvent): Record<string, Object> {
  return {
  ...(request.messages.length > 0 ? { messages: request.messages.map((item) => messageToMap(item)) } : {}),
  };
}

export function messageSendAckEventToMap(request: MessageSendAckEvent): Record<string, Object> {
  return {
  ack: sendMessageResponseToMap(request.ack),
  };
}

export function messageSendFailedEventToMap(request: MessageSendFailedEvent): Record<string, Object> {
  return {
  clientMsgId: request.clientMsgId,
  reason: request.reason,
  ...(request.error !== undefined ? { error: sdkErrorPayloadToMap(request.error) } : {}),
  };
}

export function messageMutationEventToMap(request: MessageMutationEvent): Record<string, Object> {
  return {
  name: enumWireIndex(MESSAGE_EVENT_NAME_WIRE_ORDER, request.name),
  conversationId: request.conversationId,
  ...(request.messageId !== undefined ? { messageId: request.messageId } : {}),
  ...(request.serverMsgId !== undefined ? { serverMsgId: request.serverMsgId } : {}),
  ...(request.userId !== undefined ? { userId: request.userId } : {}),
  ...(request.reason !== undefined ? { reason: request.reason } : {}),
  };
}

export function typingEventToMap(request: TypingEvent): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  userId: request.userId,
  typing: request.typing,
  };
}

export function readReceiptEventToMap(request: ReadReceiptEvent): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  userId: request.userId,
  readSeq: request.readSeq,
  };
}

export function reactionChangedEventToMap(request: ReactionChangedEvent): Record<string, Object> {
  return {
  conversationId: request.conversationId,
  serverMsgId: request.serverMsgId,
  userId: request.userId,
  emoji: request.emoji,
  action: request.action,
  };
}

export function conversationEventToMap(request: ConversationEvent): Record<string, Object> {
  return {
  name: enumWireIndex(CONVERSATION_EVENT_NAME_WIRE_ORDER, request.name),
  ...(request.conversationId !== undefined ? { conversationId: request.conversationId } : {}),
  ...(request.conversationIds.length > 0 ? { conversationIds: request.conversationIds } : {}),
  ...(request.unreadCount !== undefined ? { unreadCount: request.unreadCount } : {}),
  };
}

export function presenceChangedEventToMap(request: PresenceChangedEvent): Record<string, Object> {
  return {
  ...(request.conversationId !== undefined ? { conversationId: request.conversationId } : {}),
  userId: request.userId,
  status: request.status,
  extra: request.extra,
  };
}

export function progressEventToMap(request: ProgressEvent): Record<string, Object> {
  return {
  name: enumWireIndex(PROGRESS_EVENT_NAME_WIRE_ORDER, request.name),
  operation: request.operation,
  current: request.current,
  total: request.total,
  ...(request.taskId !== undefined ? { taskId: request.taskId } : {}),
  ...(request.detail !== undefined ? { detail: request.detail } : {}),
  };
}

export function syncEventToMap(request: SyncEvent): Record<string, Object> {
  return {
  name: enumWireIndex(SYNC_EVENT_NAME_WIRE_ORDER, request.name),
  ...(request.trigger !== undefined ? { trigger: request.trigger } : {}),
  ...(request.phase !== undefined ? { phase: request.phase } : {}),
  ...(request.task !== undefined ? { task: request.task } : {}),
  ...(request.progress !== undefined ? { progress: request.progress } : {}),
  ...(request.error !== undefined ? { error: sdkErrorPayloadToMap(request.error) } : {}),
  };
}


export function sendMessageRequestToMap(request: SendMessageRequest): Record<string, Object> {
  const result: Record<string, Object> = { message: messageToWireMap(request.message) };
  if (request.expectedUserId !== undefined) result.expectedUserId = request.expectedUserId;
  return result;
}

export function messageToWireMap(message: Message): Record<string, Object> {
  const content: Record<string, Object> | undefined = message.content
    ? {
        contentType: message.content.contentType,
        ...message.content.data,
      }
    : undefined;
  return {
    serverId: message.serverId,
    clientMsgId: message.clientMsgId,
    conversationId: message.conversationId,
    conversationType: message.conversationType,
    channelId: message.channelId,
    senderId: message.senderId,
    source: message.source,
    conversationSeq: message.conversationSeq,
    createdAt: message.createdAt,
    clientCreatedAt: message.clientCreatedAt,
    messageType: message.messageType,
    ...(content !== undefined ? { content } : {}),
    senderName: message.senderName,
    senderAvatar: message.senderAvatar,
    senderDisplayName: message.senderDisplayName,
    ...(message.replyTo !== undefined ? { replyTo: message.replyTo } : {}),
    ...(message.quotePreview !== undefined ? { quotePreview: message.quotePreview } : {}),
    ...(message.threadId !== undefined ? { threadId: message.threadId } : {}),
    status: message.status,
    isRead: message.isRead,
    isRecalled: message.isRecalled,
    isEdited: message.isEdited,
    mentionUsers: message.mentionUsers,
    mentionAll: message.mentionAll,
    attributes: message.attributes,
    extensions: message.extensions,
    reactions: message.reactions.map((item) => reactionEntryToMap(item)),
    ...(message.localState !== undefined ? { localState: messageLocalStateToMap(message.localState) } : {}),
    textPreview: message.textPreview,
    timelineKey: message.timelineKey,
    timelineSortTs: message.timelineSortTs,
    version: message.version,
    updatedAt: message.updatedAt,
  };
}

export function listOfMaps(value: Object | undefined): Array<Record<string, Object>> {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter((item) => item !== null && typeof item === 'object')
    .map((item) => item as Record<string, Object>);
}

export function intValue(value: Object | undefined): number {
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'string') {
    const parsed = Number.parseInt(value, 10);
    return Number.isNaN(parsed) ? 0 : parsed;
  }
  return 0;
}

export function field(json: Record<string, Object>, key: string): Object | undefined {
  return json[key];
}

export function stringValue(value: Object | undefined): string {
  return value === undefined || value === null ? '' : String(value);
}

export function stringList(value: Object | undefined): Array<string> {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => stringValue(item as Object));
}

export function recordValue(value: Object | undefined): Record<string, Object> {
  if (value === undefined || value === null || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, Object>;
}

export function serverClockResponseFromJson(value: Object | undefined): ServerClockResponse {
  const json = recordValue(value);
  return {
    offsetMs: intValue(field(json, 'offsetMs')),
    estimated: field(json, 'estimated') === true,
  };
}

export function networkChangeResponseFromJson(value: Object | undefined): NetworkChangeResponse {
  const json = recordValue(value);
  return {
    reconnected: field(json, 'reconnected') === true,
  };
}

export function stringRecord(value: Object | undefined): Record<string, string> {
  const record = recordValue(value);
  const out: Record<string, string> = {};
  for (const key of Object.keys(record)) {
    out[key] = stringValue(record[key]);
  }
  return out;
}

export function bytesRecord(value: Object | undefined): Record<string, Array<number>> {
  const record = recordValue(value);
  const out: Record<string, Array<number>> = {};
  for (const key of Object.keys(record)) {
    const item = record[key];
    if (Array.isArray(item)) {
      out[key] = item
        .map((entry) => intValue(entry as Object))
        .filter((entry) => entry >= 0 && entry <= 255);
    }
  }
  return out;
}

export function reactionEntryFromJson(value: Object | undefined): ReactionEntry {
  const json = recordValue(value);
  return {
    count: intValue(field(json, 'count')),
    emoji: stringValue(field(json, 'emoji')),
    userIds: stringList(field(json, 'userIds')),
  };
}

export function messageLocalStateFromJson(value: Object | undefined): MessageLocalState | undefined {
  const json = recordValue(value);
  if (Object.keys(json).length === 0) {
    return undefined;
  }
  return {
    failed: field(json, 'failed') === true,
    isLocal: field(json, 'isLocal') === true,
    sending: field(json, 'sending') === true,
    sortTs: intValue(field(json, 'sortTs')),
  };
}

/** The core's receipt projection; undefined when absent or malformed (an older core). */
export function messageReceiptViewFromJson(value: Object | undefined): MessageReceiptView | undefined {
  const json = recordValue(value);
  const raw = field(json, 'status');
  const statuses: MessageReceiptStatus[] = [
    MessageReceiptStatus.Pending,
    MessageReceiptStatus.Sending,
    MessageReceiptStatus.Sent,
    MessageReceiptStatus.Read,
    MessageReceiptStatus.Failed,
  ];
  const status = statuses.find((candidate) => candidate === raw);
  if (status === undefined) {
    return undefined;
  }
  const summary = recordValue(field(json, 'readSummary'));
  const count = (key: string): number | undefined => {
    const value = field(summary, key);
    return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : undefined;
  };
  const readCount = count('readCount');
  const unreadCount = count('unreadCount');
  const total = count('total');
  if (readCount === undefined || unreadCount === undefined || total === undefined || total === 0) {
    return { status };
  }
  const readSummary: MessageReadSummary = { readCount, unreadCount, total };
  return { status, readSummary };
}

export function messagePreviewFromJson(value: Object | undefined): MessagePreview | undefined {
  const json = recordValue(value);
  if (Object.keys(json).length === 0) {
    return undefined;
  }
  return {
    messageId: stringValue(field(json, 'messageId')),
    senderId: stringValue(field(json, 'senderId')),
    text: stringValue(field(json, 'text')),
    time: intValue(field(json, 'time')),
    type: intValue(field(json, 'type')),
  };
}

export function conversationParticipantFromJson(value: Object | undefined): ConversationParticipant {
  const json = recordValue(value);
  return {
    attributes: stringRecord(field(json, 'attributes')),
    joinedAt: intValue(field(json, 'joinedAt')),
    muted: field(json, 'muted') === true,
    nickname: stringValue(field(json, 'nickname')),
    pinned: field(json, 'pinned') === true,
    roles: stringList(field(json, 'roles')),
    userId: stringValue(field(json, 'userId')),
  };
}

export function richDocV2NormalizedFromJson(value: Object | undefined): RichDocV2Normalized {
  const json = recordValue(value);
  const sourcePayload = field(json, 'sourcePayload');
  return {
    docJson: stringValue(field(json, 'docJson')),
    contentSchema: stringValue(field(json, 'contentSchema')),
    version: intValue(field(json, 'version')),
    plainText: stringValue(field(json, 'plainText')),
    searchText: stringValue(field(json, 'searchText')),
    renderHints: recordValue(field(json, 'renderHints')),
    inputFormat: field(json, 'inputFormat') !== undefined ? stringValue(field(json, 'inputFormat')) : undefined,
    sourcePayload: sourcePayload !== undefined && sourcePayload !== null ? recordValue(sourcePayload) : undefined,
  };
}

export function messageContentTypeFromJson(value: Object | undefined): MessageContentType {
  if (typeof value === 'string') {
    return MESSAGE_CONTENT_TYPE_WIRE_VALUES.find((item) => item === value) ?? MessageContentType.Custom;
  }
  return MessageContentType.Custom;
}

export function messageContentDataFromJson(rawContent: Record<string, Object>): Record<string, Object> {
  const out: Record<string, Object> = {};
  for (const key of Object.keys(rawContent)) {
    if (key !== 'contentType' && key !== 'messageType') {
      out[key] = rawContent[key];
    }
  }
  return out;
}

export function messageFromJson(value: Object | undefined): Message {
  const json = (value ?? {}) as Record<string, Object>;
  const rawContent = field(json, 'content') as Record<string, Object> | undefined;
  const content: MessageContent | undefined = rawContent
    ? {
        contentType: messageContentTypeFromJson(field(rawContent, 'contentType')),
        data: messageContentDataFromJson(rawContent),
      }
    : undefined;
  return {
    attributes: stringRecord(field(json, 'attributes')),
    channelId: String(field(json, 'channelId') ?? ''),
    clientCreatedAt: intValue(field(json, 'clientCreatedAt')),
    clientMsgId: String(field(json, 'clientMsgId') ?? ''),
    ...(content !== undefined ? { content } : {}),
    conversationId: String(field(json, 'conversationId') ?? ''),
    conversationSeq: intValue(field(json, 'conversationSeq')),
    conversationType: intValue(field(json, 'conversationType')),
    createdAt: intValue(field(json, 'createdAt')),
    extensions: bytesRecord(field(json, 'extensions')),
    isEdited: field(json, 'isEdited') === true,
    isRead: field(json, 'isRead') === true,
    isRecalled: field(json, 'isRecalled') === true,
    ...(messageLocalStateFromJson(field(json, 'localState')) !== undefined ? { localState: messageLocalStateFromJson(field(json, 'localState')) } : {}),
    mentionAll: field(json, 'mentionAll') === true,
    mentionUsers: stringList(field(json, 'mentionUsers')),
    messageType: intValue(field(json, 'messageType')),
    ...(field(json, 'quotePreview') !== undefined ? { quotePreview: stringValue(field(json, 'quotePreview')) } : {}),
    reactions: listOfMaps(field(json, 'reactions')).map((item) => reactionEntryFromJson(item)),
    ...(field(json, 'replyTo') !== undefined ? { replyTo: stringValue(field(json, 'replyTo')) } : {}),
    ...(field(json, 'threadId') !== undefined ? { threadId: stringValue(field(json, 'threadId')) } : {}),
    senderAvatar: String(field(json, 'senderAvatar') ?? ''),
    senderDisplayName: String(field(json, 'senderDisplayName') ?? ''),
    senderId: String(field(json, 'senderId') ?? ''),
    senderName: String(field(json, 'senderName') ?? ''),
    serverId: String(field(json, 'serverId') ?? ''),
    source: intValue(field(json, 'source')),
    status: intValue(field(json, 'status')),
    textPreview: String(field(json, 'textPreview') ?? ''),
    updatedAt: intValue(field(json, 'updatedAt')),
    version: intValue(field(json, 'version')),
    timelineKey: String(field(json, 'timelineKey') ?? ''),
    timelineSortTs: intValue(field(json, 'timelineSortTs')),
    ...(messageReceiptViewFromJson(field(json, 'receipt')) !== undefined ? { receipt: messageReceiptViewFromJson(field(json, 'receipt')) } : {}),
  };
}

export function sendAckFromJson(value: Object | undefined): SendMessageResponse {
  const json = (value ?? {}) as Record<string, Object>;
  return {
    serverId: String(field(json, 'serverId') ?? ''),
    clientMsgId: String(field(json, 'clientMsgId') ?? ''),
    conversationId: String(field(json, 'conversationId') ?? ''),
    seq: intValue(field(json, 'seq')),
    timestamp: intValue(field(json, 'timestamp')),
  };
}

export function conversationVersionFromJson(value: Object | undefined): ConversationVersion {
  const json = (value ?? {}) as Record<string, Object>;
  return {
    conversationId: String(field(json, 'conversationId') ?? ''),
    version: intValue(field(json, 'version')),
  };
}

export function syncConversationSummariesResponseFromJson(value: Object | undefined): SyncConversationSummariesResponse {
  const json = (value ?? {}) as Record<string, Object>;
  return {
    changedConversations: listOfMaps(field(json, 'changedConversations')).map((item) => conversationVersionFromJson(item)),
  };
}

export function listConversationsResponseFromJson(value: Object | undefined): ListConversationsResponse {
  const json = (value ?? {}) as Record<string, Object>;
  return {
    conversations: listOfMaps(field(json, 'conversations')).map((item) => conversationFromJson(item)),
  };
}

export function conversationFromJson(json: Record<string, Object>): Conversation {
  const typeOrder = CONVERSATION_TYPE_WIRE_VALUES;
  const typeRaw = field(json, 'conversationType');
  const typeIndex = typeof typeRaw === 'string'
    ? typeOrder.indexOf(typeRaw as ConversationType)
    : -1;
  return {
    avatarUrl: String(field(json, 'avatarUrl') ?? ''),
    ...(field(json, 'badge') !== undefined ? { badge: stringValue(field(json, 'badge')) } : {}),
    businessType: String(field(json, 'businessType') ?? ''),
    channelId: String(field(json, 'channelId') ?? ''),
    conversationId: String(field(json, 'conversationId') ?? ''),
    conversationType: typeIndex >= 0 ? typeOrder[typeIndex] : ConversationType.Unspecified,
    createdAt: intValue(field(json, 'createdAt')),
    ...(field(json, 'description') !== undefined ? { description: stringValue(field(json, 'description')) } : {}),
    displayName: String(field(json, 'displayName') ?? ''),
    ...(field(json, 'draft') !== undefined ? { draft: stringValue(field(json, 'draft')) } : {}),
    ext: stringRecord(field(json, 'ext')),
    isArchived: field(json, 'isArchived') === true,
    isMuted: field(json, 'isMuted') === true,
    isPinned: field(json, 'isPinned') === true,
    ...(messagePreviewFromJson(field(json, 'lastMessage')) !== undefined ? { lastMessage: messagePreviewFromJson(field(json, 'lastMessage')) } : {}),
    lastMessageAt: field(json, 'lastMessageAt') !== undefined ? intValue(field(json, 'lastMessageAt')) : undefined,
    ...(field(json, 'lastMessageId') !== undefined ? { lastMessageId: stringValue(field(json, 'lastMessageId')) } : {}),
    ...(field(json, 'lastMessagePreview') !== undefined ? { lastMessagePreview: stringValue(field(json, 'lastMessagePreview')) } : {}),
    lastReadSeq: intValue(field(json, 'lastReadSeq')),
    lastSenderAvatarUrl: String(field(json, 'lastSenderAvatarUrl') ?? ''),
    ...(field(json, 'lastSenderId') !== undefined ? { lastSenderId: stringValue(field(json, 'lastSenderId')) } : {}),
    lastSenderNickname: String(field(json, 'lastSenderNickname') ?? ''),
    maxSeq: intValue(field(json, 'maxSeq')),
    memberPreview: listOfMaps(field(json, 'memberPreview')).map((item) => conversationParticipantFromJson(item)),
    membersCount: intValue(field(json, 'membersCount')),
    mentionCount: intValue(field(json, 'mentionCount')),
    mentionMe: field(json, 'mentionMe') === true,
    participantVersion: intValue(field(json, 'participantVersion')),
    participants: listOfMaps(field(json, 'participants')).map((item) => conversationParticipantFromJson(item)),
    peerReadSeq: intValue(field(json, 'peerReadSeq')),
    ...(field(json, 'remark') !== undefined ? { remark: stringValue(field(json, 'remark')) } : {}),
    ...(field(json, 'role') !== undefined ? { role: stringValue(field(json, 'role')) } : {}),
    unreadCount: intValue(field(json, 'unreadCount')),
    updatedAt: intValue(field(json, 'updatedAt')),
    updatedAtTs: field(json, 'updatedAtTs') !== undefined ? intValue(field(json, 'updatedAtTs')) : undefined,
    version: intValue(field(json, 'version')),
    visibleAfterSeq: intValue(field(json, 'visibleAfterSeq')),
  };
}

export function listMessagesResponseFromJson(value: Object | undefined): ListMessagesResponse {
  const json = (value ?? {}) as Record<string, Object>;
  return {
    messages: listOfMaps(field(json, 'messages')).map((item) => messageFromJson(item)),
  };
}

export function viewSnapshotFromJson(value: Object | undefined): ViewSnapshot {
  const json = recordValue(value);
  return {
    viewType: stringValue(field(json, 'viewType')),
    data: recordValue(field(json, 'data')),
  };
}

export function viewOpenResponseFromJson(value: Object | undefined): ViewOpenResponse {
  const json = recordValue(value);
  return {
    viewId: stringValue(field(json, 'viewId')),
    revision: intValue(field(json, 'revision')),
    snapshot: viewSnapshotFromJson(field(json, 'snapshot')),
  };
}

/** Decodes `ViewSnapshot.data` of a timeline view. */
export function timelineViewSnapshotFromJson(value: Object | undefined): TimelineViewSnapshot {
  const json = recordValue(value);
  const conversation = field(json, 'conversation');
  const anchorKey = field(json, 'anchorKey');
  return {
    ...(conversation !== undefined ? { conversation: conversationFromJson(recordValue(conversation)) } : {}),
    messages: listOfMaps(field(json, 'messages')).map((item) => messageFromJson(item)),
    hasOlder: field(json, 'hasOlder') === true,
    hasNewer: field(json, 'hasNewer') === true,
    attachedToLatest: field(json, 'attachedToLatest') === true,
    ...(typeof anchorKey === 'string' && anchorKey.trim() !== '' ? { anchorKey } : {}),
  };
}

export function timelineViewPageResponseFromJson(value: Object | undefined): TimelineViewPageResponse {
  const json = recordValue(value);
  return {
    viewId: stringValue(field(json, 'viewId')),
    loadedCount: intValue(field(json, 'loadedCount')),
    revision: intValue(field(json, 'revision')),
  };
}

export function viewDeltaOpFromJson(value: Object | undefined): ViewDeltaOp {
  const json = recordValue(value);
  const fromIndex = field(json, 'fromIndex');
  const item = field(json, 'item');
  return {
    op: stringValue(field(json, 'op')),
    key: stringValue(field(json, 'key')),
    index: intValue(field(json, 'index')),
    ...(fromIndex !== undefined ? { fromIndex: intValue(fromIndex) } : {}),
    ...(item !== undefined ? { item: recordValue(item) } : {}),
  };
}

function viewDeltaRowKeys(value: Object | undefined): string[] {
  return Array.isArray(value) ? (value as Object[]).filter((key) => typeof key === 'string').map((key) => key as string) : [];
}

/**
 * Timeline deltas work by row key (`clientMsgId`, else `serverId`, else
 * `seq:<conversationId>:<conversationSeq>`): remove, upsert in place, prepend, append, then
 * reorder when present. Conversation-list deltas keep the indexed `ops`.
 */
export function viewDeltaFromJson(value: Object | undefined): ViewDelta {
  const json = recordValue(value);
  const viewType = stringValue(field(json, 'viewType'));
  if (viewType === 'conversationList') {
    const totalUnread = field(json, 'totalUnread');
    const syncState = field(json, 'syncState');
    return {
      viewType,
      ops: listOfMaps(field(json, 'ops')).map((item) => viewDeltaOpFromJson(item)),
      ...(totalUnread !== undefined ? { totalUnread: intValue(totalUnread) } : {}),
      ...(syncState !== undefined ? { syncState: stringValue(syncState) } : {}),
    };
  }
  const conversation = field(json, 'conversation');
  const reorder = field(json, 'reorder');
  return {
    viewType,
    prepend: listOfMaps(field(json, 'prepend')).map((item) => messageFromJson(item)),
    append: listOfMaps(field(json, 'append')).map((item) => messageFromJson(item)),
    upsert: listOfMaps(field(json, 'upsert')).map((item) => messageFromJson(item)),
    remove: viewDeltaRowKeys(field(json, 'remove')),
    ...(reorder !== undefined && reorder !== null ? { reorder: viewDeltaRowKeys(reorder) } : {}),
    hasOlder: field(json, 'hasOlder') === true,
    hasNewer: field(json, 'hasNewer') === true,
    attachedToLatest: field(json, 'attachedToLatest') === true,
    ...(conversation !== undefined ? { conversation: conversationFromJson(recordValue(conversation)) } : {}),
  };
}

export function viewUpdateFromJson(value: Object | undefined): ViewUpdate {
  const json = recordValue(value);
  const kind = stringValue(field(json, 'kind')) === 'delta' ? 'delta' : 'snapshot';
  return {
    viewId: stringValue(field(json, 'viewId')),
    revision: intValue(field(json, 'revision')),
    kind,
    ...(kind === 'delta'
      ? { delta: viewDeltaFromJson(field(json, 'delta')) }
      : { snapshot: viewSnapshotFromJson(field(json, 'snapshot')) }),
  };
}

export function closeViewResponseFromJson(value: Object | undefined): CloseViewResponse {
  const json = recordValue(value);
  return {
    closed: field(json, 'closed') === true,
  };
}

// RUST-OWNED WIRE BOUNDARY: BEGIN
/** The FFI wire contract is canonical camelCase SDK JSON. */
export function wireEncodeRequest(value: unknown): unknown {
  return value;
}

/** The FFI wire contract is canonical camelCase SDK JSON. */
export function wireDecodeResponse(value: unknown): unknown {
  return value;
}
// RUST-OWNED WIRE BOUNDARY: END
