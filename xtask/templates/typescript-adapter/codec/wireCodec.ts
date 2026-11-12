// GENERATED. Do not edit by hand.
import { FlareSdkException } from '../../bridge/flareSdkException';
import {
  AudioContentPayload,
  BootstrapHomeTimelineRequest,
  BuildAnnouncementMessageRequest,
  BuildAudioMessageRequest,
  BuildCardMessageRequest,
  BuildCustomMessageRequest,
  BuildEmojiMessageRequest,
  BuildFileMessageRequest,
  BuildForwardMessageRequest,
  BuildImageGroupMessageRequest,
  BuildImageMessageRequest,
  BuildLinkCardMessageRequest,
  BuildLocationMessageRequest,
  BuildMiniProgramMessageRequest,
  BuildNotificationMessageRequest,
  BuildPlaceholderMessageRequest,
  BuildQuoteMessageRequest,
  BuildRichDocMessageRequest,
  BuildScheduleMessageRequest,
  BuildStickerMessageRequest,
  BuildSystemMessageRequest,
  BuildTaskMessageRequest,
  BuildTextMessageRequest,
  BuildThreadReplyMessageRequest,
  BuildTypedMessageRequest,
  BuildVideoMessageRequest,
  BuildVoteMessageRequest,
  BuildWithContentMessageRequest,
  ConnectionEvent,
  ConnectionEventName,
  Conversation,
  ConversationEvent,
  ConversationEventName,
  ConversationListQuery,
  ConversationParticipant,
  ConversationTimelineSnapshot,
  ConversationType,
  ConversationVersion,
  CreateTextMessageRequest,
  EmojiContentPayload,
  FileContentPayload,
  ForwardContentPayload,
  ForwardSourceMessage,
  HeartbeatAppState,
  HeartbeatEffectiveIntervalResponse,
  HomeTimelineSnapshot,
  ImageContentPayload,
  ImageGroupContentPayload,
  ImageGroupItem,
  LifecycleEvent,
  LifecycleEventName,
  ListConversationsResponse,
  ListMessageBuildCatalogResponse,
  ListMessagesRequest,
  ListMessagesResponse,
  MediaSourceInfo,
  Message,
  MessageBuildCatalogEntry,
  MessageBuildOp,
  MessageContent,
  MessageContentType,
  MessageEventName,
  MessageLocalState,
  MessageMutationEvent,
  MessagePreview,
  MessageReceivedBatchEvent,
  MessageReceivedEvent,
  MessageSearchQuery,
  MessageSendAckEvent,
  MessageSendFailedEvent,
  NormalizeRichDocFromDocJsonRequest,
  NormalizeRichDocFromHtmlRequest,
  NormalizeRichDocFromMarkdownRequest,
  OpenConversationTimelineRequest,
  PresenceChangedEvent,
  ProgressEvent,
  ProgressEventName,
  ReactionChangedEvent,
  ReactionEntry,
  ReadReceiptEvent,
  RichDocV2Normalized,
  SdkConnectionState,
  SdkErrorPayload,
  SdkEventEnvelope,
  SdkEventKind,
  SendMessageRequest,
  SendMessageResponse,
  SetHeartbeatAppStateRequest,
  SetHeartbeatNatTimeoutRequest,
  StickerContentPayload,
  SyncConversationSummariesRequest,
  SyncConversationSummariesResponse,
  SyncEvent,
  SyncEventName,
  TextContentPayload,
  TimelineSyncState,
  TypingEvent,
  VideoContentPayload,
} from '../../model';

export function enumWireIndex<T>(order: ReadonlyArray<T>, value: T): number {
  const index = order.indexOf(value);
  if (index >= 0) {
    return index;
  }
  throw new FlareSdkException(
    'invalidParameter',
    `invalid enum wire value: ${String(value)}`,
    'wire.enum.encode',
    { value: String(value) },
  );
}



const HEARTBEAT_APP_STATE_WIRE_ORDER: HeartbeatAppState[] = [HeartbeatAppState.Foreground, HeartbeatAppState.Background];
const CONVERSATION_TYPE_WIRE_VALUES: ConversationType[] = [ConversationType.Unspecified, ConversationType.Single, ConversationType.Group, ConversationType.Ai, ConversationType.System, ConversationType.Customer, ConversationType.Temp, ConversationType.Channel, ConversationType.Broadcast];
const TIMELINE_SYNC_STATE_WIRE_VALUES: TimelineSyncState[] = [TimelineSyncState.LocalReady, TimelineSyncState.Synced, TimelineSyncState.Partial];
const MESSAGE_CONTENT_TYPE_WIRE_VALUES: MessageContentType[] = [MessageContentType.Text, MessageContentType.Image, MessageContentType.Video, MessageContentType.Audio, MessageContentType.File, MessageContentType.Location, MessageContentType.Card, MessageContentType.Sticker, MessageContentType.Emoji, MessageContentType.Quote, MessageContentType.LinkCard, MessageContentType.Forward, MessageContentType.Thread, MessageContentType.MiniProgram, MessageContentType.RichText, MessageContentType.ImageGroup, MessageContentType.System, MessageContentType.Notification, MessageContentType.Vote, MessageContentType.Task, MessageContentType.Schedule, MessageContentType.Announcement, MessageContentType.Custom, MessageContentType.Placeholder];
const SDK_EVENT_KIND_WIRE_ORDER: SdkEventKind[] = [SdkEventKind.Lifecycle, SdkEventKind.Connection, SdkEventKind.Message, SdkEventKind.Notification, SdkEventKind.Conversation, SdkEventKind.Sync, SdkEventKind.Extension, SdkEventKind.ExtensionEvent, SdkEventKind.Presence, SdkEventKind.Media, SdkEventKind.Plugin];
const LIFECYCLE_EVENT_NAME_WIRE_ORDER: LifecycleEventName[] = [LifecycleEventName.Initializing, LifecycleEventName.Initialized, LifecycleEventName.InitFailed, LifecycleEventName.LoginSucceeded, LifecycleEventName.LoginFailed, LifecycleEventName.LoggedOut, LifecycleEventName.Disposed];
const SDK_CONNECTION_STATE_WIRE_ORDER: SdkConnectionState[] = [SdkConnectionState.Disconnected, SdkConnectionState.Connecting, SdkConnectionState.Connected, SdkConnectionState.Ready, SdkConnectionState.Reconnecting];
const CONNECTION_EVENT_NAME_WIRE_ORDER: ConnectionEventName[] = [ConnectionEventName.Connecting, ConnectionEventName.Connected, ConnectionEventName.Ready, ConnectionEventName.Disconnected, ConnectionEventName.Reconnecting, ConnectionEventName.ReconnectFailed, ConnectionEventName.StateChanged, ConnectionEventName.SyncStateChanged, ConnectionEventName.ServerError, ConnectionEventName.KickedOff, ConnectionEventName.TokenExpired];
const MESSAGE_EVENT_NAME_WIRE_ORDER: MessageEventName[] = [MessageEventName.Received, MessageEventName.ReceivedBatch, MessageEventName.SendAck, MessageEventName.SendFailed, MessageEventName.Recalled, MessageEventName.Typing, MessageEventName.Edited, MessageEventName.ReactionChanged, MessageEventName.Deleted, MessageEventName.ReadReceipt, MessageEventName.BurnScheduled, MessageEventName.Burned, MessageEventName.HardDeleted, MessageEventName.Pinned, MessageEventName.Unpinned, MessageEventName.Marked, MessageEventName.Unmarked, MessageEventName.RetentionScheduled, MessageEventName.RetentionExpired, MessageEventName.RetentionPurged, MessageEventName.PresenceChanged, MessageEventName.Custom, MessageEventName.PluginStateChanged, MessageEventName.ReadSummariesChanged];
const CONVERSATION_EVENT_NAME_WIRE_ORDER: ConversationEventName[] = [ConversationEventName.Synced, ConversationEventName.Created, ConversationEventName.Updated, ConversationEventName.UnreadCountChanged, ConversationEventName.Deleted];
const SYNC_EVENT_NAME_WIRE_ORDER: SyncEventName[] = [SyncEventName.StateChanged, SyncEventName.Started, SyncEventName.Finished, SyncEventName.Failed, SyncEventName.Progress, SyncEventName.TaskCompleted, SyncEventName.ResyncNeeded];
const PROGRESS_EVENT_NAME_WIRE_ORDER: ProgressEventName[] = [ProgressEventName.SyncProgress, ProgressEventName.UploadProgress, ProgressEventName.DownloadProgress];



export function setHeartbeatAppStateRequestToMap(request: SetHeartbeatAppStateRequest): Record<string, unknown> {
  return {
  appState: enumWireIndex(HEARTBEAT_APP_STATE_WIRE_ORDER, request.appState),
  };
}

export function setHeartbeatNatTimeoutRequestToMap(request: SetHeartbeatNatTimeoutRequest): Record<string, unknown> {
  return {
  ...(request.natTimeoutSecs !== undefined ? { natTimeoutSecs: request.natTimeoutSecs } : {}),
  };
}

export function heartbeatEffectiveIntervalResponseToMap(request: HeartbeatEffectiveIntervalResponse): Record<string, unknown> {
  return {
  connected: request.connected,
  ...(request.intervalMs !== undefined ? { intervalMs: request.intervalMs } : {}),
  ...(request.intervalSecs !== undefined ? { intervalSecs: request.intervalSecs } : {}),
  };
}

export function conversationParticipantToMap(request: ConversationParticipant): Record<string, unknown> {
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

export function messagePreviewToMap(request: MessagePreview): Record<string, unknown> {
  return {
  messageId: request.messageId,
  senderId: request.senderId,
  type: request.type,
  text: request.text,
  time: request.time,
  };
}

export function conversationToMap(request: Conversation): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  conversationType: request.conversationType,
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

export function conversationListQueryToMap(request: ConversationListQuery): Record<string, unknown> {
  return {
  ...(request.keyword !== undefined ? { keyword: request.keyword } : {}),
  includeArchived: request.includeArchived,
  unreadOnly: request.unreadOnly,
  mentionMeOnly: request.mentionMeOnly,
  pinnedOnly: request.pinnedOnly,
  mutedOnly: request.mutedOnly,
  hasDraftOnly: request.hasDraftOnly,
  hasMarkedMessages: request.hasMarkedMessages,
  ...(request.conversationTypes.length > 0 ? { conversationTypes: request.conversationTypes } : {}),
  ...(request.cursor !== undefined ? { cursor: request.cursor } : {}),
  ...(request.limit !== undefined ? { limit: request.limit } : {}),
  };
}

export function listConversationsResponseToMap(request: ListConversationsResponse): Record<string, unknown> {
  return {
  ...(request.conversations.length > 0 ? { conversations: request.conversations.map((item) => conversationToMap(item)) } : {}),
  };
}

export function bootstrapHomeTimelineRequestToMap(request: BootstrapHomeTimelineRequest): Record<string, unknown> {
  return {
  conversationLimit: request.conversationLimit,
  };
}

export function openConversationTimelineRequestToMap(request: OpenConversationTimelineRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  messageLimit: request.messageLimit,
  };
}

export function homeTimelineSnapshotToMap(request: HomeTimelineSnapshot): Record<string, unknown> {
  return {
  ...(request.conversations.length > 0 ? { conversations: request.conversations.map((item) => conversationToMap(item)) } : {}),
  totalUnread: request.totalUnread,
  syncState: request.syncState,
  };
}

export function conversationTimelineSnapshotToMap(request: ConversationTimelineSnapshot): Record<string, unknown> {
  return {
  ...(request.conversation !== undefined ? { conversation: conversationToMap(request.conversation) } : {}),
  ...(request.messages.length > 0 ? { messages: request.messages.map((item) => messageToMap(item)) } : {}),
  hasMore: request.hasMore,
  };
}

export function conversationVersionToMap(request: ConversationVersion): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  version: request.version,
  };
}

export function syncConversationSummariesRequestToMap(request: SyncConversationSummariesRequest): Record<string, unknown> {
  return {
  ...(request.knownVersions.length > 0 ? { knownVersions: request.knownVersions.map((item) => conversationVersionToMap(item)) } : {}),
  };
}

export function syncConversationSummariesResponseToMap(request: SyncConversationSummariesResponse): Record<string, unknown> {
  return {
  ...(request.changedConversations.length > 0 ? { changedConversations: request.changedConversations.map((item) => conversationVersionToMap(item)) } : {}),
  };
}

export function reactionEntryToMap(request: ReactionEntry): Record<string, unknown> {
  return {
  emoji: request.emoji,
  ...(request.userIds.length > 0 ? { userIds: request.userIds } : {}),
  count: request.count,
  };
}

export function messageLocalStateToMap(request: MessageLocalState): Record<string, unknown> {
  return {
  sending: request.sending,
  failed: request.failed,
  isLocal: request.isLocal,
  sortTs: request.sortTs,
  };
}

export function messageContentToMap(request: MessageContent): Record<string, unknown> {
  return {
  contentType: request.contentType,
  data: request.data,
  };
}

export function createTextMessageRequestToMap(request: CreateTextMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  text: request.text,
  };
}

export function sendMessageResponseToMap(request: SendMessageResponse): Record<string, unknown> {
  return {
  ackId: request.ackId,
  serverId: request.serverId,
  clientMsgId: request.clientMsgId,
  conversationId: request.conversationId,
  seq: request.seq,
  timestamp: request.timestamp,
  success: request.success,
  errorCode: request.errorCode,
  errorMessage: request.errorMessage,
  };
}

export function listMessagesRequestToMap(request: ListMessagesRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  beforeSeq: request.beforeSeq,
  limit: request.limit,
  };
}

export function listMessagesResponseToMap(request: ListMessagesResponse): Record<string, unknown> {
  return {
  ...(request.messages.length > 0 ? { messages: request.messages.map((item) => messageToMap(item)) } : {}),
  };
}

export function messageSearchQueryToMap(request: MessageSearchQuery): Record<string, unknown> {
  return {
  ...(request.keyword !== undefined ? { keyword: request.keyword } : {}),
  ...(request.conversationId !== undefined ? { conversationId: request.conversationId } : {}),
  ...(request.senderId !== undefined ? { senderId: request.senderId } : {}),
  ...(request.fromTime !== undefined ? { fromTime: request.fromTime } : {}),
  ...(request.toTime !== undefined ? { toTime: request.toTime } : {}),
  ...(request.kinds.length > 0 ? { kinds: request.kinds } : {}),
  limit: request.limit,
  includeRecalled: request.includeRecalled,
  };
}

export function mediaSourceInfoToMap(request: MediaSourceInfo): Record<string, unknown> {
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

export function textContentPayloadToMap(request: TextContentPayload): Record<string, unknown> {
  return {
  text: request.text,
  };
}

export function imageContentPayloadToMap(request: ImageContentPayload): Record<string, unknown> {
  return {
  ...(request.imageId !== undefined ? { imageId: request.imageId } : {}),
  ...(request.source !== undefined ? { source: mediaSourceInfoToMap(request.source) } : {}),
  ...(request.thumbnail !== undefined ? { thumbnail: mediaSourceInfoToMap(request.thumbnail) } : {}),
  ...(request.description !== undefined ? { description: request.description } : {}),
  };
}

export function imageGroupItemToMap(request: ImageGroupItem): Record<string, unknown> {
  return {
  imageId: request.imageId,
  ...(request.url !== undefined ? { url: request.url } : {}),
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.width !== undefined ? { width: request.width } : {}),
  ...(request.height !== undefined ? { height: request.height } : {}),
  };
}

export function imageGroupContentPayloadToMap(request: ImageGroupContentPayload): Record<string, unknown> {
  return {
  ...(request.images.length > 0 ? { images: request.images.map((item) => imageGroupItemToMap(item)) } : {}),
  ...(request.title !== undefined ? { title: request.title } : {}),
  };
}

export function videoContentPayloadToMap(request: VideoContentPayload): Record<string, unknown> {
  return {
  ...(request.videoId !== undefined ? { videoId: request.videoId } : {}),
  ...(request.source !== undefined ? { source: mediaSourceInfoToMap(request.source) } : {}),
  ...(request.cover !== undefined ? { cover: mediaSourceInfoToMap(request.cover) } : {}),
  ...(request.description !== undefined ? { description: request.description } : {}),
  };
}

export function audioContentPayloadToMap(request: AudioContentPayload): Record<string, unknown> {
  return {
  ...(request.audioId !== undefined ? { audioId: request.audioId } : {}),
  ...(request.source !== undefined ? { source: mediaSourceInfoToMap(request.source) } : {}),
  ...(request.durationMs !== undefined ? { durationMs: request.durationMs } : {}),
  };
}

export function fileContentPayloadToMap(request: FileContentPayload): Record<string, unknown> {
  return {
  ...(request.fileId !== undefined ? { fileId: request.fileId } : {}),
  ...(request.name !== undefined ? { name: request.name } : {}),
  ...(request.url !== undefined ? { url: request.url } : {}),
  ...(request.mimeType !== undefined ? { mimeType: request.mimeType } : {}),
  ...(request.size !== undefined ? { size: request.size } : {}),
  };
}

export function emojiContentPayloadToMap(request: EmojiContentPayload): Record<string, unknown> {
  return {
  emoji: request.emoji,
  };
}

export function stickerContentPayloadToMap(request: StickerContentPayload): Record<string, unknown> {
  return {
  stickerId: request.stickerId,
  ...(request.packageId !== undefined ? { packageId: request.packageId } : {}),
  ...(request.url !== undefined ? { url: request.url } : {}),
  ...(request.width !== undefined ? { width: request.width } : {}),
  ...(request.height !== undefined ? { height: request.height } : {}),
  ...(request.format !== undefined ? { format: request.format } : {}),
  };
}

export function forwardSourceMessageToMap(request: ForwardSourceMessage): Record<string, unknown> {
  return {
  sourceMessageId: request.sourceMessageId,
  ...(request.sourceConversationId !== undefined ? { sourceConversationId: request.sourceConversationId } : {}),
  ...(request.sourceSenderId !== undefined ? { sourceSenderId: request.sourceSenderId } : {}),
  ...(request.plainText !== undefined ? { plainText: request.plainText } : {}),
  };
}

export function forwardContentPayloadToMap(request: ForwardContentPayload): Record<string, unknown> {
  return {
  merge: request.merge,
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.sourceMessages.length > 0 ? { sourceMessages: request.sourceMessages.map((item) => forwardSourceMessageToMap(item)) } : {}),
  };
}

export function messageBuildCatalogEntryToMap(request: MessageBuildCatalogEntry): Record<string, unknown> {
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

export function listMessageBuildCatalogResponseToMap(request: ListMessageBuildCatalogResponse): Record<string, unknown> {
  return {
  ...(request.entries.length > 0 ? { entries: request.entries.map((item) => messageBuildCatalogEntryToMap(item)) } : {}),
  };
}

export function buildTypedMessageRequestToMap(request: BuildTypedMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  op: request.op,
  data: request.data,
  };
}

export function buildTextMessageRequestToMap(request: BuildTextMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  text: request.text,
  mentionUsers: request.mentionUsers ?? [],
  mentionAll: request.mentionAll ?? false,
  };
}

export function buildQuoteMessageRequestToMap(request: BuildQuoteMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  quotedMessageId: request.quotedMessageId,
  text: request.text,
  ...(request.quotedSenderId !== undefined ? { quotedSenderId: request.quotedSenderId } : {}),
  ...(request.quotedTextPreview !== undefined ? { quotedTextPreview: request.quotedTextPreview } : {}),
  };
}

export function buildThreadReplyMessageRequestToMap(request: BuildThreadReplyMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  threadId: request.threadId,
  text: request.text,
  };
}

export function buildForwardMessageRequestToMap(request: BuildForwardMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  merge: request.merge,
  title: request.title,
  ...(request.sourceMessages.length > 0 ? { sourceMessages: request.sourceMessages.map((item) => forwardSourceMessageToMap(item)) } : {}),
  };
}

export function buildImageMessageRequestToMap(request: BuildImageMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  imageId: request.imageId,
  ...(request.payload !== undefined ? { payload: imageContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildImageGroupMessageRequestToMap(request: BuildImageGroupMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  payload: imageGroupContentPayloadToMap(request.payload),
  };
}

export function buildVideoMessageRequestToMap(request: BuildVideoMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  videoId: request.videoId,
  ...(request.payload !== undefined ? { payload: videoContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildAudioMessageRequestToMap(request: BuildAudioMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  audioId: request.audioId,
  ...(request.payload !== undefined ? { payload: audioContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildFileMessageRequestToMap(request: BuildFileMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  fileId: request.fileId,
  ...(request.payload !== undefined ? { payload: fileContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildEmojiMessageRequestToMap(request: BuildEmojiMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  emoji: request.emoji,
  };
}

export function buildLocationMessageRequestToMap(request: BuildLocationMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  latitude: request.latitude,
  longitude: request.longitude,
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.address !== undefined ? { address: request.address } : {}),
  };
}

export function buildStickerMessageRequestToMap(request: BuildStickerMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  stickerId: request.stickerId,
  ...(request.packageId !== undefined ? { packageId: request.packageId } : {}),
  ...(request.payload !== undefined ? { payload: stickerContentPayloadToMap(request.payload) } : {}),
  };
}

export function buildLinkCardMessageRequestToMap(request: BuildLinkCardMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  url: request.url,
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.description !== undefined ? { description: request.description } : {}),
  };
}

export function buildCardMessageRequestToMap(request: BuildCardMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  id: request.id,
  ...(request.cardType !== undefined ? { cardType: request.cardType } : {}),
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.subtitle !== undefined ? { subtitle: request.subtitle } : {}),
  ...(request.avatar !== undefined ? { avatar: request.avatar } : {}),
  };
}

export function buildMiniProgramMessageRequestToMap(request: BuildMiniProgramMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  appId: request.appId,
  ...(request.pagePath !== undefined ? { pagePath: request.pagePath } : {}),
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.thumbnailUrl !== undefined ? { thumbnailUrl: request.thumbnailUrl } : {}),
  extra: request.extra,
  };
}

export function buildRichDocMessageRequestToMap(request: BuildRichDocMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  docJson: request.docJson,
  contentSchema: request.contentSchema,
  plainText: request.plainText,
  ...(request.inputFormat !== undefined ? { inputFormat: request.inputFormat } : {}),
  ...(request.inputFormatVersion !== undefined ? { inputFormatVersion: request.inputFormatVersion } : {}),
  sourcePayload: request.sourcePayload,
  ...(request.title !== undefined ? { title: request.title } : {}),
  ...(request.searchText !== undefined ? { searchText: request.searchText } : {}),
  ...(request.renderHintsJson !== undefined ? { renderHintsJson: request.renderHintsJson } : {}),
  };
}

export function normalizeRichDocFromMarkdownRequestToMap(request: NormalizeRichDocFromMarkdownRequest): Record<string, unknown> {
  return {
  markdown: request.markdown,
  };
}

export function normalizeRichDocFromHtmlRequestToMap(request: NormalizeRichDocFromHtmlRequest): Record<string, unknown> {
  return {
  html: request.html,
  };
}

export function normalizeRichDocFromDocJsonRequestToMap(request: NormalizeRichDocFromDocJsonRequest): Record<string, unknown> {
  return {
  docJson: request.docJson,
  };
}

export function richDocV2NormalizedToMap(request: RichDocV2Normalized): Record<string, unknown> {
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

export function buildSystemMessageRequestToMap(request: BuildSystemMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  eventKind: request.eventKind,
  body: request.body,
  };
}

export function buildNotificationMessageRequestToMap(request: BuildNotificationMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  title: request.title,
  body: request.body,
  };
}

export function buildVoteMessageRequestToMap(request: BuildVoteMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  voteId: request.voteId,
  title: request.title,
  ...(request.options.length > 0 ? { options: request.options } : {}),
  ...(request.participantUserIds.length > 0 ? { participantUserIds: request.participantUserIds } : {}),
  };
}

export function buildTaskMessageRequestToMap(request: BuildTaskMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  taskId: request.taskId,
  title: request.title,
  ...(request.status !== undefined ? { status: request.status } : {}),
  ...(request.participantUserIds.length > 0 ? { participantUserIds: request.participantUserIds } : {}),
  };
}

export function buildScheduleMessageRequestToMap(request: BuildScheduleMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  scheduleId: request.scheduleId,
  title: request.title,
  startTimeMs: request.startTimeMs,
  endTimeMs: request.endTimeMs,
  ...(request.participantUserIds.length > 0 ? { participantUserIds: request.participantUserIds } : {}),
  };
}

export function buildAnnouncementMessageRequestToMap(request: BuildAnnouncementMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  title: request.title,
  body: request.body,
  };
}

export function buildCustomMessageRequestToMap(request: BuildCustomMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  type: request.type,
  };
}

export function buildPlaceholderMessageRequestToMap(request: BuildPlaceholderMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  reason: request.reason,
  };
}

export function buildWithContentMessageRequestToMap(request: BuildWithContentMessageRequest): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  content: messageContentToMap(request.content),
  };
}

export function sdkErrorPayloadToMap(request: SdkErrorPayload): Record<string, unknown> {
  return {
  code: request.code,
  message: request.message,
  ...(request.operation !== undefined ? { operation: request.operation } : {}),
  retryable: request.retryable,
  details: request.details,
  };
}

export function sdkEventEnvelopeToMap(request: SdkEventEnvelope): Record<string, unknown> {
  return {
  eventId: request.eventId,
  kind: enumWireIndex(SDK_EVENT_KIND_WIRE_ORDER, request.kind),
  name: request.name,
  occurredAt: request.occurredAt,
  ...(request.traceId !== undefined ? { traceId: request.traceId } : {}),
  payload: request.payload,
  };
}

export function lifecycleEventToMap(request: LifecycleEvent): Record<string, unknown> {
  return {
  name: enumWireIndex(LIFECYCLE_EVENT_NAME_WIRE_ORDER, request.name),
  operation: request.operation,
  ...(request.userId !== undefined ? { userId: request.userId } : {}),
  ...(request.sessionId !== undefined ? { sessionId: request.sessionId } : {}),
  ...(request.error !== undefined ? { error: sdkErrorPayloadToMap(request.error) } : {}),
  };
}

export function connectionEventToMap(request: ConnectionEvent): Record<string, unknown> {
  return {
  name: enumWireIndex(CONNECTION_EVENT_NAME_WIRE_ORDER, request.name),
  state: enumWireIndex(SDK_CONNECTION_STATE_WIRE_ORDER, request.state),
  ...(request.reason !== undefined ? { reason: request.reason } : {}),
  ...(request.attempt !== undefined ? { attempt: request.attempt } : {}),
  ...(request.error !== undefined ? { error: sdkErrorPayloadToMap(request.error) } : {}),
  };
}

export function messageReceivedEventToMap(request: MessageReceivedEvent): Record<string, unknown> {
  return {
  message: messageToMap(request.message),
  };
}

export function messageReceivedBatchEventToMap(request: MessageReceivedBatchEvent): Record<string, unknown> {
  return {
  ...(request.messages.length > 0 ? { messages: request.messages.map((item) => messageToMap(item)) } : {}),
  };
}

export function messageSendAckEventToMap(request: MessageSendAckEvent): Record<string, unknown> {
  return {
  ack: sendMessageResponseToMap(request.ack),
  };
}

export function messageSendFailedEventToMap(request: MessageSendFailedEvent): Record<string, unknown> {
  return {
  clientMsgId: request.clientMsgId,
  reason: request.reason,
  ...(request.error !== undefined ? { error: sdkErrorPayloadToMap(request.error) } : {}),
  };
}

export function messageMutationEventToMap(request: MessageMutationEvent): Record<string, unknown> {
  return {
  name: enumWireIndex(MESSAGE_EVENT_NAME_WIRE_ORDER, request.name),
  conversationId: request.conversationId,
  ...(request.messageId !== undefined ? { messageId: request.messageId } : {}),
  ...(request.serverMsgId !== undefined ? { serverMsgId: request.serverMsgId } : {}),
  ...(request.userId !== undefined ? { userId: request.userId } : {}),
  ...(request.reason !== undefined ? { reason: request.reason } : {}),
  };
}

export function typingEventToMap(request: TypingEvent): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  userId: request.userId,
  typing: request.typing,
  };
}

export function readReceiptEventToMap(request: ReadReceiptEvent): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  userId: request.userId,
  readSeq: request.readSeq,
  };
}

export function reactionChangedEventToMap(request: ReactionChangedEvent): Record<string, unknown> {
  return {
  conversationId: request.conversationId,
  serverMsgId: request.serverMsgId,
  userId: request.userId,
  emoji: request.emoji,
  action: request.action,
  };
}

export function conversationEventToMap(request: ConversationEvent): Record<string, unknown> {
  return {
  name: enumWireIndex(CONVERSATION_EVENT_NAME_WIRE_ORDER, request.name),
  ...(request.conversationId !== undefined ? { conversationId: request.conversationId } : {}),
  ...(request.conversationIds.length > 0 ? { conversationIds: request.conversationIds } : {}),
  ...(request.unreadCount !== undefined ? { unreadCount: request.unreadCount } : {}),
  };
}

export function presenceChangedEventToMap(request: PresenceChangedEvent): Record<string, unknown> {
  return {
  ...(request.conversationId !== undefined ? { conversationId: request.conversationId } : {}),
  userId: request.userId,
  status: request.status,
  extra: request.extra,
  };
}

export function progressEventToMap(request: ProgressEvent): Record<string, unknown> {
  return {
  name: enumWireIndex(PROGRESS_EVENT_NAME_WIRE_ORDER, request.name),
  operation: request.operation,
  current: request.current,
  total: request.total,
  ...(request.taskId !== undefined ? { taskId: request.taskId } : {}),
  ...(request.detail !== undefined ? { detail: request.detail } : {}),
  };
}

export function syncEventToMap(request: SyncEvent): Record<string, unknown> {
  return {
  name: enumWireIndex(SYNC_EVENT_NAME_WIRE_ORDER, request.name),
  ...(request.trigger !== undefined ? { trigger: request.trigger } : {}),
  ...(request.phase !== undefined ? { phase: request.phase } : {}),
  ...(request.task !== undefined ? { task: request.task } : {}),
  ...(request.progress !== undefined ? { progress: request.progress } : {}),
  ...(request.error !== undefined ? { error: sdkErrorPayloadToMap(request.error) } : {}),
  };
}



export function sendMessageRequestToMap(request: SendMessageRequest): Record<string, unknown> {
  return { message: messageToWireMap(request.message), ...(request.expectedUserId === undefined ? {} : { expectedUserId: request.expectedUserId }) };
}

export function messageToWireMap(message: Message): Record<string, unknown> {
  const content: Record<string, unknown> | undefined = message.content
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

export const messageToMap = messageToWireMap;

export function listOfMaps(value: unknown | undefined): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter((item) => item !== null && typeof item === 'object')
    .map((item) => item as Record<string, unknown>);
}

export function requiredListOfMaps(value: unknown | undefined, fieldName: string, context: string): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) {
    throw new FlareSdkException('invalidParameter', `${context}.${fieldName} must be an array`, 'wire.decode', { field: `${context}.${fieldName}`, expected: 'array' });
  }
  return value.map((item, index) => {
    if (item === null || typeof item !== 'object' || Array.isArray(item)) {
      throw new FlareSdkException('invalidParameter', `${context}.${fieldName}[${index}] must be an object`, 'wire.decode', { field: `${context}.${fieldName}[${index}]`, expected: 'object' });
    }
    return item as Record<string, unknown>;
  });
}

export function intValue(value: unknown | undefined): number {
  if (typeof value === 'number' && Number.isFinite(value) && Number.isInteger(value) && value >= 0) {
    return value;
  }
  throw new FlareSdkException('invalidParameter', 'wire field must be an unsigned integer', 'wire.decode', { expected: 'unsigned integer' });
}

export function field(json: Record<string, unknown>, key: string): unknown {
  return json[key];
}

export function stringValue(value: unknown | undefined): string {
  if (typeof value === 'string') {
    return value;
  }
  throw new FlareSdkException('invalidParameter', 'wire field must be a string', 'wire.decode', { expected: 'string' });
}

export function requiredStringField(json: Record<string, unknown>, key: string, context: string): string {
  const value = field(json, key);
  if (typeof value !== 'string') {
    throw new Error(`${context}.${key} is required`);
  }
  return value;
}

export function requiredIntField(json: Record<string, unknown>, key: string, context: string): number {
  const value = field(json, key);
  if (value === undefined || value === null) {
    throw new Error(`${context}.${key} is required`);
  }
  if (typeof value === 'number' && Number.isFinite(value) && Number.isInteger(value) && value >= 0) {
    return value;
  }
  throw new Error(`${context}.${key} must be an unsigned integer`);
}

export function boolValue(value: unknown | undefined): boolean {
  if (typeof value === 'boolean') {
    return value;
  }
  throw new FlareSdkException('invalidParameter', 'wire field must be a boolean', 'wire.decode', { expected: 'boolean' });
}

export function requiredStringList(value: unknown | undefined, fieldName: string, context: string): string[] {
  if (!Array.isArray(value)) {
    throw new FlareSdkException('invalidParameter', `${context}.${fieldName} must be an array`, 'wire.decode', { field: `${context}.${fieldName}`, expected: 'array' });
  }
  return value.map((item, index) => {
    if (typeof item !== 'string') {
      throw new FlareSdkException('invalidParameter', `${context}.${fieldName}[${index}] must be a string`, 'wire.decode', { field: `${context}.${fieldName}[${index}]`, expected: 'string' });
    }
    return item;
  });
}

export function requiredStringMap(value: unknown | undefined, fieldName: string, context: string): Record<string, string> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new FlareSdkException('invalidParameter', `${context}.${fieldName} must be an object`, 'wire.decode', { field: `${context}.${fieldName}`, expected: 'object' });
  }
  const out: Record<string, string> = {};
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    if (typeof item !== 'string') {
      throw new FlareSdkException('invalidParameter', `${context}.${fieldName}.${key} must be a string`, 'wire.decode', { field: `${context}.${fieldName}.${key}`, expected: 'string' });
    }
    out[key] = item;
  }
  return out;
}

export function recordValue(value: unknown | undefined): Record<string, unknown> {
  if (value === undefined || value === null || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

export function messageLocalStateFromJson(value: unknown | undefined): MessageLocalState | undefined {
  if (value === undefined || value === null || typeof value !== 'object' || Array.isArray(value)) {
    return undefined;
  }
  const json = value as Record<string, unknown>;
  return {
    sending: boolValue(field(json, 'sending')),
    failed: boolValue(field(json, 'failed')),
    isLocal: boolValue(field(json, 'isLocal')),
    uploading: boolValue(field(json, 'uploading')),
    uploadProgress: intValue(field(json, 'uploadProgress')),
    sortTs: intValue(field(json, 'sortTs')),
  };
}

export function richDocV2NormalizedFromJson(value: unknown | undefined): RichDocV2Normalized {
  const json = recordValue(value);
  const sourcePayload = field(json, 'sourcePayload');
  return {
    docJson: stringValue(field(json, 'docJson')),
    contentSchema: stringValue(field(json, 'contentSchema')),
    version: intValue(field(json, 'version')),
    plainText: stringValue(field(json, 'plainText')),
    searchText: stringValue(field(json, 'searchText')),
    renderHints: recordValue(field(json, 'renderHints')),
    ...(field(json, 'inputFormat') !== undefined ? { inputFormat: stringValue(field(json, 'inputFormat')) } : {}),
    ...(sourcePayload !== undefined && sourcePayload !== null ? { sourcePayload: recordValue(sourcePayload) } : {}),
  };
}

const NAME_SOURCE_WIRE_VALUES: NameSource[] = [NameSource.Alias, NameSource.MemberNickname, NameSource.Profile, NameSource.Snapshot, NameSource.Remark, NameSource.Name];
const NAME_ENTITY_WIRE_VALUES: NameEntity[] = [NameEntity.Person, NameEntity.Group, NameEntity.System, NameEntity.Ai, NameEntity.Customer, NameEntity.Channel, NameEntity.Broadcast, NameEntity.Temp];

function unknownPersonName(): DisplayName {
  return { kind: DisplayNameKind.Unnamed, entity: NameEntity.Person, idTail: '' };
}

/**
 * A projected name: conversation `title` and `lastSender`, message `sender`, participant `name`. An absent value reads
 * as undefined; {@link requiredDisplayNameField} reads the names every value carries. A value that is there never
 * fails, by the rule the kits read names with: an unknown `entity` reads as `person`, an unknown `kind`, a `named`
 * value without text or a value that is not an object reads as an unnamed person without an id tail, an unknown
 * `source` is dropped.
 */
export function displayNameFromJson(value: unknown | undefined): DisplayName | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== 'object' || Array.isArray(value)) {
    return unknownPersonName();
  }
  const json = value as Record<string, unknown>;
  switch (field(json, 'kind')) {
    case DisplayNameKind.Named: {
      const text = field(json, 'text');
      if (typeof text !== 'string') {
        return unknownPersonName();
      }
      const source = field(json, 'source');
      return typeof source === 'string' && (NAME_SOURCE_WIRE_VALUES as string[]).includes(source)
        ? { kind: DisplayNameKind.Named, text, source: source as NameSource }
        : { kind: DisplayNameKind.Named, text };
    }
    case DisplayNameKind.Members: {
      const names = field(json, 'names');
      const total = field(json, 'total');
      return {
        kind: DisplayNameKind.Members,
        names: Array.isArray(names) ? names.filter((name): name is string => typeof name === 'string') : [],
        total: typeof total === 'number' && Number.isFinite(total) && total >= 0 ? Math.trunc(total) : 0,
      };
    }
    case DisplayNameKind.Unnamed: {
      const entity = field(json, 'entity');
      const idTail = field(json, 'idTail');
      return {
        kind: DisplayNameKind.Unnamed,
        entity: typeof entity === 'string' && (NAME_ENTITY_WIRE_VALUES as string[]).includes(entity) ? (entity as NameEntity) : NameEntity.Person,
        idTail: typeof idTail === 'string' ? idTail : '',
      };
    }
    default:
      return unknownPersonName();
  }
}

/**
 * A name the value always carries: conversation `title`, message `sender`, participant `name`. The core and this
 * package ship together and the core writes these on every value, so a value without one is a contract error, as
 * with any other required field.
 */
export function requiredDisplayNameField(json: Record<string, unknown>, key: string, context: string): DisplayName {
  const name = displayNameFromJson(field(json, key));
  if (name === undefined) {
    throw new Error(`${context}.${key} is required`);
  }
  return name;
}

export function messagePreviewFromJson(value: unknown | undefined): MessagePreview | undefined {
  if (value === undefined || value === null || typeof value !== 'object') {
    return undefined;
  }
  const json = value as Record<string, unknown>;
  return {
    messageId: requiredStringField(json, 'messageId', 'MessagePreview'),
    senderId: requiredStringField(json, 'senderId', 'MessagePreview'),
    type: requiredIntField(json, 'type', 'MessagePreview'),
    text: requiredStringField(json, 'text', 'MessagePreview'),
    time: requiredIntField(json, 'time', 'MessagePreview'),
  };
}

export function conversationParticipantFromJson(value: unknown | undefined): ConversationParticipant {
  const json = recordValue(value);
  return {
    userId: requiredStringField(json, 'userId', 'ConversationParticipant'),
    roles: requiredStringList(field(json, 'roles'), 'roles', 'ConversationParticipant'),
    muted: boolValue(field(json, 'muted')),
    pinned: boolValue(field(json, 'pinned')),
    attributes: requiredStringMap(field(json, 'attributes'), 'attributes', 'ConversationParticipant'),
    joinedAt: requiredIntField(json, 'joinedAt', 'ConversationParticipant'),
    nickname: requiredStringField(json, 'nickname', 'ConversationParticipant'),
    name: requiredDisplayNameField(json, 'name', 'ConversationParticipant'),
  };
}

const MESSAGE_RECEIPT_STATUS_WIRE_VALUES: MessageReceiptStatus[] = [MessageReceiptStatus.Pending, MessageReceiptStatus.Sending, MessageReceiptStatus.Sent, MessageReceiptStatus.Read, MessageReceiptStatus.Failed];

/** The core's receipt projection; undefined when absent or malformed (an older core). */
export function messageReceiptViewFromJson(value: unknown | undefined): MessageReceiptView | undefined {
  if (value === undefined || value === null || typeof value !== 'object' || Array.isArray(value)) {
    return undefined;
  }
  const json = value as Record<string, unknown>;
  const status = field(json, 'status');
  if (typeof status !== 'string' || !(MESSAGE_RECEIPT_STATUS_WIRE_VALUES as string[]).includes(status)) {
    return undefined;
  }
  const rawSummary = field(json, 'readSummary');
  const summary = rawSummary !== undefined && rawSummary !== null && typeof rawSummary === 'object' && !Array.isArray(rawSummary)
    ? (rawSummary as Record<string, unknown>)
    : undefined;
  const count = (raw: unknown): number | undefined =>
    typeof raw === 'number' && Number.isInteger(raw) && raw >= 0 ? raw : undefined;
  const readCount = summary ? count(field(summary, 'readCount')) : undefined;
  const unreadCount = summary ? count(field(summary, 'unreadCount')) : undefined;
  const total = summary ? count(field(summary, 'total')) : undefined;
  const readSummary: MessageReadSummary | undefined =
    readCount !== undefined && unreadCount !== undefined && total !== undefined && total > 0
      ? { readCount, unreadCount, total }
      : undefined;
  return {
    status: status as MessageReceiptStatus,
    ...(readSummary !== undefined ? { readSummary } : {}),
  };
}

export function reactionEntryFromJson(value: unknown | undefined): ReactionEntry {
  const json = recordValue(value);
  return {
    emoji: requiredStringField(json, 'emoji', 'ReactionEntry'),
    userIds: requiredStringList(field(json, 'userIds'), 'userIds', 'ReactionEntry'),
    count: requiredIntField(json, 'count', 'ReactionEntry'),
  };
}

function messageContentTypeFromJson(value: unknown | undefined): MessageContentType {
  const raw = typeof value === 'string' ? value.trim() : '';
  if ((MESSAGE_CONTENT_TYPE_WIRE_VALUES as string[]).includes(raw)) {
    return raw as MessageContentType;
  }
  throw new FlareSdkException('invalidParameter', `invalid message content type: ${raw || '<empty>'}`, 'wire.message.decode', { field: 'content.contentType' });
}

function messageContentDataFromJson(rawContent: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(rawContent)) {
    if (key === 'contentType' || key === 'messageType') {
      continue;
    }
    out[key] = item;
  }
  return out;
}

export function messageFromJson(value: unknown | undefined): Message {
  const json = recordValue(value);
  const rawContent = field(json, 'content') as Record<string, unknown> | undefined;
  const content: MessageContent | undefined = rawContent
    ? {
        contentType: messageContentTypeFromJson(field(rawContent, 'contentType')),
        data: messageContentDataFromJson(rawContent),
      }
    : undefined;
  return {
    serverId: requiredStringField(json, 'serverId', 'Message'),
    clientMsgId: requiredStringField(json, 'clientMsgId', 'Message'),
    conversationId: requiredStringField(json, 'conversationId', 'Message'),
    conversationType: requiredIntField(json, 'conversationType', 'Message'),
    channelId: requiredStringField(json, 'channelId', 'Message'),
    senderId: requiredStringField(json, 'senderId', 'Message'),
    source: requiredIntField(json, 'source', 'Message'),
    conversationSeq: requiredIntField(json, 'conversationSeq', 'Message'),
    createdAt: requiredIntField(json, 'createdAt', 'Message'),
    clientCreatedAt: requiredIntField(json, 'clientCreatedAt', 'Message'),
    messageType: requiredIntField(json, 'messageType', 'Message'),
    ...(content !== undefined ? { content } : {}),
    senderName: requiredStringField(json, 'senderName', 'Message'),
    senderAvatar: requiredStringField(json, 'senderAvatar', 'Message'),
    senderDisplayName: requiredStringField(json, 'senderDisplayName', 'Message'),
    sender: requiredDisplayNameField(json, 'sender', 'Message'),
    replyTo: field(json, 'replyTo') as string | undefined,
    quotePreview: field(json, 'quotePreview') as string | undefined,
    threadId: field(json, 'threadId') as string | undefined,
    status: requiredIntField(json, 'status', 'Message'),
    isRead: boolValue(field(json, 'isRead')),
    isRecalled: boolValue(field(json, 'isRecalled')),
    isEdited: boolValue(field(json, 'isEdited')),
    mentionUsers: requiredStringList(field(json, 'mentionUsers'), 'mentionUsers', 'Message'),
    mentionAll: boolValue(field(json, 'mentionAll')),
    attributes: requiredStringMap(field(json, 'attributes'), 'attributes', 'Message'),
    extensions: recordValue(field(json, 'extensions')),
    reactions: requiredListOfMaps(field(json, 'reactions'), 'reactions', 'Message').map((item) => reactionEntryFromJson(item)),
    textPreview: requiredStringField(json, 'textPreview', 'Message'),
    version: requiredIntField(json, 'version', 'Message'),
    updatedAt: requiredIntField(json, 'updatedAt', 'Message'),
    ...(field(json, 'localState') !== undefined ? { localState: messageLocalStateFromJson(field(json, 'localState')) } : {}),
    timelineKey: requiredStringField(json, 'timelineKey', 'Message'),
    timelineSortTs: requiredIntField(json, 'timelineSortTs', 'Message'),
    ...(messageReceiptViewFromJson(field(json, 'receipt')) !== undefined ? { receipt: messageReceiptViewFromJson(field(json, 'receipt')) } : {}),
  } as Message;
}

export function sendAckFromJson(value: unknown | undefined): SendMessageResponse {
  const json = (value ?? {}) as Record<string, unknown>;
  return {
    ackId: requiredStringField(json, 'ackId', 'SendMessageResponse'),
    serverId: requiredStringField(json, 'serverId', 'SendMessageResponse'),
    clientMsgId: requiredStringField(json, 'clientMsgId', 'SendMessageResponse'),
    conversationId: requiredStringField(json, 'conversationId', 'SendMessageResponse'),
    seq: requiredIntField(json, 'seq', 'SendMessageResponse'),
    timestamp: requiredIntField(json, 'timestamp', 'SendMessageResponse'),
    success: boolValue(field(json, 'success')),
    errorCode: intValue(field(json, 'errorCode')),
    errorMessage: stringValue(field(json, 'errorMessage')),
  };
}

export function conversationVersionFromJson(value: unknown | undefined): ConversationVersion {
  const json = recordValue(value);
  return {
    conversationId: requiredStringField(json, 'conversationId', 'ConversationVersion'),
    version: requiredIntField(json, 'version', 'ConversationVersion'),
  };
}

export function syncConversationSummariesResponseFromJson(value: unknown | undefined): SyncConversationSummariesResponse {
  const json = recordValue(value);
  return {
    changedConversations: requiredListOfMaps(field(json, 'changedConversations'), 'changedConversations', 'SyncConversationSummariesResponse').map((item) => conversationVersionFromJson(item)),
  };
}

export function listConversationsResponseFromJson(value: unknown | undefined): ListConversationsResponse {
  const json = recordValue(value);
  return {
    conversations: requiredListOfMaps(field(json, 'conversations'), 'conversations', 'ListConversationsResponse').map((item) => conversationFromJson(item)),
  };
}

export function conversationFromJson(json: Record<string, unknown>): Conversation {
  const typeRaw = field(json, 'conversationType');
  const conversationType = typeof typeRaw === 'string' ? typeRaw.trim() : '';
  if (!(CONVERSATION_TYPE_WIRE_VALUES as string[]).includes(conversationType)) {
    throw new FlareSdkException('invalidParameter', `invalid conversation type: ${conversationType || '<empty>'}`, 'wire.conversation.decode', { field: 'conversationType' });
  }
  const lastSender = displayNameFromJson(field(json, 'lastSender'));
  return {
    conversationId: requiredStringField(json, 'conversationId', 'Conversation'),
    conversationType: conversationType as ConversationType,
    businessType: requiredStringField(json, 'businessType', 'Conversation'),
    channelId: requiredStringField(json, 'channelId', 'Conversation'),
    membersCount: requiredIntField(json, 'membersCount', 'Conversation'),
    displayName: requiredStringField(json, 'displayName', 'Conversation'),
    title: requiredDisplayNameField(json, 'title', 'Conversation'),
    avatarUrl: requiredStringField(json, 'avatarUrl', 'Conversation'),
    remark: field(json, 'remark') as string | undefined,
    description: field(json, 'description') as string | undefined,
    lastMessageId: field(json, 'lastMessageId') as string | undefined,
    lastSenderId: field(json, 'lastSenderId') as string | undefined,
    lastMessageAt: field(json, 'lastMessageAt') !== undefined ? intValue(field(json, 'lastMessageAt')) : undefined,
    lastMessagePreview: field(json, 'lastMessagePreview') as string | undefined,
    lastMessage: messagePreviewFromJson(field(json, 'lastMessage')),
    lastSenderNickname: requiredStringField(json, 'lastSenderNickname', 'Conversation'),
    ...(lastSender !== undefined ? { lastSender } : {}),
    lastSenderAvatarUrl: requiredStringField(json, 'lastSenderAvatarUrl', 'Conversation'),
    unreadCount: requiredIntField(json, 'unreadCount', 'Conversation'),
    lastReadSeq: requiredIntField(json, 'lastReadSeq', 'Conversation'),
    peerReadSeq: requiredIntField(json, 'peerReadSeq', 'Conversation'),
    maxSeq: requiredIntField(json, 'maxSeq', 'Conversation'),
    visibleAfterSeq: requiredIntField(json, 'visibleAfterSeq', 'Conversation'),
    isPinned: boolValue(field(json, 'isPinned')),
    isMuted: boolValue(field(json, 'isMuted')),
    ...(typeof field(json, 'isMentionOnly') === 'boolean' ? { isMentionOnly: field(json, 'isMentionOnly') as boolean } : {}),
    notifyAll: field(json, 'notifyAll') === true,
    isArchived: boolValue(field(json, 'isArchived')),
    version: intValue(field(json, 'version')),
    updatedAt: requiredIntField(json, 'updatedAt', 'Conversation'),
    createdAt: requiredIntField(json, 'createdAt', 'Conversation'),
    updatedAtTs: field(json, 'updatedAtTs') !== undefined ? intValue(field(json, 'updatedAtTs')) : undefined,
    ext: requiredStringMap(field(json, 'ext'), 'ext', 'Conversation'),
    participantVersion: requiredIntField(json, 'participantVersion', 'Conversation'),
    memberPreview: requiredListOfMaps(field(json, 'memberPreview'), 'memberPreview', 'Conversation').map(conversationParticipantFromJson),
    participants: requiredListOfMaps(field(json, 'participants'), 'participants', 'Conversation').map(conversationParticipantFromJson),
    draft: field(json, 'draft') as string | undefined,
    mentionCount: requiredIntField(json, 'mentionCount', 'Conversation'),
    mentionMe: boolValue(field(json, 'mentionMe')),
    badge: field(json, 'badge') as string | undefined,
    role: field(json, 'role') as string | undefined,
  } as Conversation;
}

export function listMessagesResponseFromJson(value: unknown | undefined): ListMessagesResponse {
  const json = (value ?? {}) as Record<string, unknown>;
  return {
    messages: requiredListOfMaps(field(json, 'messages'), 'messages', 'ListMessagesResponse').map((item) => messageFromJson(item)),
  };
}

export function timelineSyncStateFromJson(value: unknown | undefined): TimelineSyncState {
  const raw = typeof value === 'string' ? value.trim() : '';
  if ((TIMELINE_SYNC_STATE_WIRE_VALUES as string[]).includes(raw)) {
    return raw as TimelineSyncState;
  }
  throw new FlareSdkException('invalidParameter', `invalid timeline sync state: ${raw || '<empty>'}`, 'wire.timeline.decode', { field: 'syncState' });
}

export function homeTimelineSnapshotFromJson(value: unknown | undefined): HomeTimelineSnapshot {
  const json = (value ?? {}) as Record<string, unknown>;
  return {
    conversations: requiredListOfMaps(field(json, 'conversations'), 'conversations', 'HomeTimelineSnapshot').map((item) => conversationFromJson(item)),
    totalUnread: intValue(field(json, 'totalUnread')),
    syncState: timelineSyncStateFromJson(field(json, 'syncState')),
  };
}

export function conversationTimelineSnapshotFromJson(value: unknown | undefined): ConversationTimelineSnapshot {
  const json = (value ?? {}) as Record<string, unknown>;
  const conversation = field(json, 'conversation');
  return {
    ...(conversation !== undefined && conversation !== null && typeof conversation === 'object'
      ? { conversation: conversationFromJson(conversation as Record<string, unknown>) }
      : {}),
    messages: requiredListOfMaps(field(json, 'messages'), 'messages', 'ConversationTimelineSnapshot').map((item) => messageFromJson(item)),
    hasMore: boolValue(field(json, 'hasMore')),
  };
}

function viewTypeFromJson(value: unknown | undefined, context: string): 'timeline' | 'conversationList' {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (raw === 'timeline' || raw === 'conversationList') {
    return raw;
  }
  throw new FlareSdkException('invalidParameter', `invalid view type: ${raw || '<empty>'}`, 'wire.view.decode', { field: `${context}.viewType` });
}

export function timelineViewSnapshotFromJson(value: unknown | undefined): TimelineViewSnapshot {
  const json = (value ?? {}) as Record<string, unknown>;
  const conversation = field(json, 'conversation');
  const anchorKey = field(json, 'anchorKey');
  return {
    ...(conversation !== undefined && conversation !== null && typeof conversation === 'object'
      ? { conversation: conversationFromJson(conversation as Record<string, unknown>) }
      : {}),
    messages: requiredListOfMaps(field(json, 'messages'), 'messages', 'TimelineViewSnapshot').map((item) => messageFromJson(item)),
    hasOlder: boolValue(field(json, 'hasOlder')),
    hasNewer: boolValue(field(json, 'hasNewer')),
    attachedToLatest: boolValue(field(json, 'attachedToLatest')),
    ...(typeof anchorKey === 'string' && anchorKey.trim() !== '' ? { anchorKey } : {}),
  };
}

export function viewSnapshotFromJson(value: unknown | undefined): ViewSnapshot {
  const json = (value ?? {}) as Record<string, unknown>;
  const viewType = viewTypeFromJson(field(json, 'viewType'), 'ViewSnapshot');
  if (viewType === 'timeline') {
    return {
      viewType: 'timeline',
      data: timelineViewSnapshotFromJson(field(json, 'data')),
    };
  }
  return {
    viewType: 'conversationList',
    data: homeTimelineSnapshotFromJson(field(json, 'data')),
  };
}

export function viewOpenResponseFromJson(value: unknown | undefined): ViewOpenResponse {
  const json = (value ?? {}) as Record<string, unknown>;
  return {
    viewId: requiredStringField(json, 'viewId', 'ViewOpenResponse'),
    revision: requiredIntField(json, 'revision', 'ViewOpenResponse'),
    snapshot: viewSnapshotFromJson(field(json, 'snapshot')),
  };
}

export function timelineViewPageResponseFromJson(value: unknown | undefined): TimelineViewPageResponse {
  const json = (value ?? {}) as Record<string, unknown>;
  return {
    viewId: requiredStringField(json, 'viewId', 'TimelineViewPageResponse'),
    loadedCount: requiredIntField(json, 'loadedCount', 'TimelineViewPageResponse'),
    revision: requiredIntField(json, 'revision', 'TimelineViewPageResponse'),
  };
}

function viewDeltaOpKindFromJson(value: unknown | undefined): string {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (raw === 'insert' || raw === 'update' || raw === 'remove' || raw === 'move') {
    return raw;
  }
  throw new FlareSdkException('invalidParameter', `invalid view delta op: ${raw || '<empty>'}`, 'wire.view.decode', { field: 'ViewDeltaOp.op' });
}

export function viewDeltaOpFromJson(value: unknown | undefined): ViewDeltaOp {
  const json = (value ?? {}) as Record<string, unknown>;
  const fromIndex = field(json, 'fromIndex');
  const item = field(json, 'item');
  if (item !== undefined && item !== null && (typeof item !== 'object' || Array.isArray(item))) {
    throw new FlareSdkException('invalidParameter', 'ViewDeltaOp.item must be an object', 'wire.view.decode', { field: 'ViewDeltaOp.item', expected: 'object' });
  }
  return {
    op: viewDeltaOpKindFromJson(field(json, 'op')),
    key: requiredStringField(json, 'key', 'ViewDeltaOp'),
    index: requiredIntField(json, 'index', 'ViewDeltaOp'),
    ...(fromIndex !== undefined ? { fromIndex: requiredIntField(json, 'fromIndex', 'ViewDeltaOp') } : {}),
    ...(item !== undefined && item !== null && typeof item === 'object'
      ? { item: item as Record<string, unknown> }
      : {}),
  };
}

function viewDeltaRowKeys(value: unknown, fieldName: string): string[] {
  if (!Array.isArray(value) || value.some((key) => typeof key !== 'string')) {
    throw new FlareSdkException('invalidParameter', `ViewDelta.${fieldName} must be a list of row keys`, 'wire.view.decode', { field: `ViewDelta.${fieldName}`, expected: 'string[]' });
  }
  return value as string[];
}

/**
 * Timeline deltas work by row key (`clientMsgId`, else `serverId`, else
 * `seq:<conversationId>:<conversationSeq>`): remove, upsert in place, prepend, append, then
 * reorder when present. Conversation-list deltas keep the indexed `ops`.
 */
export function viewDeltaFromJson(value: unknown | undefined): ViewDelta {
  const json = (value ?? {}) as Record<string, unknown>;
  const viewType = viewTypeFromJson(field(json, 'viewType'), 'ViewDelta');
  const conversation = field(json, 'conversation');
  if (viewType === 'conversationList') {
    const totalUnread = field(json, 'totalUnread');
    const syncState = field(json, 'syncState');
    return {
      viewType,
      ops: requiredListOfMaps(field(json, 'ops'), 'ops', 'ViewDelta').map((item) => viewDeltaOpFromJson(item)),
      ...(totalUnread !== undefined ? { totalUnread: intValue(totalUnread) } : {}),
      ...(syncState !== undefined ? { syncState: timelineSyncStateFromJson(syncState) } : {}),
    };
  }
  const rows = (name: string) => {
    const list = field(json, name);
    return list === undefined || list === null
      ? []
      : requiredListOfMaps(list, name, 'ViewDelta').map((item) => messageFromJson(item));
  };
  const remove = field(json, 'remove');
  const reorder = field(json, 'reorder');
  return {
    viewType,
    prepend: rows('prepend'),
    append: rows('append'),
    upsert: rows('upsert'),
    remove: remove === undefined || remove === null ? [] : viewDeltaRowKeys(remove, 'remove'),
    ...(reorder !== undefined && reorder !== null ? { reorder: viewDeltaRowKeys(reorder, 'reorder') } : {}),
    hasOlder: boolValue(field(json, 'hasOlder')),
    hasNewer: boolValue(field(json, 'hasNewer')),
    attachedToLatest: boolValue(field(json, 'attachedToLatest')),
    ...(conversation !== undefined && conversation !== null && typeof conversation === 'object'
      ? { conversation: conversationFromJson(conversation as Record<string, unknown>) }
      : {}),
  };
}

export function viewUpdateFromJson(value: unknown | undefined): ViewUpdate {
  const json = (value ?? {}) as Record<string, unknown>;
  const rawKind = field(json, 'kind');
  const kind = typeof rawKind === 'string' ? rawKind.trim() : '';
  if (kind !== 'snapshot' && kind !== 'delta') {
    throw new FlareSdkException('invalidParameter', `invalid view update kind: ${kind || '<empty>'}`, 'view.update.decode', { field: 'kind' });
  }
  const update: ViewUpdate = {
    viewId: requiredStringField(json, 'viewId', 'ViewUpdate'),
    revision: requiredIntField(json, 'revision', 'ViewUpdate'),
    kind,
  };
  if (update.kind === 'delta') {
    return { ...update, delta: viewDeltaFromJson(field(json, 'delta')) };
  }
  return { ...update, snapshot: viewSnapshotFromJson(field(json, 'snapshot')) };
}

export function closeViewResponseFromJson(value: unknown | undefined): CloseViewResponse {
  const json = (value ?? {}) as Record<string, unknown>;
  return {
    closed: boolValue(field(json, 'closed')),
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
