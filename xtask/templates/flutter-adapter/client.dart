// Flutter runtime adapter over generated contracts.

import '../api/api.dart';
import '../callback/callback.dart';
import '../contract/bridge_contract.dart';
import '../model/model.dart';
import 'events/default_events_api.dart';
import '../flare_core_sdk.dart';
import 'module/default_message_builder_api.dart';
import 'codec/wire_codec.dart';

/// Default high-level Flutter client implementation.
final class DefaultFlareImClient implements FlareImClient {
  DefaultFlareImClient(this._bridge)
      : events = DefaultEventsApi(_bridge),
        connection = _DefaultConnectionApi(_bridge),
        messageBuilder = DefaultMessageBuilderApi(_bridge),
        conversations = _DefaultConversationsApi(_bridge),
        messages = _DefaultMessagesApi(_bridge),
        sync = _DefaultSyncApi(_bridge),
        user = _DefaultUserApi(_bridge),
        presence = _DefaultPresenceApi(_bridge),
        media = _DefaultMediaApi(_bridge),
        capabilities = _DefaultCapabilitiesApi(_bridge),
        views = _DefaultViewsApi(_bridge),
        diagnostics = _DefaultDiagnosticsApi(_bridge);

  final NativeBridge _bridge;

  @override
  final DefaultEventsApi events;
  @override
  final ConnectionApi connection;
  @override
  final MessageBuilderApi messageBuilder;
  @override
  final ConversationsApi conversations;
  @override
  final MessagesApi messages;
  @override
  final SyncApi sync;
  @override
  final UserApi user;
  @override
  final PresenceApi presence;
  @override
  final MediaApi media;
  @override
  final CapabilitiesApi capabilities;
  @override
  final ViewsApi views;
  @override
  final DiagnosticsApi diagnostics;

  @override
  Future<Map<String, Object?>> create(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.sdkCreate, request);
  }

  @override
  Future<void> init(Map<String, Object?> request) async {
    events.emitLifecycle(const LifecycleEvent(
      name: LifecycleEventName.initializing,
      operation: 'sdk.init',
    ));
    try {
      await _bridge.invoke<void>(NativeCallMap.sdkInit, request);
      events.emitLifecycle(const LifecycleEvent(
        name: LifecycleEventName.initialized,
        operation: 'sdk.init',
      ));
    } catch (error) {
      events.emitLifecycle(LifecycleEvent(
        name: LifecycleEventName.initFailed,
        operation: 'sdk.init',
        error: sdkErrorFromException(error, 'sdk.init'),
      ));
      rethrow;
    }
  }

  @override
  Future<void> uninit() => _bridge.invoke<void>(NativeCallMap.sdkUninit);

  @override
  Future<void> login(Map<String, Object?> request) async {
    try {
      await _bridge.invoke<void>(NativeCallMap.sdkLogin, request);
      events.emitLifecycle(LifecycleEvent(
        name: LifecycleEventName.loginSucceeded,
        operation: 'sdk.login',
        userId: request['userId']?.toString(),
      ));
    } catch (error) {
      events.emitLifecycle(LifecycleEvent(
        name: LifecycleEventName.loginFailed,
        operation: 'sdk.login',
        userId: request['userId']?.toString(),
        error: sdkErrorFromException(error, 'sdk.login'),
      ));
      rethrow;
    }
  }

  @override
  Future<void> prepare(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.sdkPrepare, request);
  }

  @override
  Future<void> connect(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.sdkConnect, request);
  }

  @override
  Future<void> updateAccessToken(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.sdkUpdateAccessToken, request);
  }

  @override
  Future<void> setHeartbeatAppState(SetHeartbeatAppStateRequest request) {
    return _bridge.invoke<void>(
      NativeCallMap.sdkSetHeartbeatAppState,
      setHeartbeatAppStateRequestToMap(request),
    );
  }

  @override
  Future<void> setHeartbeatNatTimeout(SetHeartbeatNatTimeoutRequest request) {
    return _bridge.invoke<void>(
      NativeCallMap.sdkSetHeartbeatNatTimeout,
      setHeartbeatNatTimeoutRequestToMap(request),
    );
  }

  @override
  Future<HeartbeatEffectiveIntervalResponse>
      heartbeatEffectiveInterval() async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.sdkHeartbeatEffectiveInterval,
    );
    return heartbeatEffectiveIntervalResponseFromJson(raw);
  }

  @override
  Future<PushLifecycleStatus> updatePushToken(PushTokenInput request) async {
    final raw = mapValue(await _bridge.invoke<Object?>(NativeCallMap.sdkUpdatePushToken, {'provider': request.provider, 'platform': request.platform, 'token': request.token}));
    return PushLifecycleStatus(configuredProviders: intValue(raw['configuredProviders']), pendingRevocations: intValue(raw['pendingRevocations']), registeredProviders: intValue(raw['registeredProviders']), state: raw['state'] as String, lastError: raw['lastError'] as String?);
  }

  @override
  Future<PushLifecycleStatus> clearPushToken(ClearPushTokenRequest request) async {
    final raw = mapValue(await _bridge.invoke<Object?>(NativeCallMap.sdkClearPushToken, {'provider': request.provider}));
    return PushLifecycleStatus(configuredProviders: intValue(raw['configuredProviders']), pendingRevocations: intValue(raw['pendingRevocations']), registeredProviders: intValue(raw['registeredProviders']), state: raw['state'] as String, lastError: raw['lastError'] as String?);
  }

  @override
  Future<PushLifecycleStatus> setPushPermission(SetPushPermissionRequest request) async {
    final raw = mapValue(await _bridge.invoke<Object?>(NativeCallMap.sdkSetPushPermission, {'allowed': request.allowed}));
    return PushLifecycleStatus(configuredProviders: intValue(raw['configuredProviders']), pendingRevocations: intValue(raw['pendingRevocations']), registeredProviders: intValue(raw['registeredProviders']), state: raw['state'] as String, lastError: raw['lastError'] as String?);
  }

  @override
  Future<PushLifecycleStatus> reconcilePush() async {
    final raw = mapValue(await _bridge.invoke<Object?>(NativeCallMap.sdkReconcilePush));
    return PushLifecycleStatus(configuredProviders: intValue(raw['configuredProviders']), pendingRevocations: intValue(raw['pendingRevocations']), registeredProviders: intValue(raw['registeredProviders']), state: raw['state'] as String, lastError: raw['lastError'] as String?);
  }

  @override
  Future<PushLifecycleStatus> pushStatus() async {
    final raw = mapValue(await _bridge.invoke<Object?>(NativeCallMap.sdkPushStatus));
    return PushLifecycleStatus(configuredProviders: intValue(raw['configuredProviders']), pendingRevocations: intValue(raw['pendingRevocations']), registeredProviders: intValue(raw['registeredProviders']), state: raw['state'] as String, lastError: raw['lastError'] as String?);
  }

  @override
  Future<void> logout() async {
    await _bridge.invoke<void>(NativeCallMap.sdkLogout);
    events.emitLifecycle(const LifecycleEvent(
      name: LifecycleEventName.loggedOut,
      operation: 'sdk.logout',
    ));
  }

  @override
  Future<void> dispose() async {
    await events.unsubscribeAll();
    await _bridge.invoke<void>(NativeCallMap.sdkDispose);
    events.emitLifecycle(const LifecycleEvent(
      name: LifecycleEventName.disposed,
      operation: 'sdk.dispose',
    ));
  }

  @override
  Future<void> hardReset() => _bridge.invoke<void>(NativeCallMap.sdkHardReset);

  @override
  Future<Map<String, Object?>> currentUserId() {
    return _bridge.invoke<Map<String, Object?>>(NativeCallMap.sdkCurrentUserId);
  }

  @override
  Future<bool> isConnected() =>
      _bridge.invoke<bool>(NativeCallMap.sdkIsConnected);

  @override
  Future<bool> sessionActive() =>
      _bridge.invoke<bool>(NativeCallMap.sdkSessionActive);

}

final class _DefaultConnectionApi implements ConnectionApi {
  const _DefaultConnectionApi(this._bridge);

  final NativeBridge _bridge;

  @override
  Future<ConnectionState> getConnectionState() {
    return _bridge.invoke<ConnectionState>(NativeCallMap.connectionGetState);
  }

  @override
  Future<void> disconnect() =>
      _bridge.invoke<void>(NativeCallMap.connectionDisconnect);

  @override
  Future<ServerClockResponse> getServerClock() async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.connectionServerClock,
    );
    return serverClockResponseFromJson(raw);
  }

  @override
  Future<NetworkChangeResponse> notifyNetworkChange(
    NetworkChangeRequest request,
  ) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.connectionNotifyNetworkChange,
      networkChangeRequestToMap(request),
    );
    return networkChangeResponseFromJson(raw);
  }
}

final class _DefaultConversationsApi implements ConversationsApi {
  const _DefaultConversationsApi(this._bridge);

  final NativeBridge _bridge;

  Future<ListConversationsResponse> _listConversations(
    NativeCallDescriptor descriptor, [
    Map<String, Object?>? request,
  ]) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      descriptor,
      request,
    );
    return listConversationsResponseFromJson(raw);
  }

  @override
  Future<ListConversationsResponse> listConversations() {
    return _listConversations(NativeCallMap.conversationList);
  }

  @override
  Future<ListConversationsResponse> listConversationsByQuery(
      ConversationListQuery request) {
    return _listConversations(
      NativeCallMap.conversationListByQuery,
      conversationListQueryToMap(request),
    );
  }

  @override
  Future<ListConversationsResponse> listConversationsIncludingArchived() {
    return _listConversations(NativeCallMap.conversationListIncludingArchived);
  }

  @override
  Future<Conversation> getConversation(Map<String, Object?> request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.conversationGet, request);
    return _conversationFromResponse(
        raw, NativeCallMap.conversationGet.operation);
  }

  @override
  Future<Conversation> getOneConversation(Map<String, Object?> request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.conversationGetOne, request);
    return _conversationFromResponse(
      raw,
      NativeCallMap.conversationGetOne.operation,
    );
  }

  @override
  Future<Conversation> getGroupConversationByUserIds(
      Map<String, Object?> request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.conversationGetGroupByUserIds, request);
    return _conversationFromResponse(
      raw,
      NativeCallMap.conversationGetGroupByUserIds.operation,
    );
  }

  @override
  Future<ListConversationsResponse> getMultipleConversations(
      Map<String, Object?> request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.conversationGetMultiple, request);
    return listConversationsResponseFromJson(raw);
  }

  @override
  Future<ListConversationsResponse> listConversationsPaginated(
      Map<String, Object?> request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.conversationListPaginated, request);
    return listConversationsResponseFromJson(raw);
  }

  @override
  Future<ListConversationsResponse> listRawConversations() {
    return _listConversations(NativeCallMap.conversationListRaw);
  }

  @override
  Future<ConversationTotalUnreadResponse> getTotalUnread() async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.conversationTotalUnread);
    return ConversationTotalUnreadResponse(
        totalUnread: intValue(raw['totalUnread']));
  }

  Conversation _conversationFromResponse(
    Map<String, Object?> raw,
    String operation,
  ) {
    final directId = raw['conversationId']?.toString().trim() ?? '';
    if (directId.isNotEmpty) {
      return conversationFromJson(raw);
    }

    final conversation = raw['conversation'];
    if (conversation is Map<dynamic, dynamic>) {
      return conversationFromJson(conversation);
    }

    final value = raw['value'];
    if (value is Map<dynamic, dynamic>) {
      return conversationFromJson(value);
    }

    final conversations = listOfMaps(raw['conversations']);
    if (conversations.isNotEmpty) {
      return conversationFromJson(conversations.first);
    }

    warnCodecOnce(
      'conversation.invalid.single.$operation',
      'Invalid Conversation response for $operation',
      {
        'keys': raw.keys.map((key) => key.toString()).toList(growable: false),
        'conversationType': raw['conversationType'],
      },
    );
    throw FormatException(
      'Conversation response missing required `conversationId` for $operation',
      raw.keys.map((key) => key.toString()).toList(growable: false),
    );
  }

  @override
  Future<HomeTimelineSnapshot> bootstrapHomeTimeline(
      BootstrapHomeTimelineRequest request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.conversationBootstrapHome,
      bootstrapHomeTimelineRequestToMap(request),
    );
    return homeTimelineSnapshotFromJson(raw);
  }

  @override
  Future<ConversationTimelineSnapshot> openConversationTimeline(
      OpenConversationTimelineRequest request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.conversationOpenTimeline,
      openConversationTimelineRequestToMap(request),
    );
    return conversationTimelineSnapshotFromJson(raw);
  }

  @override
  Future<void> markConversationRead(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.conversationMarkRead, request);
  }

  @override
  Future<void> setConversationPinned(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.conversationSetPinned, request);
  }

  @override
  Future<void> setConversationMuted(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.conversationSetMuted, request);
  }

  @override
  Future<void> setConversationArchived(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.conversationSetArchived, request);
  }

  @override
  Future<void> markConversationUnread(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.conversationMarkUnread, request);
  }

  @override
  Future<void> deleteConversation(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.conversationDelete, request);
  }

  @override
  Future<void> updateConversationDraft(UpdateConversationDraftRequest request) {
    return _bridge.invoke<void>(
      NativeCallMap.conversationUpdateDraft,
      updateConversationDraftRequestToMap(request),
    );
  }

  @override
  Future<void> clearLocalChatHistory(Map<String, Object?> request) {
    return _bridge.invoke<void>(
        NativeCallMap.conversationClearLocalChatHistory, request);
  }
}

final class _DefaultMessagesApi implements MessagesApi {
  _DefaultMessagesApi(this._bridge);

  final NativeBridge _bridge;

  Future<Map<String, Object?>> _dispatchMap(
    NativeCallDescriptor descriptor,
    Map<String, Object?> request,
  ) {
    return _bridge.invoke<Map<String, Object?>>(descriptor, request);
  }

  Future<void> _dispatchUnit(
    NativeCallDescriptor descriptor,
    Map<String, Object?> request,
  ) {
    return _bridge.invoke<void>(descriptor, request);
  }

  @override
  Future<Message> createTextMessage(CreateTextMessageRequest request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.messageCreateText,
      {
        'conversationId': request.conversationId,
        'text': request.text,
      },
    );
    return messageFromJson(raw);
  }

  @override
  Future<Map<String, Object?>> dispatchMessage(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.messageDispatch, request);
  }

  @override
  Future<SendMessageResponse> sendMessageNoOss(
      SendMessageRequest request) async {
    final raw = await _dispatchMap(
      NativeCallMap.messageSendNoOss,
      sendMessageRequestToMap(request),
    );
    return sendAckFromJson(raw);
  }

  @override
  Future<SendMessageResponse> sendMessage(
    SendMessageRequest request, [
    MessageSendCallback? callback,
  ]) async {
    final wireRequest = sendMessageRequestToMap(request);
    try {
      final result = await _bridge.invoke<Map<String, Object?>>(
          NativeCallMap.messageSend, wireRequest);
      final ack = sendAckFromJson(result);
      callback?.onSuccess(MessageSendAckEvent(ack: ack));
      return ack;
    } catch (error) {
      callback?.onFailure(MessageSendFailedEvent(
        clientMsgId: request.message.clientMsgId,
        reason: error.toString(),
        error: sdkErrorFromException(error, 'message.send'),
      ));
      rethrow;
    }
  }

  @override
  Future<ListMessagesResponse> listMessages(ListMessagesRequest request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.messageList,
      {
        'conversationId': request.conversationId,
        'beforeSeq': request.beforeSeq,
        'limit': request.limit,
      },
    );
    return listMessagesResponseFromJson(raw);
  }

  @override
  Future<void> recallMessage(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.messageRecall, request);
  }

  @override
  Future<void> editTextByMessageId(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageEditTextByMessageId, request);
  }

  @override
  Future<void> deleteMessage(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.messageDelete, request);
  }

  @override
  Future<void> deleteMessageForSelf(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageDeleteForSelf, request);
  }

  @override
  Future<void> deleteMessageForEveryone(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageDeleteForEveryone, request);
  }

  @override
  @override
  Future<void> markMessageReadAndBurn(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageMarkReadAndBurn, request);
  }

  @override
  Future<void> addReaction(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageAddReaction, request);
  }

  @override
  Future<void> removeReaction(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageRemoveReaction, request);
  }

  @override
  Future<void> pinMessage(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messagePin, request);
  }

  @override
  Future<void> unpinMessage(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageUnpin, request);
  }

  @override
  Future<void> pinMessageById(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messagePinByMessageId, request);
  }

  @override
  Future<void> unpinMessageById(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageUnpinByMessageId, request);
  }

  @override
  Future<void> markMessage(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageMark, request);
  }

  @override
  Future<void> markMessageWithColor(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageMarkWithColor, request);
  }

  @override
  Future<void> unmarkMessage(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageUnmark, request);
  }

  @override
  Future<void> markMessageById(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageMarkByMessageId, request);
  }

  @override
  Future<void> unmarkMessageById(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageUnmarkByMessageId, request);
  }

  @override
  Future<Message> getMessage(Map<String, Object?> request) async {
    final raw = await _dispatchMap(NativeCallMap.messageGet, request);
    return messageFromJson(raw['message'] ?? raw);
  }

  @override
  Future<Map<String, Object?>> getRawMessage(Map<String, Object?> request) {
    return _dispatchMap(NativeCallMap.messageGetRaw, request);
  }

  @override
  Future<ListMessagesResponse> searchMessages(
      MessageSearchQuery request) async {
    final raw = await _dispatchMap(
      NativeCallMap.messageSearch,
      messageSearchQueryToMap(request),
    );
    return listMessagesResponseFromJson(raw);
  }

  @override
  Future<ListMessagesResponse> searchMessagesByQuery(
      MessageSearchQuery request) async {
    final raw = await _dispatchMap(
      NativeCallMap.messageSearchByQuery,
      messageSearchQueryToMap(request),
    );
    return listMessagesResponseFromJson(raw);
  }

  @override
  Future<Map<String, Object?>> readReceiptMembers(MessageReadReceiptMembersRequest request) {
    return _dispatchMap(
      NativeCallMap.messageReadReceiptMembers,
      messageReadReceiptMembersRequestToMap(request),
    );
  }

  @override
  Future<ListMessagesResponse> searchMessagesInConversation(
      MessageSearchQuery request) async {
    final raw = await _dispatchMap(
      NativeCallMap.messageSearchInConversation,
      messageSearchQueryToMap(request),
    );
    return listMessagesResponseFromJson(raw);
  }

  @override
  Future<void> editRichDocByMessageId(Map<String, Object?> request) {
    return _dispatchUnit(NativeCallMap.messageEditRichDocByMessageId, request);
  }

  @override
  Future<void> setTyping(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.messageTyping, request);
  }
}

final class _DefaultSyncApi implements SyncApi {
  const _DefaultSyncApi(this._bridge);

  final NativeBridge _bridge;

  @override
  Future<void> syncConversationSummaries() {
    return _bridge.invoke<void>(NativeCallMap.syncConversationSummaries);
  }

  @override
  Future<SyncConversationSummariesResponse>
      syncConversationSummariesWithVersions(
          SyncConversationSummariesRequest request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.syncConversationSummariesWithVersions,
      syncConversationSummariesRequestToMap(request),
    );
    return syncConversationSummariesResponseFromJson(raw);
  }

  @override
  Future<StartupHomeSyncResponse> bootstrapStartupHome(
      StartupHomeSyncRequest request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.syncBootstrapStartupHome,
      startupHomeSyncRequestToMap(request),
    );
    return startupHomeSyncResponseFromJson(raw);
  }

  @override
  Future<ConversationHistoryBackfillResponse> backfillConversationHistory(
      ConversationHistoryBackfillRequest request) async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.syncConversationHistoryBackfill,
      conversationHistoryBackfillRequestToMap(request),
    );
    return conversationHistoryBackfillResponseFromJson(raw);
  }

  @override
  Future<void> syncConversation(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.syncConversation, request);
  }

  @override
  Future<void> syncMessages(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.syncMessages, request);
  }
}

final class _DefaultPresenceApi implements PresenceApi {
  const _DefaultPresenceApi(this._bridge);

  final NativeBridge _bridge;

  @override
  Future<Map<String, Object?>> getUserPresence(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.presenceGet, request);
  }

  @override
  Future<Map<String, Object?>> batchGetUserPresence(
      Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.presenceBatchGet, request);
  }

  @override
  Future<void> subscribeUserPresence(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.presenceSubscribe, request);
  }
}

/// User profile cache facade. Business pushes identity (name/avatar) here; reads
/// batch-join the cache to render current identity. Operation `user.upsert_profiles`
/// goes through the JSON contract invoke boundary (`flare_sdk_invoke_json`).
final class _DefaultUserApi implements UserApi {
  const _DefaultUserApi(this._bridge);

  final NativeBridge _bridge;

  @override
  Future<void> upsertUserProfiles(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.userUpsertProfiles, request);
  }
}

final class _DefaultMediaApi implements MediaApi {
  const _DefaultMediaApi(this._bridge);

  final NativeBridge _bridge;

  @override
  Future<Map<String, Object?>> uploadFile(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaUploadFile, request);
  }

  @override
  Future<Map<String, Object?>> uploadImage(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaUploadImage, request);
  }

  @override
  Future<Map<String, Object?>> uploadVideo(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaUploadVideo, request);
  }

  @override
  Future<Map<String, Object?>> uploadBytes(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaUploadBytes, request);
  }

  @override
  Future<Map<String, Object?>> deleteFile(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaDeleteFile, request);
  }

  @override
  Future<Map<String, Object?>> getMediaUrl(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaGetUrl, request);
  }

  @override
  Future<Map<String, Object?>> getTempDownloadUrl(
      Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaTempDownloadUrl, request);
  }

  @override
  Future<Map<String, Object?>> resolveMediaAccess(
      Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaResolveAccess, request);
  }

  @override
  Future<Map<String, Object?>> cacheRemoteMedia(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaCacheRemote, request);
  }

  @override
  Future<Map<String, Object?>> getMediaCacheStats() {
    return _bridge.invoke<Map<String, Object?>>(NativeCallMap.mediaCacheStats);
  }

  @override
  Future<void> setMediaCacheMaxBytes(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.mediaSetCacheMaxBytes, request);
  }

  @override
  Future<void> setMediaCacheRoot(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.mediaSetCacheRoot, request);
  }

  @override
  Future<void> clearMediaCache() =>
      _bridge.invoke<void>(NativeCallMap.mediaClearCache);

  @override
  Future<Map<String, Object?>> getUserDownloadSubfolder() {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaUserDownloadGetSubfolder);
  }

  @override
  Future<void> setUserDownloadSubfolder(Map<String, Object?> request) {
    return _bridge.invoke<void>(
        NativeCallMap.mediaUserDownloadSetSubfolder, request);
  }

  @override
  Future<Map<String, Object?>> getUserDownloadSavedPath(
      Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaUserDownloadGetSavedPath, request);
  }

  @override
  Future<void> deleteUserDownloadRecord(Map<String, Object?> request) {
    return _bridge.invoke<void>(
        NativeCallMap.mediaUserDownloadDeleteRecord, request);
  }

  @override
  Future<Map<String, Object?>> getUserDownloadDirectory() {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaUserDownloadGetDirectory);
  }

  @override
  Future<Map<String, Object?>> setUserDownloadDirectory(
      Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaUserDownloadSetDirectory, request);
  }

  @override
  Future<Map<String, Object?>> downloadToUserDirectory(
      Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaDownloadToUserDirectory, request);
  }

  @override
  Future<bool> cancelUserFileDownload(Map<String, Object?> request) {
    return _bridge.invoke<bool>(
        NativeCallMap.mediaCancelUserFileDownload, request);
  }

  @override
  Future<Map<String, Object?>> downloadFileToDownloads(
      Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.mediaDownloadFileToDownloads, request);
  }
}

final class _DefaultCapabilitiesApi implements CapabilitiesApi {
  const _DefaultCapabilitiesApi(this._bridge);

  final NativeBridge _bridge;

  @override
  Future<Map<String, Object?>> preparePluginCommand(PluginCommandRequest request) {
    return _bridge.invoke<Map<String, Object?>>(NativeCallMap.capabilityPreparePluginCommand, pluginCommandRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> pendingPluginCommand(PluginCommandRequest request) {
    return _bridge.invoke<Map<String, Object?>>(NativeCallMap.capabilityPendingPluginCommand, pluginCommandRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> submitPluginCommand(PluginCommandRequest request) {
    return _bridge.invoke<Map<String, Object?>>(NativeCallMap.capabilitySubmitPluginCommand, pluginCommandRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> reconcilePluginCommand(PluginCommandRequest request) {
    return _bridge.invoke<Map<String, Object?>>(NativeCallMap.capabilityReconcilePluginCommand, pluginCommandRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> acknowledgePluginCommand(PluginCommandRequest request) {
    return _bridge.invoke<Map<String, Object?>>(NativeCallMap.capabilityAcknowledgePluginCommand, pluginCommandRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> presentPluginMessage(PluginUiEvaluationRequest request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityPluginUiPresent, pluginUiEvaluationRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> invokePluginAction(PluginUiEvaluationRequest request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityPluginUiInvoke, pluginUiEvaluationRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> pluginUiAction(PluginUiEvaluationRequest request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityPluginUiAction, pluginUiEvaluationRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> resolvePluginMessage(PluginUiEvaluationRequest request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityPluginUiResolve, pluginUiEvaluationRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> pluginUiCatalog(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityPluginUiCatalog, request);
  }

  @override
  Future<Map<String, Object?>> pluginComposerActions(PluginUiEvaluationRequest request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityPluginComposerActions, pluginUiEvaluationRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> pluginDetail(PluginUiEvaluationRequest request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityPluginDetail, pluginUiEvaluationRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> pluginPage(PluginUiEvaluationRequest request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityPluginPage, pluginUiEvaluationRequestToMap(request));
  }

  @override
  Future<Map<String, Object?>> pluginAccountEntries(PluginUiEvaluationRequest request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityPluginAccountEntries, pluginUiEvaluationRequestToMap(request));
  }

  @override
  Future<void> invalidatePluginCatalog(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.capabilityPluginCatalogInvalidate, request);
  }

  @override
  Future<Map<String, Object?>> listCapabilities(Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityList, request);
  }

  @override
  Future<Map<String, Object?>> listUserCapabilities(
      Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityListUser, request);
  }

  @override
  Future<Map<String, Object?>> dispatchCapability(
      Map<String, Object?> request) {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.capabilityDispatch, request);
  }

  @override
  Future<void> grantCapability(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.capabilityGrant, request);
  }

  @override
  Future<void> revokeCapability(Map<String, Object?> request) {
    return _bridge.invoke<void>(NativeCallMap.capabilityRevoke, request);
  }
}

final class _DefaultViewsApi implements ViewsApi {
  const _DefaultViewsApi(this._bridge);

  final NativeBridge _bridge;

  @override
  Future<ViewOpenResponse> openTimeline(OpenTimelineViewRequest request) async {
    final response = await _bridge.invoke<Object?>(
      NativeCallMap.viewTimelineOpen,
      openTimelineViewRequestToMap(request),
    );
    return viewOpenResponseFromJson(response);
  }

  @override
  Future<TimelineViewPageResponse> loadOlderTimeline(
    LoadOlderTimelineViewRequest request,
  ) async {
    final response = await _bridge.invoke<Object?>(
      NativeCallMap.viewTimelineLoadOlder,
      loadOlderTimelineViewRequestToMap(request),
    );
    return timelineViewPageResponseFromJson(response);
  }

  @override
  Future<TimelineViewPageResponse> loadNewerTimeline(
    LoadNewerTimelineViewRequest request,
  ) async {
    final response = await _bridge.invoke<Object?>(
      NativeCallMap.viewTimelineLoadNewer,
      loadNewerTimelineViewRequestToMap(request),
    );
    return timelineViewPageResponseFromJson(response);
  }

  @override
  Future<TimelineViewPageResponse> jumpToLatestTimeline(
    TimelineViewRequest request,
  ) async {
    final response = await _bridge.invoke<Object?>(
      NativeCallMap.viewTimelineJumpToLatest,
      timelineViewRequestToMap(request),
    );
    return timelineViewPageResponseFromJson(response);
  }

  @override
  Future<ViewOpenResponse> timelineSnapshot(TimelineViewRequest request) async {
    final response = await _bridge.invoke<Object?>(
      NativeCallMap.viewTimelineSnapshot,
      timelineViewRequestToMap(request),
    );
    return viewOpenResponseFromJson(response);
  }

  @override
  Future<ViewOpenResponse> openConversationList(
    OpenConversationListViewRequest request,
  ) async {
    final response = await _bridge.invoke<Object?>(
      NativeCallMap.viewConversationListOpen,
      openConversationListViewRequestToMap(request),
    );
    return viewOpenResponseFromJson(response);
  }

  @override
  Future<CloseViewResponse> close(CloseViewRequest request) async {
    final response = await _bridge.invoke<Object?>(
      NativeCallMap.viewClose,
      closeViewRequestToMap(request),
    );
    return closeViewResponseFromJson(response);
  }
}

final class _DefaultDiagnosticsApi implements DiagnosticsApi {
  const _DefaultDiagnosticsApi(this._bridge);

  final NativeBridge _bridge;

  @override
  Future<Map<String, Object?>> getSdkVersion() {
    return _bridge
        .invoke<Map<String, Object?>>(NativeCallMap.diagnosticsSdkVersion);
  }

  @override
  Future<Map<String, Object?>> getFfiContractVersion() {
    return _bridge.invoke<Map<String, Object?>>(
        NativeCallMap.diagnosticsFfiContractVersion);
  }

  @override
  Future<Map<String, Object?>> getDataRoot() {
    return _bridge
        .invoke<Map<String, Object?>>(NativeCallMap.diagnosticsDataRoot);
  }

  @override
  Future<RuntimeHealthResponse> getRuntimeHealth() async {
    final raw = await _bridge.invoke<Map<String, Object?>>(
      NativeCallMap.diagnosticsRuntimeHealth,
    );
    return runtimeHealthResponseFromJson(raw);
  }
}

SdkErrorPayload sdkErrorFromException(Object error, String operation) {
  if (error is FlareSdkException) {
    return SdkErrorPayload(
      code: error.code,
      message: error.message,
      operation: error.operation ?? operation,
      details: error.details is Map
          ? (error.details as Map)
              .map((key, value) => MapEntry(key.toString(), value.toString()))
          : const {},
    );
  }
  return SdkErrorPayload(
    code: 'dart_error',
    message: error.toString(),
    operation: operation,
  );
}
