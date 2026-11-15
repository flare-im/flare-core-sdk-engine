// Flutter runtime codec over generated contracts.

import 'dart:developer' as developer;

import '../../model/model.dart';

Map<String, Object?> pluginUiEvaluationRequestToMap(PluginUiEvaluationRequest request) {
  return {'payload': request.payload};
}

Map<String, Object?> pluginCommandRequestToMap(PluginCommandRequest request) {
  return {'payload': request.payload};
}

Map<String, Object?> messageReadReceiptMembersRequestToMap(MessageReadReceiptMembersRequest request) {
  return {'payload': request.payload};
}

Map<String, Object?> conversationListQueryToMap(ConversationListQuery query) {
  return {
    if (query.keyword != null) 'keyword': query.keyword!,
    'includeArchived': query.includeArchived,
    'unreadOnly': query.unreadOnly,
    'mentionMeOnly': query.mentionMeOnly,
    'pinnedOnly': query.pinnedOnly,
    if (query.mutedOnly != null) 'mutedOnly': query.mutedOnly!,
    'hasDraftOnly': query.hasDraftOnly,
    'hasMarkedMessages': query.hasMarkedMessages,
    if (query.conversationTypes.isNotEmpty)
      'conversationTypes': query.conversationTypes
          .map(conversationTypeWireValue)
          .toList(growable: false),
    if (query.cursor != null) 'cursor': query.cursor!,
    if (query.limit != null) 'limit': query.limit!,
  };
}

Map<String, Object?> messageSearchQueryToMap(MessageSearchQuery query) {
  return {
    if (query.keyword != null) 'keyword': query.keyword!,
    if (query.conversationId != null) 'conversationId': query.conversationId!,
    if (query.senderId != null) 'senderId': query.senderId!,
    if (query.fromTime != null) 'fromTime': query.fromTime!,
    if (query.toTime != null) 'toTime': query.toTime!,
    if (query.kinds.isNotEmpty)
      'kinds': query.kinds.map((k) => k.name).toList(growable: false),
    'limit': query.limit,
    'includeRecalled': query.includeRecalled,
  };
}

Map<String, Object?> listMessagesRequestToMap(ListMessagesRequest request) {
  return {
    'conversationId': request.conversationId,
    'beforeSeq': request.beforeSeq,
    'limit': request.limit,
  };
}

Map<String, Object?> bootstrapHomeTimelineRequestToMap(
  BootstrapHomeTimelineRequest request,
) {
  return {'conversationLimit': request.conversationLimit};
}

Map<String, Object?> openConversationTimelineRequestToMap(
  OpenConversationTimelineRequest request,
) {
  return {
    'conversationId': request.conversationId,
    'messageLimit': request.messageLimit,
  };
}

Map<String, Object?> openTimelineViewRequestToMap(
  OpenTimelineViewRequest request,
) {
  final anchor = request.anchor;
  return {
    'conversationId': request.conversationId,
    'messageLimit': request.messageLimit,
    if (anchor != null)
      'anchor': {
        if (anchor.messageId != null) 'messageId': anchor.messageId,
        if (anchor.seq != null) 'seq': anchor.seq,
      },
  };
}

Map<String, Object?> loadOlderTimelineViewRequestToMap(
  LoadOlderTimelineViewRequest request,
) {
  return {'viewId': request.viewId, 'messageLimit': request.messageLimit};
}

Map<String, Object?> loadNewerTimelineViewRequestToMap(
  LoadNewerTimelineViewRequest request,
) {
  return {'viewId': request.viewId, 'messageLimit': request.messageLimit};
}

Map<String, Object?> timelineViewRequestToMap(TimelineViewRequest request) {
  return {'viewId': request.viewId};
}

Map<String, Object?> openConversationListViewRequestToMap(
  OpenConversationListViewRequest request,
) {
  return {'conversationLimit': request.conversationLimit};
}

Map<String, Object?> closeViewRequestToMap(CloseViewRequest request) {
  return {'viewId': request.viewId};
}

Map<String, Object?> conversationVersionToMap(ConversationVersion version) {
  return {'conversationId': version.conversationId, 'version': version.version};
}

Map<String, Object?> syncConversationSummariesRequestToMap(
  SyncConversationSummariesRequest request,
) {
  return {
    'knownVersions': request.knownVersions
        .map(conversationVersionToMap)
        .toList(growable: false),
  };
}

Map<String, Object?> updateConversationDraftRequestToMap(
  UpdateConversationDraftRequest request,
) {
  return {
    'conversationId': request.conversationId,
    if (request.draft != null) 'draft': request.draft!,
  };
}

Map<String, Object?> setAppStateRequestToMap(SetAppStateRequest request) {
  return {'appState': request.appState.name};
}

Map<String, Object?> setConnectTokenRefresherRequestToMap(
  SetConnectTokenRefresherRequest request,
) {
  return {'enabled': request.enabled};
}

Map<String, Object?> completeConnectTokenRefreshRequestToMap(
  CompleteConnectTokenRefreshRequest request,
) {
  return {
    'requestId': request.requestId,
    if (request.token != null) 'token': request.token!,
  };
}

Map<String, Object?> accessPinRequestToMap(AccessPinRequest request) {
  return {'accessPointId': request.accessPointId};
}

Map<String, Object?> accessImportRecoveryRequestToMap(
  AccessImportRecoveryRequest request,
) {
  return {'link': request.link};
}

Map<String, Object?> updateCheckRequestToMap(UpdateCheckRequest request) {
  return {'manual': request.manual};
}

Map<String, Object?> updateGetStateRequestToMap(
  UpdateGetStateRequest request,
) {
  return {
    if (request.foreground != null) 'foreground': request.foreground,
  };
}

Map<String, Object?> updateSetLocaleRequestToMap(
  UpdateSetLocaleRequest request,
) {
  return {'locale': request.locale};
}

Map<String, Object?> updateGetResourcesRequestToMap(
  UpdateGetResourcesRequest request,
) {
  return {};
}

Map<String, Object?> updateDownloadRequestToMap(
  UpdateDownloadRequest request,
) {
  return {'allowMetered': request.allowMetered};
}

Map<String, Object?> updateReportInstallRequestToMap(
  UpdateReportInstallRequest request,
) {
  return {
    'outcome': request.outcome,
    if (request.code != null) 'code': request.code,
  };
}

Map<String, Object?> updateSkipRequestToMap(UpdateSkipRequest request) {
  return {'build': request.build};
}

Map<String, Object?> updateMarkPromptedRequestToMap(
  UpdateMarkPromptedRequest request,
) {
  return {
    'form': request.form,
    if (request.build != null) 'build': request.build,
  };
}

Map<String, Object?> setHeartbeatNatTimeoutRequestToMap(
  SetHeartbeatNatTimeoutRequest request,
) {
  return {
    if (request.natTimeoutSecs != null)
      'natTimeoutSecs': request.natTimeoutSecs!,
  };
}

/// Unset optional fields are left out; `capabilities` always goes as a list.
Map<String, Object?> pushTokenInputToMap(PushTokenInput request) {
  return {
    'provider': request.provider,
    'platform': request.platform,
    'token': request.token,
    if (request.appBundle != null) 'appBundle': request.appBundle!,
    if (request.environment != null) 'environment': request.environment!,
    if (request.locale != null) 'locale': request.locale!,
    if (request.timeZone != null) 'timeZone': request.timeZone!,
    if (request.appVersion != null) 'appVersion': request.appVersion!,
    if (request.capabilities != null)
      'capabilities': List<String>.of(request.capabilities!),
  };
}

/// `payload` goes as a JSON object of strings.
Map<String, Object?> handleRemoteMessageRequestToMap(
  HandleRemoteMessageRequest request,
) {
  return {
    'provider': request.provider,
    'payload': Map<String, String>.of(request.payload),
    if (request.budgetMs != null) 'budgetMs': request.budgetMs!,
  };
}

Map<String, Object?> resolveNotificationTapRequestToMap(
  ResolveNotificationTapRequest request,
) {
  return {'payload': Map<String, String>.of(request.payload)};
}

/// No conversation open: the key is left out.
Map<String, Object?> setNotificationFocusRequestToMap(
  SetNotificationFocusRequest request,
) {
  return {
    if (request.conversationId != null)
      'conversationId': request.conversationId!,
  };
}

Map<String, Object?> setBackgroundPresenterRequestToMap(
  SetBackgroundPresenterRequest request,
) {
  return {'attached': request.attached};
}

/// No directory: the key is left out (the engine stops writing the snapshot).
Map<String, Object?> setNotificationSnapshotDirectoryRequestToMap(
  SetNotificationSnapshotDirectoryRequest request,
) {
  return {if (request.directory != null) 'directory': request.directory!};
}

Map<String, Object?> backgroundRefreshRequestToMap(
  BackgroundRefreshRequest request,
) {
  return {'budgetMs': request.budgetMs};
}

Map<String, Object?> quietHoursToMap(QuietHours value) {
  return {
    'startMinute': value.startMinute,
    'endMinute': value.endMinute,
    'weekdays': value.weekdays,
    'allowMentions': value.allowMentions,
  };
}

/// Items the update leaves alone are left out.
Map<String, Object?> updateUserNotificationSettingsRequestToMap(
  UpdateUserNotificationSettingsRequest request,
) {
  return {
    if (request.quietHours != null)
      'quietHours': quietHoursToMap(request.quietHours!),
    'clearQuietHours': request.clearQuietHours,
    if (request.preview != null) 'preview': request.preview!.name,
    if (request.multiDevice != null) 'multiDevice': request.multiDevice!.name,
    'baseSettingsVersion': request.baseSettingsVersion,
  };
}

Map<String, Object?> networkChangeRequestToMap(NetworkChangeRequest request) {
  return {
    if (request.available != null) 'available': request.available!,
    if (request.interface != null) 'interface': request.interface!.name,
    if (request.expensive != null) 'expensive': request.expensive!,
    if (request.metered != null) 'metered': request.metered!,
    if (request.reason != null) 'reason': request.reason!,
    if (request.fingerprintHint != null)
      'fingerprintHint': request.fingerprintHint!,
  };
}

ConnectionPhase connectionPhaseFromValue(Object? value) {
  for (final phase in ConnectionPhase.values) {
    if (phase.name == value) {
      return phase;
    }
  }
  throw FormatException(
    'invalid connection phase: ${value ?? '<empty>'}',
    value,
  );
}

ConnectionQuality connectionQualityFromValue(Object? value) {
  for (final quality in ConnectionQuality.values) {
    if (quality.name == value) {
      return quality;
    }
  }
  throw FormatException(
    'invalid connection quality: ${value ?? '<empty>'}',
    value,
  );
}

/// `connection://status` 的载荷与 `connection.getStatus` 的返回值：每个字段都出现，无值为 null。
ConnectionStatus connectionStatusFromJson(Object? value) {
  const context = 'ConnectionStatus';
  final json = mapValue(value);
  final quality = json['quality'];
  return ConnectionStatus(
    phase: connectionPhaseFromValue(json['phase']),
    quality: quality == null ? null : connectionQualityFromValue(quality),
    accessPointId: _optionalStringField(json, 'accessPointId', context),
    cellId: _optionalStringField(json, 'cellId', context),
    regionId: _optionalStringField(json, 'regionId', context),
    protocol: _optionalStringField(json, 'protocol', context),
    rttMs: _optionalUnsignedField(json, 'rttMs', context),
    attempt: requiredIntField(json, 'attempt', context),
    nextRetryAt: _optionalSignedField(json, 'nextRetryAt', context),
    reason: _optionalStringField(json, 'reason', context),
    developmentMode: _requiredBoolField(json, 'developmentMode', context),
  );
}

/// `connection.sync_state_changed` 只是粗粒度提示，没有监听器：宽松解码，不因载荷形状抛异常。
ConnectionSyncStateEvent connectionSyncStateEventFromJson(Object? value) {
  final state = mapValue(value)['state'];
  return ConnectionSyncStateEvent(state: state is String ? state : '');
}

AccessPin accessPinFromJson(Object? value) {
  const context = 'AccessPin';
  final json = mapValue(value);
  return AccessPin(
    accessPointId: requiredStringField(json, 'accessPointId', context),
    expiresAtMs: _requiredSignedField(json, 'expiresAtMs', context),
  );
}

AccessStatus accessStatusFromJson(Object? value) {
  const context = 'AccessStatus';
  final json = mapValue(value);
  final pinned = json['pinned'];
  final connectAck = json['connectAck'];
  final refresh = json['refresh'];
  final recovering = json['recovering'];
  return AccessStatus(
    loaded: _requiredBoolField(json, 'loaded', context),
    serial: requiredIntField(json, 'serial', context),
    source: _optionalStringField(json, 'source', context),
    stale: _requiredBoolField(json, 'stale', context),
    developmentMode: _requiredBoolField(json, 'developmentMode', context),
    issuedAtMs: _optionalSignedField(json, 'issuedAtMs', context),
    expiresAtMs: _optionalSignedField(json, 'expiresAtMs', context),
    fetchedAtMs: _optionalSignedField(json, 'fetchedAtMs', context),
    regionId: _optionalStringField(json, 'regionId', context),
    cellId: _optionalStringField(json, 'cellId', context),
    keystateSerial: _optionalUnsignedField(json, 'keystateSerial', context),
    accessPointId: _optionalStringField(json, 'accessPointId', context),
    protocol: _optionalStringField(json, 'protocol', context),
    pinned: pinned == null
        ? null
        : accessPinFromJson(_requiredMapField(json, 'pinned', context)),
    connectAck: connectAck == null
        ? null
        : accessConnectAckFromJson(
            _requiredMapField(json, 'connectAck', context),
          ),
    refresh: refresh == null
        ? null
        : accessRefreshStatusFromJson(
            _requiredMapField(json, 'refresh', context),
          ),
    recovering: recovering == null
        ? null
        : accessRecoveringStatusFromJson(
            _requiredMapField(json, 'recovering', context),
          ),
  );
}

/// 清单刷新进度（`AccessStatus.refresh`）。
AccessRefreshStatus accessRefreshStatusFromJson(Object? value) {
  const context = 'AccessRefreshStatus';
  final json = mapValue(value);
  return AccessRefreshStatus(
    lastAttemptAtMs: _optionalSignedField(json, 'lastAttemptAtMs', context),
    lastSuccessAtMs: _optionalSignedField(json, 'lastSuccessAtMs', context),
    lastOutcome: _optionalStringField(json, 'lastOutcome', context),
    nextAtMs: _optionalSignedField(json, 'nextAtMs', context),
  );
}

/// 全部接入点失败时的恢复状态（`AccessStatus.recovering`）。
AccessRecoveringStatus accessRecoveringStatusFromJson(Object? value) {
  const context = 'AccessRecoveringStatus';
  final json = mapValue(value);
  return AccessRecoveringStatus(
    sinceMs: _requiredSignedField(json, 'sinceMs', context),
    rounds: requiredIntField(json, 'rounds', context),
    nextRoundAtMs: _optionalSignedField(json, 'nextRoundAtMs', context),
  );
}

AccessRefreshAttempt accessRefreshAttemptFromJson(Object? value) {
  const context = 'AccessRefreshAttempt';
  final json = mapValue(value);
  return AccessRefreshAttempt(
    source: requiredStringField(json, 'source', context),
    tier: requiredStringField(json, 'tier', context),
    document: requiredStringField(json, 'document', context),
    result: requiredStringField(json, 'result', context),
    code: _optionalStringField(json, 'code', context),
    serial: _optionalUnsignedField(json, 'serial', context),
  );
}

/// `update.getState` 等 `update.*` 的返回值，也是 `update://state` 的载荷。
UpdateStatus updateStatusFromJson(Object? value) {
  const context = 'UpdateStatus';
  final json = mapValue(value);
  final release = json['release'];
  final progress = json['progress'];
  final error = json['error'];
  return UpdateStatus(
    state: requiredStringField(json, 'state', context),
    current: updateCurrentAppFromJson(
      _requiredMapField(json, 'current', context),
    ),
    release: release == null
        ? null
        : updateReleaseSummaryFromJson(
            _requiredMapField(json, 'release', context),
          ),
    progress: progress == null
        ? null
        : updateProgressFromJson(_requiredMapField(json, 'progress', context)),
    prompt: requiredStringField(json, 'prompt', context),
    blockUi: _requiredBoolField(json, 'blockUi', context),
    readOnly: _requiredBoolField(json, 'readOnly', context),
    lastCheckedAt: _optionalSignedField(json, 'lastCheckedAt', context),
    error: error == null
        ? null
        : updateErrorInfoFromJson(_requiredMapField(json, 'error', context)),
    blockReason: _optionalStringField(json, 'blockReason', context),
  );
}

UpdateCurrentApp updateCurrentAppFromJson(Object? value) {
  const context = 'UpdateCurrentApp';
  final json = mapValue(value);
  return UpdateCurrentApp(
    version: _optionalStringField(json, 'version', context),
    build: _optionalUnsignedField(json, 'build', context),
    channel: requiredStringField(json, 'channel', context),
    distribution: _optionalStringField(json, 'distribution', context),
  );
}

UpdateReleaseSummary updateReleaseSummaryFromJson(Object? value) {
  const context = 'UpdateReleaseSummary';
  final json = mapValue(value);
  return UpdateReleaseSummary(
    releaseId: requiredStringField(json, 'releaseId', context),
    version: requiredStringField(json, 'version', context),
    build: requiredIntField(json, 'build', context),
    notes: stringList(json['notes']),
    sizeBytes: _optionalUnsignedField(json, 'sizeBytes', context),
    mandatory: _requiredBoolField(json, 'mandatory', context),
    recommended: _requiredBoolField(json, 'recommended', context),
    kind: requiredStringField(json, 'kind', context),
  );
}

UpdateProgress updateProgressFromJson(Object? value) {
  const context = 'UpdateProgress';
  final json = mapValue(value);
  return UpdateProgress(
    received: requiredIntField(json, 'received', context),
    total: requiredIntField(json, 'total', context),
  );
}

UpdateErrorInfo updateErrorInfoFromJson(Object? value) {
  const context = 'UpdateErrorInfo';
  final json = mapValue(value);
  return UpdateErrorInfo(
    code: requiredStringField(json, 'code', context),
    retryable: _requiredBoolField(json, 'retryable', context),
    fallbackUrl: _optionalStringField(json, 'fallbackUrl', context),
  );
}

/// `update.get_resources` 的返回值：`contentJson` 是文本，由 Kit 和宿主解析。
UpdateResources updateResourcesFromJson(Object? value) {
  const context = 'UpdateResources';
  final json = mapValue(value);
  return UpdateResources(
    packs: requiredWireListOfMaps(json['packs'], 'packs', context)
        .map(updateResourcePackFromJson)
        .toList(growable: false),
  );
}

UpdateResourcePack updateResourcePackFromJson(Object? value) {
  const context = 'UpdateResourcePack';
  final json = mapValue(value);
  return UpdateResourcePack(
    id: requiredStringField(json, 'id', context),
    kind: requiredStringField(json, 'kind', context),
    packVersion: requiredIntField(json, 'packVersion', context),
    contentJson: requiredStringField(json, 'contentJson', context),
  );
}

/// `update.install` 的返回值：`kind` 决定哪些字段有值。
UpdateInstallPlan updateInstallPlanFromJson(Object? value) {
  const context = 'UpdateInstallPlan';
  final json = mapValue(value);
  bool? optionalFlag(String key) =>
      json[key] == null ? null : _requiredBoolField(json, key, context);
  final urls = json['assetManifestUrls'];
  return UpdateInstallPlan(
    kind: requiredStringField(json, 'kind', context),
    store: _optionalStringField(json, 'store', context),
    url: _optionalStringField(json, 'url', context),
    packageName: _optionalStringField(json, 'packageName', context),
    appId: _optionalStringField(json, 'appId', context),
    immediate: optionalFlag('immediate'),
    path: _optionalStringField(json, 'path', context),
    sha256: _optionalStringField(json, 'sha256', context),
    signingCertSha256: _optionalStringField(json, 'signingCertSha256', context),
    build: _optionalUnsignedField(json, 'build', context),
    sizeBytes: _optionalUnsignedField(json, 'sizeBytes', context),
    buildId: _optionalStringField(json, 'buildId', context),
    assetManifestUrls: urls == null ? null : stringList(urls),
    assetManifestSha256:
        _optionalStringField(json, 'assetManifestSha256', context),
    mandatory: optionalFlag('mandatory'),
    blockUi: optionalFlag('blockUi'),
    version: _optionalStringField(json, 'version', context),
    target: _optionalStringField(json, 'target', context),
  );
}

/// `access.refresh` 的返回值。
AccessRefreshReport accessRefreshReportFromJson(Object? value) {
  const context = 'AccessRefreshReport';
  final json = mapValue(value);
  return AccessRefreshReport(
    outcome: requiredStringField(json, 'outcome', context),
    mode: requiredStringField(json, 'mode', context),
    serial: requiredIntField(json, 'serial', context),
    source: _optionalStringField(json, 'source', context),
    keystateSerial: _optionalUnsignedField(json, 'keystateSerial', context),
    attempts: requiredWireListOfMaps(json['attempts'], 'attempts', context)
        .map(accessRefreshAttemptFromJson)
        .toList(growable: false),
  );
}

/// `access.importRecovery` 的返回值：链接有问题也在这里，不抛错。
AccessRecoveryImport accessRecoveryImportFromJson(Object? value) {
  const context = 'AccessRecoveryImport';
  final json = mapValue(value);
  return AccessRecoveryImport(
    outcome: requiredStringField(json, 'outcome', context),
    code: _optionalStringField(json, 'code', context),
    serial: requiredIntField(json, 'serial', context),
  );
}

/// 当前连接 CONNECT_ACK 带回的诊断字段（`AccessStatus.connectAck`），只用于展示。
AccessConnectAck accessConnectAckFromJson(Object? value) {
  const context = 'AccessConnectAck';
  final json = mapValue(value);
  return AccessConnectAck(
    gatewayId: _optionalStringField(json, 'gatewayId', context),
    accessPointId: _optionalStringField(json, 'accessPointId', context),
    accessSerial: _optionalUnsignedField(json, 'accessSerial', context),
    heartbeatIntervalHintSec:
        _optionalUnsignedField(json, 'heartbeatIntervalHintSec', context),
    accessPointMismatch:
        _requiredBoolField(json, 'accessPointMismatch', context),
  );
}

AccessPointInfo accessPointInfoFromJson(Object? value) {
  const context = 'AccessPointInfo';
  final json = mapValue(value);
  return AccessPointInfo(
    accessPointId: requiredStringField(json, 'accessPointId', context),
    regionId: requiredStringField(json, 'regionId', context),
    host: requiredStringField(json, 'host', context),
    status: requiredStringField(json, 'status', context),
    rttMs: _optionalDoubleField(json, 'rttMs', context),
    breaker: requiredStringField(json, 'breaker', context),
    protocolWinner: _optionalStringField(json, 'protocolWinner', context),
    pinned: _requiredBoolField(json, 'pinned', context),
    current: _requiredBoolField(json, 'current', context),
  );
}

/// `access.listAccessPoints` 的响应是裸 JSON 数组；桥把数组包成 `{items: [...]}` 时同样接受。
List<AccessPointInfo> accessPointInfoListFromJson(Object? value) {
  final items = value is Map && value['items'] is List ? value['items'] : value;
  return requiredWireListOfMaps(items, 'items', 'AccessPointInfoList')
      .map(accessPointInfoFromJson)
      .toList(growable: false);
}

AccessEndpoint accessEndpointFromJson(Object? value) {
  const context = 'AccessEndpoint';
  final json = mapValue(value);
  return AccessEndpoint(
    ap: requiredStringField(json, 'ap', context),
    protocol: requiredStringField(json, 'protocol', context),
  );
}

AccessSwitchedEvent accessSwitchedEventFromJson(Object? value) {
  const context = 'AccessSwitchedEvent';
  final json = mapValue(value);
  return AccessSwitchedEvent(
    from: accessEndpointFromJson(_requiredMapField(json, 'from', context)),
    to: accessEndpointFromJson(_requiredMapField(json, 'to', context)),
    reason: requiredStringField(json, 'reason', context),
  );
}

AccessChanges accessChangesFromJson(Object? value) {
  const context = 'AccessChanges';
  final json = mapValue(value);
  return AccessChanges(
    accessPointsAdded: stringList(json['accessPointsAdded']),
    accessPointsRemoved: stringList(json['accessPointsRemoved']),
    policyChanged: _requiredBoolField(json, 'policyChanged', context),
  );
}

AccessChangedEvent accessChangedEventFromJson(Object? value) {
  const context = 'AccessChangedEvent';
  final json = mapValue(value);
  return AccessChangedEvent(
    serial: requiredIntField(json, 'serial', context),
    source: requiredStringField(json, 'source', context),
    stale: _requiredBoolField(json, 'stale', context),
    developmentMode: _requiredBoolField(json, 'developmentMode', context),
    changes: accessChangesFromJson(_requiredMapField(json, 'changes', context)),
  );
}

bool _requiredBoolField(
  Map<String, Object?> json,
  String key,
  String context,
) {
  final value = json[key];
  if (value is bool) return value;
  throw ArgumentError.value(
    json,
    context,
    value == null
        ? '$context.$key is required'
        : '$context.$key must be a boolean',
  );
}

Map<String, Object?> _requiredMapField(
  Map<String, Object?> json,
  String key,
  String context,
) {
  final value = json[key];
  if (value is Map) return mapValue(value);
  throw ArgumentError.value(
    json,
    context,
    value == null
        ? '$context.$key is required'
        : '$context.$key must be an object',
  );
}

String? _optionalStringField(
  Map<String, Object?> json,
  String key,
  String context,
) {
  final value = json[key];
  if (value == null || value is String) return value as String?;
  throw ArgumentError.value(json, context, '$context.$key must be a string');
}

int? _optionalUnsignedField(
  Map<String, Object?> json,
  String key,
  String context,
) {
  if (json[key] == null) return null;
  return requiredIntField(json, key, context);
}

int _requiredSignedField(
  Map<String, Object?> json,
  String key,
  String context,
) {
  final value = json[key];
  if (value is num && value.isFinite && value.truncateToDouble() == value) {
    return value.toInt();
  }
  throw ArgumentError.value(
    json,
    context,
    value == null
        ? '$context.$key is required'
        : '$context.$key must be an integer',
  );
}

int? _optionalSignedField(
  Map<String, Object?> json,
  String key,
  String context,
) {
  if (json[key] == null) return null;
  return _requiredSignedField(json, key, context);
}

double? _optionalDoubleField(
  Map<String, Object?> json,
  String key,
  String context,
) {
  final value = json[key];
  if (value == null) return null;
  if (value is num && value.isFinite) return value.toDouble();
  throw ArgumentError.value(json, context, '$context.$key must be a number');
}

ServerClockResponse serverClockResponseFromJson(Object? value) {
  final json = mapValue(value);
  final offset = json['offsetMs'];
  return ServerClockResponse(
    offsetMs: offset is num && offset.isFinite && offset.truncateToDouble() == offset
        ? offset.toInt()
        : 0,
    estimated: json['estimated'] is bool ? json['estimated'] as bool : false,
  );
}

RemoteMessageOutcome remoteMessageOutcomeFromJson(Object? value) {
  const context = 'RemoteMessageOutcome';
  final json = mapValue(value);
  final clearCollapseIds = json['clearCollapseIds'];
  final presentation = json['presentation'];
  return RemoteMessageOutcome(
    action: requiredStringField(json, 'action', context),
    badge: _optionalUnsignedField(json, 'badge', context),
    clearCollapseIds: clearCollapseIds == null
        ? const []
        : stringList(clearCollapseIds),
    presentation: presentation == null
        ? null
        : notificationPresentationFromJson(
            _requiredMapField(json, 'presentation', context),
          ),
    reason: _optionalStringField(json, 'reason', context),
  );
}

NotificationPresentation notificationPresentationFromJson(Object? value) {
  const context = 'NotificationPresentation';
  final json = mapValue(value);
  return NotificationPresentation(
    body: requiredStringField(json, 'body', context),
    channel: requiredStringField(json, 'channel', context),
    collapseId: requiredStringField(json, 'collapseId', context),
    conversationId: requiredStringField(json, 'conversationId', context),
    count: requiredIntField(json, 'count', context),
    messageId: requiredStringField(json, 'messageId', context),
    seq: requiredIntField(json, 'seq', context),
    sound: _requiredBoolField(json, 'sound', context),
    surface: requiredStringField(json, 'surface', context),
    threadId: requiredStringField(json, 'threadId', context),
    title: requiredStringField(json, 'title', context),
  );
}

UserNotificationSettings userNotificationSettingsFromJson(Object? value) {
  const context = 'UserNotificationSettings';
  final json = mapValue(value);
  final quietHours = json['quietHours'];
  return UserNotificationSettings(
    multiDevice: _wireEnum(
      MultiDeviceAlertPolicy.values,
      requiredStringField(json, 'multiDevice', context),
      context,
      'multiDevice',
    ),
    preview: _wireEnum(
      NotificationPreview.values,
      requiredStringField(json, 'preview', context),
      context,
      'preview',
    ),
    quietHours: quietHours == null
        ? null
        : quietHoursFromJson(_requiredMapField(json, 'quietHours', context)),
    settingsVersion: requiredIntField(json, 'settingsVersion', context),
  );
}

QuietHours quietHoursFromJson(Object? value) {
  const context = 'QuietHours';
  final json = mapValue(value);
  return QuietHours(
    allowMentions: _requiredBoolField(json, 'allowMentions', context),
    endMinute: requiredIntField(json, 'endMinute', context),
    startMinute: requiredIntField(json, 'startMinute', context),
    weekdays: requiredIntField(json, 'weekdays', context),
  );
}

/// The enum value whose name is the wire string; an unknown string is a decode error.
T _wireEnum<T extends Enum>(
  List<T> values,
  String wire,
  String context,
  String key,
) {
  for (final value in values) {
    if (value.name == wire) return value;
  }
  throw ArgumentError.value(wire, context, '$context.$key has an unknown value');
}

BackgroundRefreshResult backgroundRefreshResultFromJson(Object? value) {
  const context = 'BackgroundRefreshResult';
  final json = mapValue(value);
  return BackgroundRefreshResult(
    completed: _requiredBoolField(json, 'completed', context),
  );
}

NotificationTapResult notificationTapResultFromJson(Object? value) {
  const context = 'NotificationTapResult';
  final json = mapValue(value);
  return NotificationTapResult(
    conversationId: _optionalStringField(json, 'conversationId', context),
    messageId: _optionalStringField(json, 'messageId', context),
    seq: _optionalUnsignedField(json, 'seq', context),
  );
}

HeartbeatEffectiveIntervalResponse heartbeatEffectiveIntervalResponseFromJson(
  Object? value,
) {
  final json = mapValue(value);
  return HeartbeatEffectiveIntervalResponse(
    connected: json['connected'] is bool ? json['connected'] as bool : false,
    intervalMs: json['intervalMs'] == null
        ? null
        : intValue(json['intervalMs']),
    intervalSecs: json['intervalSecs'] == null
        ? null
        : intValue(json['intervalSecs']),
  );
}

RuntimeHealthResponse runtimeHealthResponseFromJson(Object? value) {
  final json = mapValue(value);
  return RuntimeHealthResponse(
    metricsEnabled: json['metricsEnabled'] is bool
        ? json['metricsEnabled'] as bool
        : false,
    state: '${json['state'] ?? ''}',
    stateCode: intValue(json['stateCode']),
    sessionGeneration: intValue(json['sessionGeneration']),
    rawSubscriberDroppedTotal: intValue(json['rawSubscriberDroppedTotal']),
    metricsJson: '${json['metricsJson'] ?? ''}',
  );
}

Map<String, Object?> sendMessageRequestToMap(SendMessageRequest request) {
  return {'message': messageToWireMap(request.message), if(request.expectedUserId != null) 'expectedUserId':request.expectedUserId};
}

Map<String, Object?> messageToWireMap(Message message) {
  return {
    'serverId': message.serverId,
    'clientMsgId': message.clientMsgId,
    'conversationId': message.conversationId,
    'conversationType': message.conversationType,
    'channelId': message.channelId,
    'senderId': message.senderId,
    'source': message.source,
    'conversationSeq': message.conversationSeq,
    'createdAt': message.createdAt,
    'clientCreatedAt': message.clientCreatedAt,
    'messageType': message.messageType,
    if (message.content != null)
      'content': messageContentToWireMap(message.content!),
    'senderName': message.senderName,
    'senderAvatar': message.senderAvatar,
    'senderDisplayName': message.senderDisplayName,
    if (message.replyTo != null) 'replyTo': message.replyTo!,
    if (message.quotePreview != null) 'quotePreview': message.quotePreview!,
    if (message.threadId != null) 'threadId': message.threadId!,
    'status': message.status,
    'isRead': message.isRead,
    'isRecalled': message.isRecalled,
    'isEdited': message.isEdited,
    'mentionUsers': message.mentionUsers,
    'mentionAll': message.mentionAll,
    'attributes': message.attributes,
    'extensions': message.extensions,
    'reactions': message.reactions.map(reactionToMap).toList(growable: false),
    'textPreview': message.textPreview,
    'version': message.version,
    'updatedAt': message.updatedAt,
    if (message.localState != null)
      'localState': localStateToMap(message.localState!),
    'timelineKey': message.timelineKey,
    'timelineSortTs': message.timelineSortTs,
  };
}

Map<String, Object?> messageContentToWireMap(MessageContent content) {
  final out = <String, Object?>{
    'contentType': messageContentTypeWireValue(content.contentType),
    ...content.data,
  };
  if (content.contentType == MessageContentType.text &&
      out['mentions'] == null) {
    out['mentions'] = const <Object?>[];
  }
  return out;
}

ListConversationsResponse listConversationsResponseFromJson(Object? value) {
  final json = value is Map ? value : const <String, Object?>{};
  return ListConversationsResponse(
    conversations: conversationsFromJsonList(
      json['conversations'],
      'listConversations',
    ),
  );
}

Conversation conversationFromJson(Map<dynamic, dynamic> rawJson) {
  final json = mapValue(rawJson);
  final conversation = Conversation(
    conversationId: requiredStringField(json, 'conversationId', 'Conversation'),
    conversationType: conversationTypeFromValue(json['conversationType']),
    businessType: stringFieldOrEmpty(json, 'businessType'),
    channelId: requiredStringField(json, 'channelId', 'Conversation'),
    membersCount: intValue(json['membersCount']),
    displayName: stringFieldOrEmpty(json, 'displayName'),
    title: requiredDisplayNameField(json, 'title', 'Conversation'),
    avatarUrl: stringFieldOrEmpty(json, 'avatarUrl'),
    remark: json['remark']?.toString(),
    description: json['description']?.toString(),
    lastMessageId: json['lastMessageId']?.toString(),
    lastSenderId: json['lastSenderId']?.toString(),
    lastMessageAt: json['lastMessageAt'] == null
        ? null
        : intValue(json['lastMessageAt']),
    lastMessagePreview: json['lastMessagePreview']?.toString(),
    lastMessage: messagePreviewFromJson(json['lastMessage']),
    lastSenderNickname: stringFieldOrEmpty(json, 'lastSenderNickname'),
    lastSender: displayNameFromJson(json['lastSender']),
    lastSenderAvatarUrl: stringFieldOrEmpty(json, 'lastSenderAvatarUrl'),
    unreadCount: intValue(json['unreadCount']),
    lastReadSeq: intValue(json['lastReadSeq']),
    peerReadSeq: intValue(json['peerReadSeq']),
    maxSeq: intValue(json['maxSeq']),
    visibleAfterSeq: intValue(json['visibleAfterSeq']),
    isPinned: boolValue(json['isPinned']),
    isMuted: boolValue(json['isMuted']),
    isMentionOnly: json['isMentionOnly'] is bool
        ? json['isMentionOnly'] as bool
        : null,
    notifyAll: json['notifyAll'] == true,
    isArchived: boolValue(json['isArchived']),
    updatedAt: intValue(json['updatedAt']),
    createdAt: intValue(json['createdAt']),
    updatedAtTs: json['updatedAtTs'] == null
        ? null
        : intValue(json['updatedAtTs']),
    draft: json['draft']?.toString(),
    ext: stringMap(json['ext']),
    participantVersion: intValue(json['participantVersion']),
    memberPreview: conversationParticipantList(
      json['memberPreview'],
      'memberPreview',
      'Conversation',
    ),
    mentionCount: intValue(json['mentionCount']),
    mentionMe: boolValue(json['mentionMe']),
    badge: json['badge']?.toString(),
    role: json['role']?.toString(),
    participants: conversationParticipantList(
      json['participants'],
      'participants',
      'Conversation',
    ),
    version: intValue(json['version']),
  );
  return conversation;
}

String requiredWireString(
  Map<dynamic, dynamic> json,
  String key,
  String model,
) {
  final value = json[key];
  if (value is! String || value.trim().isEmpty) {
    throw FormatException(
      '$model response missing required `$key`',
      json.keys.map((key) => key.toString()).toList(growable: false),
    );
  }
  return value;
}

String stringFieldOrEmpty(Map<dynamic, dynamic> json, String key) {
  return json[key]?.toString() ?? '';
}

List<Conversation> conversationsFromJsonList(Object? value, String source) {
  return requiredWireListOfMaps(
    value,
    'conversations',
    source,
  ).map(conversationFromJson).toList(growable: false);
}

final Set<String> _codecWarningKeys = <String>{};

void warnCodecOnce(String key, String message, Object? details) {
  if (!_codecWarningKeys.add(key)) return;
  developer.log(
    message,
    name: 'flare_core_flutter_sdk.codec',
    level: 900,
    error: details,
  );
}

ConversationType conversationTypeFromValue(Object? value) {
  final wireValue = value?.toString().trim() ?? '';
  for (final type in ConversationType.values) {
    if (type.name == wireValue) {
      return type;
    }
  }
  throw FormatException(
    'invalid conversation type: ${wireValue.isEmpty ? '<empty>' : wireValue}',
    value,
  );
}

String conversationTypeWireValue(ConversationType type) {
  return type.name;
}

ListMessagesResponse listMessagesResponseFromJson(Object? value) {
  final json = value is Map ? value : const <String, Object?>{};
  final items = requiredWireListOfMaps(
    json['messages'],
    'messages',
    'ListMessagesResponse',
  );
  return ListMessagesResponse(
    messages: items.map(messageFromJson).toList(growable: false),
  );
}

HomeTimelineSnapshot homeTimelineSnapshotFromJson(Object? value) {
  final json = value is Map ? value : const <String, Object?>{};
  final conversations = conversationsFromJsonList(
    json['conversations'],
    'bootstrapHomeTimeline',
  );
  return HomeTimelineSnapshot(
    conversations: conversations,
    totalUnread: intValue(json['totalUnread']),
    syncState: timelineSyncStateFromValue(json['syncState']),
  );
}

Map<String, Object?> startupHomeSyncRequestToMap(
  StartupHomeSyncRequest request,
) {
  return {
    'backfillVisibleHistories': request.backfillVisibleHistories,
    'conversationLimit': request.conversationLimit,
    'historyBackfillLimit': request.historyBackfillLimit,
    'historyBackfillMaxConversations': request.historyBackfillMaxConversations,
    'historyBackfillMaxPagesPerConversation':
        request.historyBackfillMaxPagesPerConversation,
    'startBackgroundConvergence': request.startBackgroundConvergence,
  };
}

StartupHomeSyncResponse startupHomeSyncResponseFromJson(Object? value) {
  final json = value is Map ? value : const <String, Object?>{};
  final degradedReason = json['degradedReason'];
  return StartupHomeSyncResponse(
    backgroundConvergenceStarted: json['backgroundConvergenceStarted'] is bool
        ? json['backgroundConvergenceStarted'] as bool
        : false,
    coldSyncPerformed: json['coldSyncPerformed'] is bool
        ? json['coldSyncPerformed'] as bool
        : false,
    degradedReason: degradedReason is String && degradedReason.isNotEmpty
        ? degradedReason
        : null,
    servedFromLocal: json['servedFromLocal'] is bool
        ? json['servedFromLocal'] as bool
        : false,
    snapshot: homeTimelineSnapshotFromJson(json['snapshot']),
  );
}

Map<String, Object?> conversationHistoryBackfillRequestToMap(
  ConversationHistoryBackfillRequest request,
) {
  return {
    'conversationId': request.conversationId,
    if (request.limit != null) 'limit': request.limit,
    if (request.maxPages != null) 'maxPages': request.maxPages,
  };
}

ConversationHistoryBackfillResponse conversationHistoryBackfillResponseFromJson(
  Object? value,
) {
  final json = value is Map ? value : const <String, Object?>{};
  return ConversationHistoryBackfillResponse(
    conversationId: json['conversationId'] is String
        ? json['conversationId'] as String
        : '',
    pagesLoaded: intValue(json['pagesLoaded']),
    oldestSeqBefore: intValue(json['oldestSeqBefore']),
    oldestSeqAfter: intValue(json['oldestSeqAfter']),
    hasMore: json['hasMore'] is bool ? json['hasMore'] as bool : false,
    completed: json['completed'] is bool ? json['completed'] as bool : false,
  );
}

ConversationTimelineSnapshot conversationTimelineSnapshotFromJson(
  Object? value,
) {
  final json = value is Map ? value : const <String, Object?>{};
  final conversationRaw = json['conversation'];
  return ConversationTimelineSnapshot(
    conversation: conversationRaw is Map
        ? conversationFromJson(conversationRaw)
        : null,
    messages: requiredWireListOfMaps(
      json['messages'],
      'messages',
      'ConversationTimelineSnapshot',
    ).map(messageFromJson).toList(growable: false),
    hasMore: json['hasMore'] is bool ? json['hasMore'] as bool : false,
  );
}

String viewTypeFromValue(Object? value, String source) {
  final wireValue = value is String ? value.trim() : '';
  return switch (wireValue) {
    'timeline' || 'conversationList' => wireValue,
    _ => throw FormatException(
      'invalid view type: ${wireValue.isEmpty ? '<empty>' : wireValue}',
      {'source': source, 'value': value},
    ),
  };
}

TimelineViewSnapshot timelineViewSnapshotFromJson(Object? value) {
  final json = mapValue(value);
  final conversationRaw = json['conversation'];
  final anchorKey = json['anchorKey'];
  return TimelineViewSnapshot(
    conversation: conversationRaw is Map
        ? conversationFromJson(conversationRaw)
        : null,
    messages: requiredWireListOfMaps(
      json['messages'],
      'messages',
      'TimelineViewSnapshot',
    ).map(messageFromJson).toList(growable: false),
    hasOlder: json['hasOlder'] is bool ? json['hasOlder'] as bool : false,
    hasNewer: json['hasNewer'] is bool ? json['hasNewer'] as bool : false,
    attachedToLatest: json['attachedToLatest'] is bool
        ? json['attachedToLatest'] as bool
        : false,
    anchorKey: anchorKey is String && anchorKey.trim().isNotEmpty
        ? anchorKey
        : null,
  );
}

ViewSnapshot viewSnapshotFromJson(Object? value) {
  final json = mapValue(value);
  final viewType = viewTypeFromValue(json['viewType'], 'ViewSnapshot');
  return ViewSnapshot(
    viewType: viewType,
    data: switch (viewType) {
      'timeline' => timelineViewSnapshotFromJson(json['data']),
      'conversationList' => homeTimelineSnapshotFromJson(json['data']),
      _ => throw StateError('unreachable view type: $viewType'),
    },
  );
}

ViewOpenResponse viewOpenResponseFromJson(Object? value) {
  final json = mapValue(value);
  return ViewOpenResponse(
    viewId: requiredStringField(json, 'viewId', 'ViewOpenResponse'),
    revision: requiredIntField(json, 'revision', 'ViewOpenResponse'),
    snapshot: viewSnapshotFromJson(json['snapshot']),
  );
}

TimelineViewPageResponse timelineViewPageResponseFromJson(Object? value) {
  final json = mapValue(value);
  return TimelineViewPageResponse(
    viewId: requiredStringField(json, 'viewId', 'TimelineViewPageResponse'),
    loadedCount: requiredIntField(
      json,
      'loadedCount',
      'TimelineViewPageResponse',
    ),
    revision: requiredIntField(json, 'revision', 'TimelineViewPageResponse'),
  );
}

String viewDeltaOpKindFromValue(Object? value) {
  final wireValue = value is String ? value.trim() : '';
  return switch (wireValue) {
    'insert' || 'update' || 'remove' || 'move' => wireValue,
    _ => throw FormatException(
      'invalid view delta op: ${wireValue.isEmpty ? '<empty>' : wireValue}',
      value,
    ),
  };
}

ViewDeltaOp viewDeltaOpFromJson(Object? value) {
  final json = mapValue(value);
  final rawItem = json['item'];
  if (rawItem != null && rawItem is! Map) {
    throw FormatException('ViewDeltaOp.item must be an object', rawItem);
  }
  return ViewDeltaOp(
    op: viewDeltaOpKindFromValue(json['op']),
    key: requiredStringField(json, 'key', 'ViewDeltaOp'),
    index: requiredIntField(json, 'index', 'ViewDeltaOp'),
    fromIndex: json['fromIndex'] == null
        ? null
        : requiredIntField(json, 'fromIndex', 'ViewDeltaOp'),
    item: rawItem is Map ? mapValue(rawItem) : null,
  );
}

List<String> viewDeltaRowKeys(Object? value, String field) {
  if (value is! List || value.any((key) => key is! String)) {
    throw FormatException('ViewDelta.$field must be a list of row keys', value);
  }
  return value.cast<String>().toList(growable: false);
}

/// Timeline deltas work by row key (`clientMsgId`, else `serverId`, else
/// `seq:<conversationId>:<conversationSeq>`): remove, upsert in place,
/// prepend, append, then reorder when present. Conversation-list deltas keep
/// the indexed `ops`.
ViewDelta viewDeltaFromJson(Object? value) {
  final json = mapValue(value);
  final viewType = viewTypeFromValue(json['viewType'], 'ViewDelta');
  if (viewType == 'conversationList') {
    return ViewDelta(
      viewType: viewType,
      ops: requiredWireListOfMaps(
        json['ops'],
        'ops',
        'ViewDelta',
      ).map(viewDeltaOpFromJson).toList(growable: false),
      totalUnread: json['totalUnread'] == null
          ? null
          : intValue(json['totalUnread']),
      syncState: json['syncState']?.toString(),
    );
  }
  List<Message> rows(String field) => json[field] == null
      ? const <Message>[]
      : requiredWireListOfMaps(
          json[field],
          field,
          'ViewDelta',
        ).map(messageFromJson).toList(growable: false);
  final rawConversation = json['conversation'];
  return ViewDelta(
    viewType: viewType,
    prepend: rows('prepend'),
    append: rows('append'),
    upsert: rows('upsert'),
    remove: json['remove'] == null
        ? const <String>[]
        : viewDeltaRowKeys(json['remove'], 'remove'),
    reorder: json['reorder'] == null
        ? null
        : viewDeltaRowKeys(json['reorder'], 'reorder'),
    hasOlder: json['hasOlder'] is bool ? json['hasOlder'] as bool : false,
    hasNewer: json['hasNewer'] is bool ? json['hasNewer'] as bool : false,
    attachedToLatest: json['attachedToLatest'] is bool
        ? json['attachedToLatest'] as bool
        : false,
    conversation: rawConversation is Map
        ? conversationFromJson(rawConversation)
        : null,
  );
}

ViewUpdate viewUpdateFromJson(Object? value) {
  final json = mapValue(value);
  final kind = json['kind']?.toString().trim() ?? '';
  if (kind != 'snapshot' && kind != 'delta') {
    throw FormatException(
      'invalid view update kind: ${kind.isEmpty ? '<empty>' : kind}',
      value,
    );
  }
  return ViewUpdate(
    viewId: requiredStringField(json, 'viewId', 'ViewUpdate'),
    revision: requiredIntField(json, 'revision', 'ViewUpdate'),
    kind: kind,
    snapshot: kind == 'delta' ? null : viewSnapshotFromJson(json['snapshot']),
    delta: kind == 'delta' ? viewDeltaFromJson(json['delta']) : null,
  );
}

CloseViewResponse closeViewResponseFromJson(Object? value) {
  final json = value is Map ? value : const <String, Object?>{};
  return CloseViewResponse(
    closed: json['closed'] is bool ? json['closed'] as bool : false,
  );
}

TimelineSyncState timelineSyncStateFromValue(Object? value) {
  final wireValue = value?.toString().trim() ?? '';
  return switch (wireValue) {
    'synced' => TimelineSyncState.synced,
    'partial' => TimelineSyncState.partial,
    'localReady' => TimelineSyncState.localReady,
    _ => throw FormatException(
      'invalid timeline sync state: ${wireValue.isEmpty ? '<empty>' : wireValue}',
      value,
    ),
  };
}

ConversationVersion conversationVersionFromJson(Object? value) {
  final json = mapValue(value);
  return ConversationVersion(
    conversationId: requiredStringField(
      json,
      'conversationId',
      'ConversationVersion',
    ),
    version: requiredIntField(json, 'version', 'ConversationVersion'),
  );
}

SyncConversationSummariesResponse syncConversationSummariesResponseFromJson(
  Object? value,
) {
  final json = mapValue(value);
  return SyncConversationSummariesResponse(
    changedConversations: requiredWireListOfMaps(
      json['changedConversations'],
      'changedConversations',
      'SyncConversationSummariesResponse',
    ).map(conversationVersionFromJson).toList(growable: false),
  );
}

SdkErrorPayload? sdkErrorPayloadFromJson(Object? value) {
  if (value == null) return null;
  if (value is String) {
    final normalized = value.trim();
    if (normalized.isEmpty || normalized == 'null') return null;
    return SdkErrorPayload(
      code: 'native_error',
      message: normalized,
    );
  }
  if (value is! Map) {
    final normalized = value.toString().trim();
    if (normalized.isEmpty || normalized == 'null') return null;
    throw FormatException('invalid SDK error payload field: error', {
      'field': 'error',
      'expected': 'object',
    });
  }
  return SdkErrorPayload(
    code: _requiredSdkErrorString(value, 'code'),
    machineReason: _sdkErrorMachineReason(value['machineReason']),
    message: _requiredSdkErrorString(value, 'message'),
    operation: _optionalSdkErrorString(value['operation'], 'error.operation'),
    retryable: _optionalSdkErrorBool(value['retryable'], 'error.retryable'),
    details: _optionalSdkErrorStringMap(value['details'], 'error.details'),
  );
}

String _requiredSdkErrorString(Map<dynamic, dynamic> json, String key) {
  final value = json[key];
  if (value is String && value.isNotEmpty) return value;
  throw FormatException('invalid SDK error payload field: error.$key', {
    'field': 'error.$key',
    'expected': 'non-empty string',
  });
}

/// The server's typed reason; empty for a local failure, which reads as no reason.
String? _sdkErrorMachineReason(Object? value) {
  if (value == null || value == '') return null;
  if (value is String) return value;
  throw FormatException('invalid SDK error payload field: error.machineReason', {
    'field': 'error.machineReason',
    'expected': 'string',
  });
}

String? _optionalSdkErrorString(Object? value, String field) {
  if (value == null) return null;
  if (value is String && value.isNotEmpty) return value;
  throw FormatException('invalid SDK error payload field: $field', {
    'field': field,
    'expected': 'non-empty string',
  });
}

bool? _optionalSdkErrorBool(Object? value, String field) {
  if (value == null) return null;
  if (value is bool) return value;
  throw FormatException('invalid SDK error payload field: $field', {
    'field': field,
    'expected': 'boolean',
  });
}

Map<String, String> _optionalSdkErrorStringMap(Object? value, String field) {
  if (value == null) return const {};
  if (value is! Map) {
    throw FormatException('invalid SDK error payload field: $field', {
      'field': field,
      'expected': 'object',
    });
  }
  return value.map((key, entry) {
    if (key is! String || entry is! String) {
      final suffix = key is String ? '.$key' : '';
      throw FormatException('invalid SDK error payload field: $field$suffix', {
        'field': '$field$suffix',
        'expected': 'string',
      });
    }
    return MapEntry(key, entry);
  });
}

Map<String, Object?> mapValue(Object? value) {
  if (value is! Map) {
    return const {};
  }
  return value.map((key, entry) => MapEntry(key.toString(), entry));
}

RichDocV2Normalized richDocV2NormalizedFromJson(Object? value) {
  final json = mapValue(value);
  final sourcePayload = json['sourcePayload'];
  return RichDocV2Normalized(
    docJson: json['docJson']?.toString() ?? '',
    contentSchema: json['contentSchema']?.toString() ?? '',
    version: intValue(json['version']),
    plainText: json['plainText']?.toString() ?? '',
    searchText: json['searchText']?.toString() ?? '',
    renderHints: mapValue(json['renderHints']),
    inputFormat: json['inputFormat']?.toString(),
    sourcePayload: sourcePayload == null ? null : mapValue(sourcePayload),
  );
}

Message messageFromJson(Object? value) {
  final json = mapValue(value);
  final rawContent = json['content'];
  MessageContent? content;
  if (rawContent is Map) {
    final contentJson = mapValue(rawContent);
    content = MessageContent(
      contentType: messageContentTypeFromJson(contentJson['contentType']),
      data: messageContentDataFromJson(contentJson),
    );
  }
  return Message(
    serverId: stringFieldOrEmpty(json, 'serverId'),
    clientMsgId: requiredStringField(json, 'clientMsgId', 'Message'),
    conversationId: requiredStringField(json, 'conversationId', 'Message'),
    conversationType: intValue(json['conversationType']),
    channelId: requiredStringField(json, 'channelId', 'Message'),
    senderId: requiredStringField(json, 'senderId', 'Message'),
    source: intValue(json['source']),
    conversationSeq: requiredIntField(json, 'conversationSeq', 'Message'),
    createdAt: intValue(json['createdAt']),
    clientCreatedAt: intValue(json['clientCreatedAt']),
    messageType: intValue(json['messageType']),
    content: content,
    senderName: stringFieldOrEmpty(json, 'senderName'),
    senderAvatar: stringFieldOrEmpty(json, 'senderAvatar'),
    senderDisplayName: stringFieldOrEmpty(json, 'senderDisplayName'),
    sender: requiredDisplayNameField(json, 'sender', 'Message'),
    replyTo: json['replyTo']?.toString(),
    quotePreview: json['quotePreview']?.toString(),
    threadId: json['threadId']?.toString(),
    status: intValue(json['status']),
    isRead: boolValue(json['isRead']),
    isRecalled: boolValue(json['isRecalled']),
    isEdited: boolValue(json['isEdited']),
    mentionUsers: stringList(json['mentionUsers']),
    mentionAll: boolValue(json['mentionAll']),
    attributes: stringMap(json['attributes']),
    extensions: bytesMap(json['extensions']),
    reactions: reactionList(json['reactions']),
    textPreview: stringFieldOrEmpty(json, 'textPreview'),
    version: intValue(json['version']),
    updatedAt: intValue(json['updatedAt']),
    localState: localStateFromJson(json['localState']),
    timelineKey: requiredStringField(json, 'timelineKey', 'Message'),
    timelineSortTs: requiredIntField(json, 'timelineSortTs', 'Message'),
    receipt: receiptFromJson(json['receipt']),
  );
}

MessageContentType messageContentTypeFromJson(Object? value) {
  final normalized = value?.toString().trim() ?? '';
  for (final type in MessageContentType.values) {
    if (messageContentTypeWireValue(type) == normalized) {
      return type;
    }
  }
  throw FormatException(
    'invalid message content type: ${normalized.isEmpty ? '<empty>' : normalized}',
    value,
  );
}

String messageContentTypeWireValue(MessageContentType type) {
  return type.name
      .replaceAllMapped(RegExp(r'([A-Z])'), (match) => '_${match.group(1)}')
      .toLowerCase();
}

Map<String, Object?> messageContentDataFromJson(
  Map<String, Object?> contentJson,
) {
  final out = <String, Object?>{};
  for (final entry in contentJson.entries) {
    switch (entry.key) {
      case 'contentType':
      case 'messageType':
        break;
      default:
        out[entry.key] = entry.value;
    }
  }
  return out;
}

SendMessageResponse sendAckFromJson(Object? value) {
  final json = mapValue(value);
  final success = json['success'] is bool ? json['success'] as bool : false;
  return SendMessageResponse(
    ackId: success
        ? requiredStringField(json, 'ackId', 'SendMessageResponse')
        : stringFieldOrEmpty(json, 'ackId'),
    serverId: success
        ? requiredStringField(json, 'serverId', 'SendMessageResponse')
        : stringFieldOrEmpty(json, 'serverId'),
    clientMsgId: success
        ? requiredStringField(json, 'clientMsgId', 'SendMessageResponse')
        : stringFieldOrEmpty(json, 'clientMsgId'),
    conversationId: success
        ? requiredStringField(json, 'conversationId', 'SendMessageResponse')
        : stringFieldOrEmpty(json, 'conversationId'),
    seq: success ? requiredIntField(json, 'seq', 'SendMessageResponse') : 0,
    timestamp: success
        ? requiredIntField(json, 'timestamp', 'SendMessageResponse')
        : 0,
    success: success,
    errorCode: json['errorCode'] == null ? 0 : intValue(json['errorCode']),
    errorMessage: json['errorMessage']?.toString() ?? '',
  );
}

const DisplayName _unknownPersonName = DisplayName(
  kind: DisplayNameKind.unnamed,
  entity: NameEntity.person,
  idTail: '',
);

/// A projected name: conversation `title` and `lastSender`, message `sender`,
/// participant `name`. An absent value reads as null;
/// [requiredDisplayNameField] reads the names every value carries. A value that
/// is there never fails, by the rule the kits read names with: an unknown
/// `entity` reads as `person`, an unknown `kind`, a `named` value without text
/// or a value that is not an object reads as an unnamed person without an id
/// tail, an unknown `source` is dropped.
DisplayName? displayNameFromJson(Object? value) {
  if (value == null) return null;
  if (value is! Map) return _unknownPersonName;
  final json = mapValue(value);
  switch (json['kind']) {
    case 'named':
      final text = json['text'];
      if (text is! String) return _unknownPersonName;
      final source = json['source'];
      return DisplayName(
        kind: DisplayNameKind.named,
        text: text,
        source: NameSource.values
            .where((candidate) => candidate.name == source)
            .firstOrNull,
      );
    case 'members':
      final names = json['names'];
      final total = json['total'];
      return DisplayName(
        kind: DisplayNameKind.members,
        names: names is List
            ? names.whereType<String>().toList(growable: false)
            : const <String>[],
        total: total is num && total.isFinite && total >= 0 ? total.truncate() : 0,
      );
    case 'unnamed':
      final entity = json['entity'];
      final idTail = json['idTail'];
      return DisplayName(
        kind: DisplayNameKind.unnamed,
        entity: NameEntity.values
                .where((candidate) => candidate.name == entity)
                .firstOrNull ??
            NameEntity.person,
        idTail: idTail is String ? idTail : '',
      );
    default:
      return _unknownPersonName;
  }
}

/// A name the value always carries: conversation `title`, message `sender`,
/// participant `name`. The core and this package ship together and the core
/// writes these on every value, so a value without one is a contract error, as
/// with any other required field.
DisplayName requiredDisplayNameField(
  Map<String, Object?> json,
  String key,
  String context,
) {
  final name = displayNameFromJson(json[key]);
  if (name == null) {
    throw ArgumentError.value(json, context, '$context.$key is required');
  }
  return name;
}

MessagePreview? messagePreviewFromJson(Object? value) {
  if (value == null) return null;
  final json = mapValue(value);
  return MessagePreview(
    messageId: requiredStringField(json, 'messageId', 'MessagePreview'),
    senderId: requiredStringField(json, 'senderId', 'MessagePreview'),
    type: requiredIntField(json, 'type', 'MessagePreview'),
    text: stringFieldOrEmpty(json, 'text'),
    time: requiredIntField(json, 'time', 'MessagePreview'),
  );
}

ConversationParticipant conversationParticipantFromJson(Object? value) {
  final json = mapValue(value);
  return ConversationParticipant(
    userId: requiredStringField(json, 'userId', 'ConversationParticipant'),
    roles: stringList(json['roles']),
    muted: boolValue(json['muted']),
    pinned: boolValue(json['pinned']),
    attributes: stringMap(json['attributes']),
    joinedAt: requiredIntField(json, 'joinedAt', 'ConversationParticipant'),
    nickname: requiredStringField(json, 'nickname', 'ConversationParticipant'),
    name: requiredDisplayNameField(json, 'name', 'ConversationParticipant'),
  );
}

List<ConversationParticipant> conversationParticipantList(
  Object? value,
  String field,
  String source,
) {
  return requiredWireListOfMaps(
    value,
    field,
    source,
  ).map(conversationParticipantFromJson).toList(growable: false);
}

ReactionEntry reactionFromJson(Object? value) {
  final json = mapValue(value);
  return ReactionEntry(
    emoji: requiredStringField(json, 'emoji', 'ReactionEntry'),
    userIds: stringList(json['userIds']),
    count: requiredIntField(json, 'count', 'ReactionEntry'),
  );
}

Map<String, Object?> reactionToMap(ReactionEntry reaction) {
  return {
    'emoji': reaction.emoji,
    'userIds': reaction.userIds,
    'count': reaction.count,
  };
}

List<ReactionEntry> reactionList(Object? value) {
  return requiredWireListOfMaps(
    value,
    'reactions',
    'Message',
  ).map(reactionFromJson).toList(growable: false);
}

/// The core's receipt projection; null when absent or malformed (an older core).
MessageReceiptView? receiptFromJson(Object? value) {
  if (value is! Map) return null;
  final json = mapValue(value);
  final raw = json['status'];
  MessageReceiptStatus? status;
  for (final candidate in MessageReceiptStatus.values) {
    if (candidate.name == raw) status = candidate;
  }
  if (status == null) return null;
  int? count(Map<String, Object?> summary, String key) {
    final value = summary[key];
    return value is int && value >= 0 ? value : null;
  }
  MessageReadSummary? readSummary;
  final rawSummary = json['readSummary'];
  if (rawSummary is Map) {
    final summary = mapValue(rawSummary);
    final readCount = count(summary, 'readCount');
    final unreadCount = count(summary, 'unreadCount');
    final total = count(summary, 'total');
    if (readCount != null && unreadCount != null && total != null && total > 0) {
      readSummary = MessageReadSummary(readCount: readCount, unreadCount: unreadCount, total: total);
    }
  }
  return MessageReceiptView(status: status, readSummary: readSummary);
}

MessageLocalState? localStateFromJson(Object? value) {
  if (value == null) return null;
  final json = mapValue(value);
  return MessageLocalState(
    sending: boolValue(json['sending']),
    failed: boolValue(json['failed']),
    isLocal: boolValue(json['isLocal']),
    sortTs: intValue(json['sortTs']),
  );
}

Map<String, Object?> localStateToMap(MessageLocalState state) {
  return {
    'sending': state.sending,
    'failed': state.failed,
    'isLocal': state.isLocal,
    'sortTs': state.sortTs,
  };
}

List<Map<dynamic, dynamic>> listOfMaps(Object? value) {
  if (value is! List) {
    return const [];
  }
  return value.whereType<Map<dynamic, dynamic>>().toList(growable: false);
}

List<Map<dynamic, dynamic>> requiredWireListOfMaps(
  Object? value,
  String field,
  String source,
) {
  if (value is! List) {
    throw FormatException(
      '$source response missing required `$field` array',
      value,
    );
  }
  final out = <Map<dynamic, dynamic>>[];
  for (var index = 0; index < value.length; index += 1) {
    final item = value[index];
    if (item is! Map<dynamic, dynamic>) {
      throw FormatException(
        '$source response `$field.$index` must be an object',
        item,
      );
    }
    out.add(item);
  }
  return out;
}

int intValue(Object? value) {
  if (value is num &&
      value.isFinite &&
      value >= 0 &&
      value.truncateToDouble() == value) {
    return value.toInt();
  }
  throw ArgumentError.value(
    value,
    'wire',
    'wire field must be an unsigned integer',
  );
}

/// 「必填」= 字段**存在且是字符串**，空串是合法值。
///
/// 曾经额外要求非空，于是同一条服务端数据在 web/iOS 上正常、在 Flutter/Android
/// 上直接抛异常：真实事件里 clientMsgId 常常是空串（别人发来的消息没有我方的
/// 客户端去重 id），protobuf3 又会把未设置的字符串序列化成 ""。
/// 结果一收到实时消息批就整批解码失败，等于收不到消息。
///
/// TypeScript 与 Swift 一直是「存在即可」，四端必须一致，
/// 否则同一份 wire 数据在不同端有不同结果。
String requiredStringField(
  Map<String, Object?> json,
  String key,
  String context,
) {
  final value = json[key];
  if (value is! String) {
    throw ArgumentError.value(json, context, '$context.$key is required');
  }
  return value;
}

int requiredIntField(Map<String, Object?> json, String key, String context) {
  final value = json[key];
  if (value == null || (value is String && value.trim().isEmpty)) {
    throw ArgumentError.value(json, context, '$context.$key is required');
  }
  if (value is num &&
      value.isFinite &&
      value >= 0 &&
      value.truncateToDouble() == value) {
    return value.toInt();
  }
  throw ArgumentError.value(
    json,
    context,
    '$context.$key must be an unsigned integer',
  );
}

int? optionalInt(Object? value) {
  if (value == null) return null;
  return intValue(value);
}

bool boolValue(Object? value) {
  if (value is bool) return value;
  throw ArgumentError.value(value, 'wire', 'wire field must be a boolean');
}

List<String> stringList(Object? value) {
  if (value is! List) {
    throw ArgumentError.value(
      value,
      'wire',
      'wire field must be a string array',
    );
  }
  return value
      .map((item) {
        if (item is! String) {
          throw ArgumentError.value(
            item,
            'wire',
            'wire array item must be a string',
          );
        }
        return item;
      })
      .toList(growable: false);
}

Map<String, String> stringMap(Object? value) {
  if (value is! Map) {
    throw ArgumentError.value(value, 'wire', 'wire field must be a string map');
  }
  return value.map((key, entry) {
    if (key is! String || entry is! String) {
      throw ArgumentError.value(
        value,
        'wire',
        'wire map entries must be strings',
      );
    }
    return MapEntry(key, entry);
  });
}

Map<String, List<int>> bytesMap(Object? value) {
  if (value is! Map) return const {};
  return value.map((key, entry) {
    final bytes = entry is List
        ? entry
              .whereType<num>()
              .map((item) => item.toInt())
              .toList(growable: false)
        : const <int>[];
    return MapEntry(key.toString(), bytes);
  });
}

// RUST-OWNED WIRE BOUNDARY: BEGIN
/// The FFI wire contract is canonical camelCase SDK JSON.
Object? wireEncodeRequest(Object? value) {
  return value;
}

/// The FFI wire contract is canonical camelCase SDK JSON.
Object? wireDecodeResponse(Object? value) {
  return value;
}
// RUST-OWNED WIRE BOUNDARY: END
