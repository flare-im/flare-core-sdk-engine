import Foundation
import CFlareImCoreSdkFFI
import FlarePlatformLifecycle

/// Real FFI bridge over `libflare_im_core_sdk_ffi`, aligned with Flutter `FfiNativeBridge`.
public final class FfiNativeBridge: NativeBridgeProtocol, @unchecked Sendable {
    private let bindings: FlareNativeBindings
    private var handle: FlareHandle = 0
    private var released = false
    private var contractVersionChecked = false
    /// `sdk.init` 时补进配置的应用版本信息（宿主在请求里显式给的值优先）。
    private let appInfo: FlareAppInfo

    /// `appInfo` 默认从 `Bundle.main` 自动采集；扩展、测试等 bundle 不是宿主应用时可显式传入。
    public init(libraryPath: String? = nil, appInfo: FlareAppInfo = .fromBundle()) throws {
        bindings = try FlareNativeBindings(libraryPath: libraryPath)
        self.appInfo = appInfo
    }

    public func invoke(_ descriptor: NativeCallDescriptor, request: AnySendable?) async throws -> AnySendable {
        if descriptor.operation != SdkOperations.diagnosticsFfiContractVersion {
            try ensureContractVersion()
        }
        let value = try await invokeRaw(descriptor, request: request)
        return AnySendable(value as Any)
    }

    private func invokeRaw(_ descriptor: NativeCallDescriptor, request: AnySendable?) async throws -> Any? {
        if descriptor.transport == "message-dispatch-json",
           descriptor.cApi == "flare_message_dispatch_json" {
            let future = try await dispatchJson(FfiJson.asMap(request?.value), opOverride: descriptor.dispatchOp)
            if descriptor.returnMode == "callback-unit" {
                return nil
            }
            return FfiJson.asResponseMap(future)
        }

        if descriptor.transport == "capability-dispatch-json",
           descriptor.cApi == "flare_capability_dispatch_json" {
            let future = try await capabilityDispatchJson(FfiJson.asMap(request?.value), opOverride: descriptor.dispatchOp)
            if descriptor.returnMode == "callback-unit" {
                return nil
            }
            return FfiJson.asResponseMap(future)
        }

        if descriptor.transport == "media-dispatch-json",
           descriptor.cApi == "flare_media_dispatch_json" {
            let future = try await mediaDispatchJson(FfiJson.asMap(request?.value), opOverride: descriptor.dispatchOp)
            if descriptor.returnMode == "callback-unit" {
                return nil
            }
            return FfiJson.asResponseMap(future)
        }

        if descriptor.transport == "contract-invoke-json",
           descriptor.cApi == "flare_sdk_invoke_json" {
            let future = try await contractInvokeJson(descriptor.operation, FfiJson.asMap(request?.value))
            if descriptor.returnMode == "callback-unit" {
                return nil
            }
            return FfiJson.asResponseValue(future)
        }

        if descriptor.transport == "dispatch-json",
           descriptor.cApi == "flare_message_build_json" {
            return FfiJson.asResponseMap(try await callBuildJson(request?.value))
        }

        if descriptor.transport == "dispatch-json",
           descriptor.cApi == "flare_message_dispatch_json" {
            return FfiJson.asResponseMap(try await dispatchJson(FfiJson.asMap(request?.value), opOverride: descriptor.dispatchOp))
        }

        switch descriptor.operation {
        case "sdk.create":
            return try createHandle()
        case "sdk.dispose":
            try await dispose()
            return nil
        case "sdk.hard_reset":
            bindings.sdkHardReset()
            FfiTokenRefreshRouter.shared.removeAll()
            handle = 0
            released = true
            return nil
        case "sdk.init":
            return try await callWithJson(bindings.sdkInit, appInfo.fill(FfiJson.asMap(request?.value)))
        case "sdk.uninit":
            return try await call0(bindings.sdkUninit)
        case "sdk.login":
            return try await login(FfiJson.asMap(request?.value))
        case "sdk.prepare":
            return try await prepare(FfiJson.asMap(request?.value))
        case "sdk.connect":
            return try await connect(FfiJson.asMap(request?.value))
        case "sdk.update_access_token":
            return try await updateAccessToken(FfiJson.asMap(request?.value))
        case "sdk.logout":
            return try await call0(bindings.sdkLogout)
        case "sdk.current_user_id":
            return FfiJson.asResponseMap(try await call0(bindings.sdkCurrentUserId))
        case "sdk.is_connected":
            return bindings.sdkIsConnected(try requireHandle())
        case "sdk.session_active":
            return bindings.sdkSessionActive(try requireHandle())
        case "connection.disconnect":
            return try await call0(bindings.sdkDisconnect)
        case "message.create_text":
            return FfiJson.asResponseMap(try await createTextMessage(FfiJson.asMap(request?.value)))
        case "message.send":
            return FfiJson.asResponseMap(try await callWithJson(bindings.messageSend, request?.value))
        case "message.list":
            return FfiJson.asResponseMap(try await listMessages(FfiJson.asMap(request?.value)))
        case "message.recall":
            return try await messageMutation(bindings.messageRecall, FfiJson.asMap(request?.value))
        case "message.delete":
            return try await messageMutation(bindings.messageDelete, FfiJson.asMap(request?.value))
        case "sync.conversation":
            return try await callWithString(bindings.syncConversation, FfiFields.string(request?.value, "conversationId"))
        case "sync.messages":
            return try await syncMessages(FfiJson.asMap(request?.value))
        case "presence.get":
            return FfiJson.asResponseMap(try await callWithString(bindings.presenceGet, FfiFields.string(request?.value, "userId")))
        case "presence.batch_get":
            return FfiJson.asResponseMap(try await callWithJson(bindings.presenceBatchGet, FfiFields.list(request?.value, "userIds")))
        case "presence.subscribe":
            return try await callWithJson(bindings.presenceSubscribe, FfiFields.list(request?.value, "userIds"))
        case "media.upload_file":
            return FfiJson.asResponseMap(try await mediaUploadPath(bindings.mediaUploadFile, FfiJson.asMap(request?.value)))
        case "media.upload_image":
            return FfiJson.asResponseMap(try await mediaUploadPath(bindings.mediaUploadImage, FfiJson.asMap(request?.value)))
        case "media.upload_video":
            return FfiJson.asResponseMap(try await mediaUploadPath(bindings.mediaUploadVideo, FfiJson.asMap(request?.value)))
        case "media.upload_bytes":
            return FfiJson.asResponseMap(try await mediaUploadBytes(FfiJson.asMap(request?.value)))
        case "media.delete_file":
            return try await mediaDeleteFile(FfiJson.asMap(request?.value))
        case "media.cancel_user_file_download":
            return try await mediaCancelUserFileDownload(FfiJson.asMap(request?.value))
        case "media.download_file_to_downloads":
            return FfiJson.asResponseMap(try await callWithJson(bindings.mediaDownloadFileToDownloads, request?.value))
        case "event.subscribe":
            return try subscribeEvents(request?.value)
        case "event.subscribe_batch":
            return try subscribeEventsBatch(request?.value)
        case "event.unsubscribe":
            unsubscribe(FfiJson.asMap(request?.value))
            return nil
        case "event.unsubscribe_all":
            bindings.eventUnsubscribeAll()
            return nil
        case "diagnostics.sdk_version":
            return ["version": takeString(bindings.sdkVersion())]
        case "diagnostics.ffi_contract_version":
            return ["version": takeString(bindings.sdkFfiContractVersion())]
        case "diagnostics.data_root":
            return FfiJson.asResponseMap(try await call0(bindings.sdkDataRoot))
        default:
            throw FlareSdkException(
                code: "unsupported_operation",
                message: "Unsupported Apple FFI operation: \(descriptor.operation)",
                operation: descriptor.operation
            )
        }
    }

    /// Installs (or, with nil, removes) the host's connect token refresher. The core calls back on
    /// an SDK thread; the refresher runs on a task and its token goes back to the core.
    public func setConnectTokenRefresher(_ refresher: ConnectTokenRefresher?) async throws {
        try ensureContractVersion()
        let handle = try requireHandle()
        if let refresher {
            let bindings = self.bindings
            FfiTokenRefreshRouter.shared.install(handle: handle, refresher: refresher) { requestId, token in
                // FLARE_ERR_INVALID_PARAM: the core stopped waiting; nothing to retry.
                _ = FfiStringCodec.withCString(token ?? "") { pointer in
                    bindings.sdkCompleteConnectTokenRefresh(handle, requestId, pointer)
                }
            }
        } else {
            FfiTokenRefreshRouter.shared.remove(handle: handle)
        }
        let id = FfiCallbackRouter.shared.reserveContextId()
        do {
            _ = try await FfiCallbackRouter.shared.wait(for: id, operation: "sdk.set_connect_token_refresher") { context in
                bindings.sdkSetConnectTokenRefresher(
                    handle,
                    UnsafeMutableRawPointer(bitPattern: UInt(handle)),
                    refresher == nil ? nil : FfiNativeCallbacks.tokenRefresh,
                    context,
                    FfiNativeCallbacks.result
                )
            }
        } catch {
            if refresher != nil {
                FfiTokenRefreshRouter.shared.remove(handle: handle)
            }
            throw error
        }
    }

    private func ensureContractVersion() throws {
        if contractVersionChecked {
            return
        }
        try FfiContractVersionGuard.validate(takeString(bindings.sdkFfiContractVersion()))
        contractVersionChecked = true
    }

    private func createHandle() throws -> [String: AnySendable] {
        if handle == 0 || released {
            handle = bindings.sdkCreate()
            released = false
        }
        if handle == 0 {
            throw FlareSdkException(code: "native_create_failed", message: "flare_sdk_create returned an invalid handle.", operation: "sdk.create")
        }
        return ["handle": AnySendable(handle)]
    }

    private func dispose() async throws {
        if handle != 0, !released {
            bindings.sdkRelease(handle)
            FfiTokenRefreshRouter.shared.remove(handle: handle)
        }
        handle = 0
        released = true
    }

    private func requireHandle() throws -> FlareHandle {
        if handle == 0 || released {
            _ = try createHandle()
        }
        return handle
    }

    private func enqueue(
        _ operation: String,
        _ start: (FlareHandle, UnsafeMutableRawPointer) -> Int32
    ) async throws -> Any? {
        let activeHandle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: operation) { context in
            start(activeHandle, context)
        }
    }

    private func call0(_ fn: Async0Fn) async throws -> Any? {
        try await enqueue("native.call0") { handle, context in
            fn(handle, context, FfiNativeCallbacks.result)
        }
    }

    private func callWithString(_ fn: AsyncStringFn, _ value: String) async throws -> Any? {
        try await enqueue("native.string") { handle, context in
            FfiStringCodec.withCString(value) { ptr in
                fn(handle, ptr, context, FfiNativeCallbacks.result)
            }
        }
    }

    private func callWithJson(_ fn: AsyncJsonFn, _ request: Any?) async throws -> Any? {
        let encoded = FfiWireBoundary.encodeRequest(FfiJson.asMap(request))
        return try await callWithString(fn, FfiJson.encode(encoded))
    }

    private func callBuildJson(_ request: Any?) async throws -> Any? {
        try await callWithJson(bindings.messageBuild, request)
    }

    private func login(_ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "sdk.login") { context in
            let userIdValue = FfiFields.string(request, "userId")
            let tokenValue = FfiFields.string(request, "token")
            let storeConfig = FfiFields.string(request, "storeConfigJson")
            return userIdValue.withCString { userId in
                tokenValue.withCString { token in
                    guard !storeConfig.isEmpty else {
                        return bindings.sdkLogin(handle, userId, token, nil, context, FfiNativeCallbacks.result)
                    }
                    return storeConfig.withCString { storePtr in
                        bindings.sdkLogin(handle, userId, token, storePtr, context, FfiNativeCallbacks.result)
                    }
                }
            }
        }
    }

    private func updateAccessToken(_ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "sdk.update_access_token") { context in
            FfiStringCodec.withCString(FfiFields.string(request, "accessToken")) { accessToken in
                FfiStringCodec.withCString(FfiFields.string(request, "tenantId")) { tenantId in
                    bindings.sdkUpdateAccessToken(handle, accessToken, tenantId, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func prepare(_ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "sdk.prepare") { context in
            FfiFields.string(request, "userId").withCString { userId in
                let storeConfig = FfiFields.string(request, "storeConfigJson")
                if storeConfig.isEmpty {
                    return bindings.sdkPrepare(handle, userId, nil, context, FfiNativeCallbacks.result)
                }
                return storeConfig.withCString { store in
                    bindings.sdkPrepare(handle, userId, store, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func connect(_ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "sdk.connect") { context in
            FfiFields.string(request, "userId").withCString { userId in
                FfiFields.string(request, "token").withCString { token in
                    bindings.sdkConnect(handle, userId, token, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func createTextMessage(_ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "message.create_text") { context in
            FfiStringCodec.withCString(FfiFields.string(request, "conversationId")) { conversationId in
                FfiStringCodec.withCString(FfiFields.string(request, "text")) { text in
                    bindings.messageCreateText(handle, conversationId, text, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func listMessages(_ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "message.list") { context in
            FfiStringCodec.withCString(FfiFields.string(request, "conversationId")) { conversationId in
                bindings.messageList(handle, conversationId, UInt64(FfiFields.int(request, "beforeSeq")), Int32(FfiFields.int(request, "limit", default: 20)), context, FfiNativeCallbacks.result)
            }
        }
    }

    private func messageMutation(_ fn: Async2StringFn, _ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "message.mutation") { context in
            FfiStringCodec.withCString(FfiFields.string(request, "conversationId")) { conversationId in
                FfiStringCodec.withCString(FfiFields.string(request, "messageId")) { messageId in
                    fn(handle, conversationId, messageId, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func syncMessages(_ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "sync.messages") { context in
            FfiStringCodec.withCString(FfiFields.string(request, "conversationId")) { conversationId in
                bindings.syncMessages(handle, conversationId, UInt64(FfiFields.int(request, "lastSeq")), Int32(FfiFields.int(request, "limit", default: 50)), context, FfiNativeCallbacks.result)
            }
        }
    }

    private func mediaUploadPath(_ fn: Async2StringFn, _ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "media.upload_path") { context in
            FfiStringCodec.withCString(FfiFields.string(request, "absolutePath")) { path in
                let optionsJson = FfiFields.optionalJsonString(request, "options")
                return optionsJson.withCString { optionsPtr in
                    fn(handle, path, optionsPtr, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func mediaUploadBytes(_ request: [String: Any]) async throws -> Any? {
        guard let bytes = request["bytes"] as? Data else {
            throw FlareSdkException(code: "invalid_param", message: "Missing bytes field.", operation: "media.upload_bytes")
        }
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "media.upload_bytes") { context in
            return bytes.withUnsafeBytes { raw in
                let view = FlareBytesView(ptr: raw.bindMemory(to: UInt8.self).baseAddress, len: bytes.count)
                return FfiStringCodec.withCString(FfiFields.string(request, "fileName")) { fileName in
                    FfiStringCodec.withCString(FfiFields.string(request, "mimeType")) { mimeType in
                        let optionsJson = FfiFields.optionalJsonString(request, "options")
                        return optionsJson.withCString { optionsPtr in
                            bindings.mediaUploadBytes(handle, view, fileName, mimeType, optionsPtr, context, FfiNativeCallbacks.result)
                        }
                    }
                }
            }
        }
    }

    private func mediaDeleteFile(_ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "media.delete_file") { context in
            FfiStringCodec.withCString(FfiFields.string(request, "fileId")) { fileId in
                bindings.mediaDeleteFile(handle, fileId, FfiFields.bool(request, "hardDelete"), context, FfiNativeCallbacks.result)
            }
        }
    }

    /// The C function answers through the result callback with `{cancelled: bool}`.
    private func mediaCancelUserFileDownload(_ request: [String: Any]) async throws -> Bool {
        let result = try await callWithString(
            bindings.mediaCancelUserFileDownload, FfiFields.string(request, "downloadKey"))
        if let map = result as? [String: Any] { return map["cancelled"] as? Bool ?? false }
        return result as? Bool ?? false
    }

    private func dispatchJson(_ request: [String: Any], opOverride: String? = nil) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "message.dispatch") { context in
            let op = opOverride ?? (request["op"] as? String ?? "")
            var params = request
            params.removeValue(forKey: "op")
            return op.withCString { opPtr in
                let json = FfiJson.encode(FfiWireBoundary.encodeRequest(params) ?? params)
                return json.withCString { jsonPtr in
                    bindings.messageDispatch(handle, opPtr, jsonPtr, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func capabilityDispatchJson(_ request: [String: Any], opOverride: String? = nil) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "capability.dispatch") { context in
            let op = opOverride ?? (request["op"] as? String ?? "")
            var params = request
            params.removeValue(forKey: "op")
            return op.withCString { opPtr in
                let json = FfiJson.encode(FfiWireBoundary.encodeRequest(params) ?? params)
                return json.withCString { jsonPtr in
                    bindings.capabilityDispatch(handle, opPtr, jsonPtr, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func mediaDispatchJson(_ request: [String: Any], opOverride: String? = nil) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: "media.dispatch") { context in
            let op = opOverride ?? (request["op"] as? String ?? "")
            var params = request
            params.removeValue(forKey: "op")
            return op.withCString { opPtr in
                let json = FfiJson.encode(FfiWireBoundary.encodeRequest(params) ?? params)
                return json.withCString { jsonPtr in
                    bindings.mediaDispatch(handle, opPtr, jsonPtr, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func contractInvokeJson(_ operation: String, _ request: [String: Any]) async throws -> Any? {
        let handle = try requireHandle()
        let id = FfiCallbackRouter.shared.reserveContextId()
        return try await FfiCallbackRouter.shared.wait(for: id, operation: operation) { context in
            return operation.withCString { apiIdPtr in
                let json = FfiJson.encode(FfiWireBoundary.encodeRequest(request) ?? request)
                return json.withCString { jsonPtr in
                    bindings.sdkInvokeJson(handle, apiIdPtr, jsonPtr, context, FfiNativeCallbacks.result)
                }
            }
        }
    }

    private func subscribeEvents(_ request: Any?) throws -> [String: AnySendable] {
        let contextId = FfiCallbackRouter.shared.reserveContextId()
        if let handler = (request as? [String: Any])?["handler"] as? (Int, Any?) -> Void {
            FfiCallbackRouter.shared.registerEventSink(contextId, handler: handler)
        }
        let handle = try requireHandle()
        let subscription = bindings.eventSubscribe(handle, UnsafeMutableRawPointer(bitPattern: UInt(contextId)), FfiNativeCallbacks.event)
        if subscription == 0 {
            FfiCallbackRouter.shared.removeEventSink(contextId)
            throw FlareSdkException(code: "event_subscribe_failed", message: "flare_event_subscribe returned an invalid subscription handle.", operation: "event.subscribe")
        }
        return [
            "subscription": AnySendable(subscription),
            "context": AnySendable(contextId),
        ]
    }

    private func subscribeEventsBatch(_ request: Any?) throws -> [String: AnySendable] {
        let contextId = FfiCallbackRouter.shared.reserveContextId()
        if let handler = (request as? [String: Any])?["handler"] as? (Int, Any?) -> Void {
            FfiCallbackRouter.shared.registerEventSink(contextId, handler: handler)
        }
        let handle = try requireHandle()
        let subscription = bindings.eventSubscribeBatch(handle, UnsafeMutableRawPointer(bitPattern: UInt(contextId)), FfiNativeCallbacks.eventBatch)
        if subscription == 0 {
            FfiCallbackRouter.shared.removeEventSink(contextId)
            throw FlareSdkException(code: "event_subscribe_failed", message: "flare_event_subscribe_batch returned an invalid subscription handle.", operation: "event.subscribe_batch")
        }
        return [
            "subscription": AnySendable(subscription),
            "context": AnySendable(contextId),
        ]
    }

    private func unsubscribe(_ request: [String: Any]) {
        let subscription = UInt64(FfiFields.int(request, "subscription"))
        bindings.eventUnsubscribe(subscription)
        let context = FfiFields.int(request, "context")
        if context != 0 {
            FfiCallbackRouter.shared.removeEventSink(UInt64(context))
        }
    }

    private func takeString(_ value: FlareString) -> String {
        let text = FfiStringCodec.decode(value)
        bindings.stringFree(value)
        return text
    }

    private func camel(_ snake: String) -> String {
        snake.split(separator: "_").enumerated().map { index, part in
            index == 0 ? String(part) : part.capitalized
        }.joined()
    }
}
