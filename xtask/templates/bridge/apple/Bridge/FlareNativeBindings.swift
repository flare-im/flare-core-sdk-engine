import Foundation
import CFlareImCoreSdkFFI

/// dlsym bindings for the Flare Core C ABI.
public final class FlareNativeBindings: @unchecked Sendable {
    public static weak var shared: FlareNativeBindings?

    private let handle: UnsafeMutableRawPointer

    public init(libraryPath: String? = nil) throws {
        handle = try NativeLibraryLoader.load(libraryPath: libraryPath)
        FlareNativeBindings.shared = self
        bindAll()
    }

    deinit { dlclose(handle) }

    // MARK: - Sync symbols
    public private(set) var sdkCreate: SyncCreateFn!
    public private(set) var sdkRelease: SyncReleaseFn!
    public private(set) var sdkHardReset: SyncHardResetFn!
    public private(set) var sdkIsConnected: SyncBoolFn!
    public private(set) var sdkSessionActive: SyncBoolFn!
    public private(set) var sdkVersion: SyncStringFn!
    public private(set) var sdkFfiContractVersion: SyncStringFn!
    public private(set) var stringFree: SyncStringFreeFn!
    public private(set) var errorHeapFree: ErrorHeapFreeFn!
    public private(set) var eventUnsubscribe: EventUnsubscribeFn!
    public private(set) var eventUnsubscribeAll: EventUnsubscribeAllFn!
    public private(set) var mediaCancelUserFileDownload: AsyncStringFn!

    // MARK: - Async symbols
    public private(set) var sdkInit: AsyncJsonFn!
    public private(set) var sdkUninit: Async0Fn!
    public private(set) var sdkLogin: AsyncLoginFn!
    public private(set) var sdkPrepare: Async2StringFn!
    public private(set) var sdkConnect: Async2StringFn!
    public private(set) var sdkUpdateAccessToken: Async2StringFn!
    public private(set) var sdkSetConnectTokenRefresher: SetConnectTokenRefresherFn!
    public private(set) var sdkCompleteConnectTokenRefresh: CompleteConnectTokenRefreshFn!
    public private(set) var sdkLogout: Async0Fn!
    public private(set) var sdkCurrentUserId: Async0Fn!
    public private(set) var sdkDataRoot: Async0Fn!
    public private(set) var sdkDisconnect: Async0Fn!
    public private(set) var capabilityDispatch: AsyncDispatchFn!
    public private(set) var mediaDispatch: AsyncDispatchFn!
    public private(set) var sdkInvokeJson: AsyncDispatchFn!
    public private(set) var messageCreateText: Async2StringFn!
    public private(set) var messageSend: AsyncJsonFn!
    public private(set) var messageList: AsyncStringUIntIntFn!
    public private(set) var messageRecall: Async2StringFn!
    public private(set) var messageDelete: Async2StringFn!
    public private(set) var messageBuild: AsyncJsonFn!
    public private(set) var messageDispatch: AsyncDispatchFn!
    public private(set) var syncConversation: AsyncStringFn!
    public private(set) var syncMessages: AsyncStringUIntIntFn!
    public private(set) var presenceGet: AsyncStringFn!
    public private(set) var presenceBatchGet: AsyncJsonFn!
    public private(set) var presenceSubscribe: AsyncJsonFn!
    public private(set) var mediaUploadFile: Async2StringFn!
    public private(set) var mediaUploadImage: Async2StringFn!
    public private(set) var mediaUploadVideo: Async2StringFn!
    public private(set) var mediaUploadBytes: AsyncUploadBytesFn!
    public private(set) var mediaDeleteFile: AsyncStringBoolFn!
    public private(set) var mediaDownloadFileToDownloads: AsyncJsonFn!
    public private(set) var eventSubscribe: EventSubscribeFn!
    public private(set) var eventSubscribeBatch: EventSubscribeBatchFn!

    private func bindAll() {
        sdkCreate = load("flare_sdk_create")
        sdkRelease = load("flare_sdk_release")
        sdkHardReset = load("flare_sdk_hard_reset")
        sdkIsConnected = load("flare_sdk_is_connected")
        sdkSessionActive = load("flare_sdk_session_active")
        sdkVersion = load("flare_sdk_version")
        sdkFfiContractVersion = load("flare_sdk_ffi_contract_version")
        stringFree = load("flare_string_free")
        errorHeapFree = load("flare_error_heap_free")
        eventUnsubscribe = load("flare_event_unsubscribe")
        eventUnsubscribeAll = load("flare_event_unsubscribe_all")
        mediaCancelUserFileDownload = load("flare_media_cancel_user_file_download")
        sdkInit = load("flare_sdk_init")
        sdkUninit = load("flare_sdk_uninit")
        sdkLogin = load("flare_sdk_login")
        sdkPrepare = load("flare_sdk_prepare")
        sdkConnect = load("flare_sdk_connect")
        sdkUpdateAccessToken = load("flare_sdk_update_access_token")
        sdkSetConnectTokenRefresher = load("flare_sdk_set_connect_token_refresher")
        sdkCompleteConnectTokenRefresh = load("flare_sdk_complete_connect_token_refresh")
        sdkLogout = load("flare_sdk_logout")
        sdkCurrentUserId = load("flare_sdk_current_user_id")
        sdkDataRoot = load("flare_sdk_data_root")
        sdkDisconnect = load("flare_sdk_disconnect")
        capabilityDispatch = load("flare_capability_dispatch_json")
        mediaDispatch = load("flare_media_dispatch_json")
        sdkInvokeJson = load("flare_sdk_invoke_json")
        messageCreateText = load("flare_message_create_text")
        messageSend = load("flare_message_send")
        messageList = load("flare_message_list")
        messageRecall = load("flare_message_recall")
        messageDelete = load("flare_message_delete")
        messageBuild = load("flare_message_build_json")
        messageDispatch = load("flare_message_dispatch_json")
        syncConversation = load("flare_sdk_sync_conversation")
        syncMessages = load("flare_sdk_sync_messages")
        presenceGet = load("flare_sdk_get_user_presence")
        presenceBatchGet = load("flare_sdk_batch_get_user_presence")
        presenceSubscribe = load("flare_sdk_subscribe_user_presence")
        mediaUploadFile = load("flare_media_upload_file")
        mediaUploadImage = load("flare_media_upload_image")
        mediaUploadVideo = load("flare_media_upload_video")
        mediaUploadBytes = load("flare_media_upload_bytes")
        mediaDeleteFile = load("flare_media_delete_file")
        mediaDownloadFileToDownloads = load("flare_media_download_file_to_downloads")
        eventSubscribe = load("flare_event_subscribe")
        eventSubscribeBatch = load("flare_event_subscribe_batch")
    }

    private func load<T>(_ name: String) -> T {
        guard let symbol = dlsym(handle, name) else {
            fatalError("Missing FFI symbol: \(name)")
        }
        return unsafeBitCast(symbol, to: T.self)
    }
}

public typealias SyncCreateFn = @convention(c) () -> FlareHandle
public typealias SyncReleaseFn = @convention(c) (FlareHandle) -> Void
public typealias SyncHardResetFn = @convention(c) () -> Void
public typealias SyncBoolFn = @convention(c) (FlareHandle) -> Bool
public typealias SyncStringFn = @convention(c) () -> FlareString
public typealias SyncStringFreeFn = @convention(c) (FlareString) -> Void
public typealias ErrorHeapFreeFn = @convention(c) (UnsafePointer<FlareError>?) -> Void
public typealias EventUnsubscribeFn = @convention(c) (FlareSubscriptionHandle) -> Void
public typealias EventUnsubscribeAllFn = @convention(c) () -> Void
public typealias Async0Fn = @convention(c) (FlareHandle, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncJsonFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncStringFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias Async2StringFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncStringBoolFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, Bool, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncStringIntFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, Int32, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncStringUIntFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, UInt32, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncStringUInt64Fn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, UInt64, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncStringUIntIntFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, UInt64, Int32, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncUIntFn = @convention(c) (FlareHandle, UInt64, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncOptionalStringFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncDispatchFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncUploadBytesFn = @convention(c) (FlareHandle, FlareBytesView, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
public typealias AsyncLoginFn = @convention(c) (FlareHandle, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafePointer<CChar>?, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
/// `FlareTokenRefreshCallback`: the core asks the host for a new connect token (`request_id`).
public typealias TokenRefreshCallbackFn = @convention(c) (UnsafeMutableRawPointer?, UInt64) -> Void
/// `flare_sdk_set_connect_token_refresher`; a nil callback removes the refresher.
public typealias SetConnectTokenRefresherFn = @convention(c) (FlareHandle, UnsafeMutableRawPointer?, TokenRefreshCallbackFn?, UnsafeMutableRawPointer?, FlareResultCallback) -> Int32
/// `flare_sdk_complete_connect_token_refresh`; a nil token means none was obtained.
public typealias CompleteConnectTokenRefreshFn = @convention(c) (FlareHandle, UInt64, UnsafePointer<CChar>?) -> Int32
public typealias EventSubscribeFn = @convention(c) (FlareHandle, UnsafeMutableRawPointer?, FlareEventCallback) -> FlareSubscriptionHandle
public typealias EventSubscribeBatchFn = @convention(c) (FlareHandle, UnsafeMutableRawPointer?, FlareEventBatchCallback) -> FlareSubscriptionHandle
