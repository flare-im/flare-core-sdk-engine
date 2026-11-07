import Foundation
import CFlareImCoreSdkFFI

enum FfiJson {
    static func encode(_ value: Any?) -> String {
        guard let value else { return "{}" }
        if JSONSerialization.isValidJSONObject(value),
           let data = try? JSONSerialization.data(withJSONObject: value),
           let text = String(data: data, encoding: .utf8) {
            return text
        }
        return "{}"
    }

    static func decode(_ raw: String) -> Any? {
        guard !raw.isEmpty, let data = raw.data(using: .utf8) else { return nil }
        return try? JSONSerialization.jsonObject(with: data)
    }

    static func asMap(_ value: Any?) -> [String: Any] {
        if let map = value as? [String: Any] { return map.mapValues { unwrapSendableValue($0) } }
        if let map = value as? [String: AnySendable] { return map.mapValues { unwrapSendableValue($0.value) } }
        if let sendable = value as? AnySendable { return asMap(sendable.value) }
        return [:]
    }

    static func asResponseMap(_ value: Any?) -> [String: AnySendable] {
        let decoded = FfiWireBoundary.decodeResponse(asMap(value)) as? [String: Any] ?? asMap(value)
        return decoded.mapValues { AnySendable($0) }
    }

    /// Like `asResponseMap`, but a bare JSON array answer (`access.list_access_points`) stays an
    /// array of plain JSON values instead of collapsing to an empty object.
    static func asResponseValue(_ value: Any?) -> Any {
        if let sendable = value as? AnySendable {
            return asResponseValue(sendable.value)
        }
        if let list = value as? [Any] {
            let plain = list.map { unwrapSendableValue($0) }
            return FfiWireBoundary.decodeResponse(plain) as? [Any] ?? plain
        }
        return asResponseMap(value)
    }

    private static func unwrapSendableValue(_ value: Any) -> Any {
        if let sendable = value as? AnySendable {
            return unwrapSendableValue(sendable.value)
        }
        if let map = value as? [String: AnySendable] {
            return map.mapValues { unwrapSendableValue($0.value) }
        }
        if let map = value as? [String: Any] {
            return map.mapValues { unwrapSendableValue($0) }
        }
        if let list = value as? [AnySendable] {
            return list.map { unwrapSendableValue($0.value) }
        }
        if let list = value as? [Any] {
            return list.map { unwrapSendableValue($0) }
        }
        return value
    }
}

enum FfiFields {
    static func string(_ request: Any?, _ key: String) -> String {
        let map = FfiJson.asMap(request)
        if let value = map[key] as? String, !value.isEmpty { return value }
        return ""
    }

    static func int(_ request: Any?, _ key: String, default fallback: Int = 0) -> Int {
        let map = FfiJson.asMap(request)
        if let value = map[key] as? Int { return value }
        if let value = map[key] as? Int64 { return Int(value) }
        if let value = map[key] as? UInt64 { return Int(value) }
        if let value = map[key] as? String, let parsed = Int(value) { return parsed }
        return fallback
    }

    static func bool(_ request: Any?, _ key: String, default fallback: Bool = false) -> Bool {
        let map = FfiJson.asMap(request)
        if let value = map[key] as? Bool { return value }
        if let value = map[key] as? String {
            if value == "true" { return true }
            if value == "false" { return false }
        }
        return fallback
    }

    static func list(_ request: Any?, _ key: String) -> [Any] {
        let map = FfiJson.asMap(request)
        if let value = map[key] as? [Any] { return value }
        return []
    }

    static func optionalJsonString(_ request: Any?, _ key: String) -> String {
        let map = FfiJson.asMap(request)
        if let value = map[key] {
            if value is NSNull { return "null" }
            let encoded = FfiWireBoundary.encodeRequest(value) ?? value
            return FfiJson.encode(encoded)
        }
        return "{}"
    }
}

enum FfiStringCodec {
    static func decode(_ value: FlareString) -> String {
        guard let ptr = value.ptr, value.len > 0 else { return "" }
        let data = Data(bytes: ptr, count: value.len)
        var text = String(data: data, encoding: .utf8) ?? ""
        if text.hasSuffix("\0") { text.removeLast() }
        return text
    }

    static func withCString<T>(_ text: String, _ body: (UnsafePointer<CChar>?) throws -> T) rethrows -> T {
        if text.isEmpty { return try body(nil) }
        return try text.withCString { try body($0) }
    }
}

final class FfiCallbackRouter: @unchecked Sendable {
    static let shared = FfiCallbackRouter()
    private struct PendingCompletion {
        let result: Any?
        let error: Error?
    }

    private struct PendingWait {
        let continuation: CheckedContinuation<Any?, Error>
        var startCompleted = false
        var bufferedCompletion: PendingCompletion?
    }

    private var pending: [UInt64: PendingWait] = [:]
    private var eventSinks: [UInt64: (Int, Any?) -> Void] = [:]
    private var nextContextId: UInt64 = 1
    private let lock = NSLock()

    func reserveContextId() -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        let id = nextContextId
        nextContextId &+= 1
        return id
    }

    func registerEventSink(_ id: UInt64, handler: @escaping (Int, Any?) -> Void) {
        lock.lock()
        eventSinks[id] = handler
        lock.unlock()
    }

    func removeEventSink(_ id: UInt64) {
        lock.lock()
        eventSinks.removeValue(forKey: id)
        lock.unlock()
    }

    func wait(for id: UInt64, operation: String, start: (UnsafeMutableRawPointer) -> Int32) async throws -> Any? {
        try await withCheckedThrowingContinuation { continuation in
            lock.lock()
            pending[id] = PendingWait(continuation: continuation)
            lock.unlock()
            let code = start(UnsafeMutableRawPointer(bitPattern: UInt(id))!)
            var completion: PendingCompletion?
            var startError: FlareSdkException?
            if code != 0 {
                startError = FlareSdkException(
                    code: "native_error_\(code)",
                    message: "Native call returned error code \(code) before callback.",
                    operation: operation
                )
            }
            lock.lock()
            guard var wait = pending[id] else {
                lock.unlock()
                return
            }
            wait.startCompleted = true
            if let startError {
                pending.removeValue(forKey: id)
                lock.unlock()
                wait.continuation.resume(throwing: startError)
                return
            }
            if let buffered = wait.bufferedCompletion {
                pending.removeValue(forKey: id)
                completion = buffered
            } else {
                pending[id] = wait
            }
            lock.unlock()
            if let completion {
                Self.resume(wait.continuation, with: completion)
            }
        }
    }

    func complete(id: UInt64, result: Any?, error: Error?) {
        lock.lock()
        guard var wait = pending[id] else {
            lock.unlock()
            return
        }
        let completion = PendingCompletion(result: result, error: error)
        guard wait.startCompleted else {
            if wait.bufferedCompletion == nil {
                wait.bufferedCompletion = completion
                pending[id] = wait
            }
            lock.unlock()
            return
        }
        pending.removeValue(forKey: id)
        lock.unlock()
        Self.resume(wait.continuation, with: completion)
    }

    private static func resume(_ continuation: CheckedContinuation<Any?, Error>, with completion: PendingCompletion) {
        if let error = completion.error {
            continuation.resume(throwing: error)
        } else {
            continuation.resume(returning: completion.result)
        }
    }

    fileprivate func emitEvent(id: UInt64, eventType: Int32, payload: Any?) {
        lock.lock()
        let handler = eventSinks[id]
        lock.unlock()
        handler?(Int(eventType), payload)
    }

    fileprivate func emitEventBatch(id: UInt64, payload: Any?) {
        guard let map = payload as? [String: Any], let events = map["events"] as? [Any] else {
            return
        }
        for item in events {
            guard
                let event = item as? [String: Any],
                let eventType = FfiNativeCallbacks.eventType(from: event["eventType"])
            else {
                continue
            }
            emitEvent(id: id, eventType: eventType, payload: event["payload"])
        }
    }
}

/// Routes the core's connect token requests (`FlareTokenRefreshCallback`, called on an SDK thread)
/// to the refresher installed for that handle and hands the answer back through `complete`
/// (`flare_sdk_complete_connect_token_refresh`) within the core's 20 second wait.
final class FfiTokenRefreshRouter: @unchecked Sendable {
    static let shared = FfiTokenRefreshRouter()

    private struct Entry {
        let refresher: ConnectTokenRefresher
        let complete: @Sendable (UInt64, String?) -> Void
    }

    private let timeoutNanos: UInt64
    private let lock = NSLock()
    private var entries: [FlareHandle: Entry] = [:]

    init(timeoutNanos: UInt64 = 20_000_000_000) {
        self.timeoutNanos = timeoutNanos
    }

    func install(
        handle: FlareHandle,
        refresher: @escaping ConnectTokenRefresher,
        complete: @escaping @Sendable (_ requestId: UInt64, _ token: String?) -> Void
    ) {
        lock.lock()
        entries[handle] = Entry(refresher: refresher, complete: complete)
        lock.unlock()
    }

    func remove(handle: FlareHandle) {
        lock.lock()
        entries.removeValue(forKey: handle)
        lock.unlock()
    }

    func removeAll() {
        lock.lock()
        entries.removeAll()
        lock.unlock()
    }

    /// Returns at once: the refresher runs on a task, off the SDK thread that asked.
    func request(handle: FlareHandle, requestId: UInt64) {
        lock.lock()
        let entry = entries[handle]
        lock.unlock()
        // No refresher any more: the core's request was dropped with it.
        guard let entry else { return }
        let answer = FfiOnce()
        let work = Task.detached {
            let token = await entry.refresher()
            if answer.claim() {
                entry.complete(requestId, token.flatMap { $0.trimmingCharacters(in: .whitespaces).isEmpty ? nil : $0 })
            }
        }
        let timeoutNanos = self.timeoutNanos
        Task.detached {
            try? await Task.sleep(nanoseconds: timeoutNanos)
            if answer.claim() {
                work.cancel()
                entry.complete(requestId, nil)
            }
        }
    }
}

/// The first `claim()` wins.
final class FfiOnce: @unchecked Sendable {
    private let lock = NSLock()
    private var claimed = false

    func claim() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if claimed { return false }
        claimed = true
        return true
    }
}

enum FfiNativeCallbacks {
    /// The core needs a new connect token; the context is the client handle.
    static let tokenRefresh: TokenRefreshCallbackFn = { context, requestId in
        guard let context else { return }
        FfiTokenRefreshRouter.shared.request(handle: FlareHandle(UInt(bitPattern: context)), requestId: requestId)
    }

    static let result: FlareResultCallback = { context, error, result in
        guard let context else { return }
        let id = UInt64(UInt(bitPattern: context))
        guard let bindings = FlareNativeBindings.shared else { return }
        if let error {
            let message = FfiStringCodec.decode(error.pointee.message)
            let detailsJson = FfiStringCodec.decode(error.pointee.details_json)
            let code = error.pointee.code
            bindings.errorHeapFree(error)
            FfiCallbackRouter.shared.complete(
                id: id,
                result: nil,
                error: FlareSdkException.fromNative(
                    nativeCode: code,
                    message: message.isEmpty ? "Native operation failed." : message,
                    operation: nil,
                    detailsJson: detailsJson
                )
            )
            return
        }
        let raw = FfiStringCodec.decode(result)
        bindings.stringFree(result)
        FfiCallbackRouter.shared.complete(id: id, result: FfiWireBoundary.decodeResponse(FfiJson.decode(raw)), error: nil)
    }

    static let event: FlareEventCallback = { context, eventType, eventJson in
        guard let context else { return }
        let id = UInt64(UInt(bitPattern: context))
        guard let bindings = FlareNativeBindings.shared else { return }
        let raw = FfiStringCodec.decode(eventJson)
        bindings.stringFree(eventJson)
        FfiCallbackRouter.shared.emitEvent(id: id, eventType: eventType, payload: FfiWireBoundary.decodeResponse(FfiJson.decode(raw)))
    }

    static let eventBatch: FlareEventBatchCallback = { context, _, eventsJson in
        guard let context else { return }
        let id = UInt64(UInt(bitPattern: context))
        guard let bindings = FlareNativeBindings.shared else { return }
        let raw = FfiStringCodec.decode(eventsJson)
        bindings.stringFree(eventsJson)
        FfiCallbackRouter.shared.emitEventBatch(id: id, payload: FfiWireBoundary.decodeResponse(FfiJson.decode(raw)))
    }

    fileprivate static func eventType(from value: Any?) -> Int32? {
        if let value = value as? Int32 { return value }
        if let value = value as? Int { return Int32(value) }
        if let value = value as? NSNumber { return value.int32Value }
        if let value = value as? String, let parsed = Int32(value) { return parsed }
        return nil
    }
}

// RUST-OWNED WIRE BOUNDARY: BEGIN
enum FfiWireBoundary {
    static func encodeRequest(_ value: Any?) -> Any? {
        return value
    }

    static func decodeResponse(_ value: Any?) -> Any? {
        return value
    }
}
// RUST-OWNED WIRE BOUNDARY: END
