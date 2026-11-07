import Foundation

/// Stable Swift error surfaced by the Apple SDK bridge/adapter.
///
/// An SDK failure carries the binding's structured error (`bindings/contract/errors.json`):
/// `code` is the core error code (`PERMISSION_DENIED`), `reason` the localized key
/// (`sdk.plugin.disabled`), `machineReason` Core's or the plugin's code (`PLUGIN_DISABLED`,
/// `CALL_FULL`), `httpStatus` and `remoteCode` Core's answer, `refusedMessageIds` the messages the
/// operation refused. Read these, never `message`. The failures a host handles by kind
/// (overloaded, upgrade required, network uncertain) also carry `kind`.
public struct FlareSdkException: Error, LocalizedError, Sendable {
    public let code: String
    public let message: String
    public let operation: String?
    public let details: [String: String]
    public let reason: String?
    public let retryable: Bool
    public let httpStatus: Int?
    public let remoteCode: Int?
    public let machineReason: String?
    public let refusedMessageIds: [String]?
    /// How long an overloaded server asked to wait, when it said.
    public let retryAfterMs: Int?

    public init(
        code: String,
        message: String,
        operation: String? = nil,
        details: [String: String] = [:],
        reason: String? = nil,
        retryable: Bool = false,
        httpStatus: Int? = nil,
        remoteCode: Int? = nil,
        machineReason: String? = nil,
        refusedMessageIds: [String]? = nil,
        retryAfterMs: Int? = nil
    ) {
        self.code = code
        self.message = message
        self.operation = operation
        self.details = details
        self.reason = reason
        self.retryable = retryable
        self.httpStatus = httpStatus
        self.remoteCode = remoteCode
        self.machineReason = machineReason
        self.refusedMessageIds = refusedMessageIds
        self.retryAfterMs = retryAfterMs
    }

    /// The typed failure behind `code`, when it is one a host handles by kind.
    public var kind: FlareSdkErrorKind? {
        switch code {
        case "OVERLOADED", SdkErrorCodes.overloaded:
            return .overloaded(retryAfterMs: retryAfterMs)
        case "UPGRADE_REQUIRED", SdkErrorCodes.upgradeRequired:
            return .upgradeRequired
        case "NETWORK_UNCERTAIN", SdkErrorCodes.networkUncertain:
            return .networkUncertain
        default:
            return nil
        }
    }

    /// The error of a failed native call: the structured error the SDK wrote in `details_json`,
    /// else the C ABI code alone (the core code name for the typed ones, `native_error_<code>`
    /// otherwise).
    public static func fromNative(
        nativeCode: Int32,
        message: String,
        operation: String?,
        detailsJson: String
    ) -> FlareSdkException {
        let structured = detailsJson.data(using: .utf8)
            .flatMap { try? JSONSerialization.jsonObject(with: $0) as? [String: Any] } ?? [:]
        let code = (structured["code"] as? String).flatMap { $0.isEmpty ? nil : $0 }
        return FlareSdkException(
            code: code ?? typedCode(nativeCode) ?? "native_error_\(nativeCode)",
            message: message,
            operation: operation,
            reason: structured["reason"] as? String,
            retryable: structured["retryable"] as? Bool ?? false,
            httpStatus: (structured["httpStatus"] as? NSNumber)?.intValue,
            remoteCode: (structured["remoteCode"] as? NSNumber)?.intValue,
            machineReason: structured["machineReason"] as? String,
            refusedMessageIds: (structured["refusedMessageIds"] as? [String]).flatMap { $0.isEmpty ? nil : $0 },
            retryAfterMs: (structured["retryAfterMs"] as? NSNumber)?.intValue
        )
    }

    /// The core code name of the typed C ABI codes (`FLARE_ERR_OVERLOADED` and so on).
    private static func typedCode(_ nativeCode: Int32) -> String? {
        switch nativeCode {
        case 12: return "OVERLOADED"
        case 13: return "UPGRADE_REQUIRED"
        case 14: return "NETWORK_UNCERTAIN"
        default: return nil
        }
    }

    public var errorDescription: String? {
        if let operation {
            return "FlareSdkException(code=\(code), operation=\(operation), message=\(message))"
        }
        return "FlareSdkException(code=\(code), message=\(message))"
    }
}

/// The failures a host handles by kind (C ABI `FLARE_ERR_OVERLOADED` 12,
/// `FLARE_ERR_UPGRADE_REQUIRED` 13, `FLARE_ERR_NETWORK_UNCERTAIN` 14).
public enum FlareSdkErrorKind: Equatable, Sendable {
    /// The server is shedding load: retry on the same access point after `retryAfterMs` when
    /// given. It is never an access point failure.
    case overloaded(retryAfterMs: Int?)
    /// The app build is below the server minimum; the session stays stopped
    /// (`actionRequired` / `upgradeRequired`) until the user upgrades.
    case upgradeRequired
    /// A non-idempotent request may or may not have reached the server: reconcile with the same
    /// Idempotency-Key before retrying.
    case networkUncertain
}
