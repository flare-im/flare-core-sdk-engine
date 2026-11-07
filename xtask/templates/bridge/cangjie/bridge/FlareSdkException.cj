package flare_core_harmony_cangjie_sdk.bridge

// GENERATED. Do not edit by hand.

// A failed SDK call. `code` is the core `ErrorCode` wire name (`PERMISSION_DENIED`) when the core
// gave one, else the C ABI code's name (`FLARE_ERR_INVALID_HANDLE`) or the bridge's own code.
public open class FlareSdkException <: Exception {
    public let code: String
    public let operation: ?String

    public init(code: String, message: String, operation!: ?String = None) {
        super(message)
        this.code = code
        this.operation = operation
    }
}

// The server is shedding load (`OVERLOADED`, C `FLARE_ERR_OVERLOADED` = 12). Retry on the same
// access point after `retryAfterMs` when given; it is never an access point failure.
public class OverloadedError <: FlareSdkException {
    public let retryAfterMs: ?Int64

    public init(message: String, operation!: ?String = None, retryAfterMs!: ?Int64 = None) {
        super("OVERLOADED", message, operation: operation)
        this.retryAfterMs = retryAfterMs
    }
}

// The app build is below the server minimum (`UPGRADE_REQUIRED`, C `FLARE_ERR_UPGRADE_REQUIRED` = 13).
// The session stays stopped (`actionRequired` / `upgradeRequired`) until the user upgrades.
public class UpgradeRequiredError <: FlareSdkException {
    public init(message: String, operation!: ?String = None) {
        super("UPGRADE_REQUIRED", message, operation: operation)
    }
}

// A non-idempotent request may or may not have reached the server (`NETWORK_UNCERTAIN`,
// C `FLARE_ERR_NETWORK_UNCERTAIN` = 14). Reconcile with the same Idempotency-Key before retrying.
public class NetworkUncertainError <: FlareSdkException {
    public init(message: String, operation!: ?String = None) {
        super("NETWORK_UNCERTAIN", message, operation: operation)
    }
}

// The exception for `code`: one of the typed errors above when the code is theirs (core wire name
// or client code), else a plain FlareSdkException.
public func createSdkException(code: String, message: String, operation!: ?String = None,
    retryAfterMs!: ?Int64 = None): FlareSdkException {
    if (code == "OVERLOADED" || code == "overloaded") {
        return OverloadedError(message, operation: operation, retryAfterMs: retryAfterMs)
    }
    if (code == "UPGRADE_REQUIRED" || code == "upgradeRequired") {
        return UpgradeRequiredError(message, operation: operation)
    }
    if (code == "NETWORK_UNCERTAIN" || code == "networkUncertain") {
        return NetworkUncertainError(message, operation: operation)
    }
    return FlareSdkException(code, message, operation: operation)
}

// The exception for a failed C ABI call: its `FlareError.code`, message and the core code from
// `details_json` when there is one. 12, 13 and 14 always map to their typed error.
public func sdkExceptionFromNative(nativeCode: Int32, message: String, operation!: ?String = None,
    coreCode!: ?String = None, retryAfterMs!: ?Int64 = None): FlareSdkException {
    let code = match (nativeCode) {
        case 12 => "OVERLOADED"
        case 13 => "UPGRADE_REQUIRED"
        case 14 => "NETWORK_UNCERTAIN"
        case _ => match (coreCode) {
            case Some(value) => value
            case None => nativeErrorName(nativeCode)
        }
    }
    let text = if (message.isEmpty()) { code } else { message }
    return createSdkException(code, text, operation: operation, retryAfterMs: retryAfterMs)
}

// C ABI result codes (`FLARE_ERR_*`, `bindings/contract/errors.json` `cAbi.codes`).
public func nativeErrorName(nativeCode: Int32): String {
    return match (nativeCode) {
        case 1 => "FLARE_ERR_INVALID_HANDLE"
        case 2 => "FLARE_ERR_INVALID_PARAM"
        case 3 => "FLARE_ERR_NOT_CONNECTED"
        case 4 => "FLARE_ERR_OPERATION_TIMEOUT"
        case 5 => "FLARE_ERR_NETWORK_ERROR"
        case 6 => "FLARE_ERR_AUTH_FAILED"
        case 7 => "FLARE_ERR_TOKEN_EXPIRED"
        case 8 => "FLARE_ERR_INTERNAL"
        case 9 => "FLARE_ERR_JSON_PARSE"
        case 10 => "FLARE_ERR_NULL_POINTER"
        case 11 => "FLARE_ERR_FFI_PANIC"
        case 12 => "FLARE_ERR_OVERLOADED"
        case 13 => "FLARE_ERR_UPGRADE_REQUIRED"
        case 14 => "FLARE_ERR_NETWORK_UNCERTAIN"
        case _ => "FLARE_ERR_${nativeCode}"
    }
}
