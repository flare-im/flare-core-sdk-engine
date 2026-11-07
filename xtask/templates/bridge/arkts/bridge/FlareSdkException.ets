// GENERATED. Do not edit by hand.

/**
 * The structured part of an SDK failure, as the C ABI hands it over in `details_json`
 * (`bindings/contract/errors.json`). Hosts read these fields; `message` is for people.
 */
export interface FlareSdkErrorDetails {
  /** Localized key or reason, e.g. `sdk.plugin.disabled`. */
  reason?: string;
  /** Whether Core judges the failure worth retrying. */
  retryable?: boolean;
  /** The HTTP status Core answered (100..=599). */
  httpStatus?: number;
  /** Core's numeric error code. */
  remoteCode?: number;
  /** Core's or the plugin's code, e.g. `PLUGIN_DISABLED`, `CALL_FULL`. */
  machineReason?: string;
}

/** Stable ArkTS error surfaced by the HarmonyOS SDK bridge/adapter. */
export class FlareSdkException extends Error {
  /** The core `ErrorCode` wire name of an SDK failure (`PERMISSION_DENIED`), else the bridge's own code. */
  readonly code: string;
  readonly operation?: string;
  readonly details?: Record<string, string>;
  readonly reason?: string;
  readonly retryable: boolean;
  readonly httpStatus?: number;
  readonly remoteCode?: number;
  readonly machineReason?: string;

  constructor(
    code: string,
    message: string,
    operation?: string,
    details?: Record<string, string>,
    sdk?: FlareSdkErrorDetails,
  ) {
    super(message);
    this.name = 'FlareSdkException';
    this.code = code;
    this.operation = operation;
    this.details = details;
    this.reason = sdk?.reason;
    this.retryable = sdk?.retryable === true;
    this.httpStatus = sdk?.httpStatus;
    this.remoteCode = sdk?.remoteCode;
    this.machineReason = sdk?.machineReason;
  }
}

/**
 * The server is shedding load (`OVERLOADED`, C `FLARE_ERR_OVERLOADED` = 12). Retry on the same
 * access point after `retryAfterMs` when given; it is never an access point failure.
 */
export class OverloadedError extends FlareSdkException {
  /** How long the server asked to wait, when it said. */
  readonly retryAfterMs?: number;

  constructor(message: string, operation?: string, details?: Record<string, string>, sdk?: FlareSdkErrorDetails) {
    super('OVERLOADED', message, operation, details, sdk);
    this.name = 'OverloadedError';
    const wait = details !== undefined && details['retryAfterMs'] !== undefined ? Number(details['retryAfterMs']) : NaN;
    this.retryAfterMs = Number.isFinite(wait) && wait >= 0 ? wait : undefined;
  }
}

/**
 * The app build is below the server minimum (`UPGRADE_REQUIRED`, C `FLARE_ERR_UPGRADE_REQUIRED` = 13).
 * The session stays stopped (`actionRequired` / `upgradeRequired`) until the user upgrades.
 */
export class UpgradeRequiredError extends FlareSdkException {
  constructor(message: string, operation?: string, details?: Record<string, string>, sdk?: FlareSdkErrorDetails) {
    super('UPGRADE_REQUIRED', message, operation, details, sdk);
    this.name = 'UpgradeRequiredError';
  }
}

/**
 * A non-idempotent request may or may not have reached the server (`NETWORK_UNCERTAIN`,
 * C `FLARE_ERR_NETWORK_UNCERTAIN` = 14). Reconcile with the same Idempotency-Key before retrying.
 */
export class NetworkUncertainError extends FlareSdkException {
  constructor(message: string, operation?: string, details?: Record<string, string>, sdk?: FlareSdkErrorDetails) {
    super('NETWORK_UNCERTAIN', message, operation, details, sdk);
    this.name = 'NetworkUncertainError';
  }
}

/** The exception for `code`: one of the typed errors above when the code is theirs. */
export function createSdkException(
  code: string,
  message: string,
  operation?: string,
  details?: Record<string, string>,
  sdk?: FlareSdkErrorDetails,
): FlareSdkException {
  switch (code) {
    case 'OVERLOADED':
    case 'overloaded':
      return new OverloadedError(message, operation, details, sdk);
    case 'UPGRADE_REQUIRED':
    case 'upgradeRequired':
      return new UpgradeRequiredError(message, operation, details, sdk);
    case 'NETWORK_UNCERTAIN':
    case 'networkUncertain':
      return new NetworkUncertainError(message, operation, details, sdk);
    default:
      return new FlareSdkException(code, message, operation, details, sdk);
  }
}

/** C ABI result codes (`FLARE_ERR_*`, `bindings/contract/errors.json` `cAbi.codes`). */
const NATIVE_ERROR_NAMES: Record<number, string> = {
  1: 'FLARE_ERR_INVALID_HANDLE',
  2: 'FLARE_ERR_INVALID_PARAM',
  3: 'FLARE_ERR_NOT_CONNECTED',
  4: 'FLARE_ERR_OPERATION_TIMEOUT',
  5: 'FLARE_ERR_NETWORK_ERROR',
  6: 'FLARE_ERR_AUTH_FAILED',
  7: 'FLARE_ERR_TOKEN_EXPIRED',
  8: 'FLARE_ERR_INTERNAL',
  9: 'FLARE_ERR_JSON_PARSE',
  10: 'FLARE_ERR_NULL_POINTER',
  11: 'FLARE_ERR_FFI_PANIC',
  12: 'FLARE_ERR_OVERLOADED',
  13: 'FLARE_ERR_UPGRADE_REQUIRED',
  14: 'FLARE_ERR_NETWORK_UNCERTAIN',
};

/** The three C codes that always become their typed error, whatever `details_json` says. */
const NATIVE_TYPED_CODES: Record<number, string> = {
  12: 'OVERLOADED',
  13: 'UPGRADE_REQUIRED',
  14: 'NETWORK_UNCERTAIN',
};

/**
 * The exception for a failed C ABI call: its `FlareError.code`, `message` and `details_json`
 * (`{code, reason, retryable, httpStatus?, remoteCode?, machineReason?, retryAfterMs?}`).
 * `code` is the core `ErrorCode` wire name from the details when they carry one, else the
 * C code's name; 12, 13 and 14 always map to their typed error.
 */
export function sdkExceptionFromNative(
  nativeCode: number,
  message: string,
  detailsJson: string,
  operation?: string,
): FlareSdkException {
  const parsed = parseDetails(detailsJson);
  const typed = NATIVE_TYPED_CODES[nativeCode];
  const coreCode = typeof parsed['code'] === 'string' ? parsed['code'] as string : undefined;
  const code = typed ?? coreCode ?? NATIVE_ERROR_NAMES[nativeCode] ?? `FLARE_ERR_${nativeCode}`;
  const details: Record<string, string> = { nativeCode: `${nativeCode}` };
  const retryAfterMs = parsed['retryAfterMs'];
  if (typeof retryAfterMs === 'number') {
    details['retryAfterMs'] = `${retryAfterMs}`;
  }
  const sdk: FlareSdkErrorDetails = {};
  if (typeof parsed['reason'] === 'string') {
    sdk.reason = parsed['reason'] as string;
  }
  if (typeof parsed['retryable'] === 'boolean') {
    sdk.retryable = parsed['retryable'] as boolean;
  }
  if (typeof parsed['httpStatus'] === 'number') {
    sdk.httpStatus = parsed['httpStatus'] as number;
  }
  if (typeof parsed['remoteCode'] === 'number') {
    sdk.remoteCode = parsed['remoteCode'] as number;
  }
  if (typeof parsed['machineReason'] === 'string') {
    sdk.machineReason = parsed['machineReason'] as string;
  }
  return createSdkException(code, message.length > 0 ? message : code, operation, details, sdk);
}

function parseDetails(detailsJson: string): Record<string, Object> {
  if (detailsJson.trim().length === 0) {
    return {};
  }
  try {
    const value = JSON.parse(detailsJson) as Object;
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
      return value as Record<string, Object>;
    }
  } catch (e) {
    // A malformed details payload still leaves the C code and the message.
  }
  return {};
}
