// GENERATED. Do not edit by hand.

/**
 * The structured part of an SDK failure, as every binding hands it over
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
  /** The messages the operation refused, by id (a forward whose sources cannot all be forwarded). */
  refusedMessageIds?: string[];
}

/** Stable TypeScript error surfaced by platform bridges. */
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
  readonly refusedMessageIds?: string[];

  constructor(
    code: string,
    message: string,
    operation?: string,
    details?: Record<string, string>,
    sdk: FlareSdkErrorDetails = {},
  ) {
    super(message);
    this.name = 'FlareSdkException';
    this.code = code;
    this.operation = operation;
    this.details = details;
    this.reason = sdk.reason;
    this.retryable = sdk.retryable ?? false;
    this.httpStatus = sdk.httpStatus;
    this.remoteCode = sdk.remoteCode;
    this.machineReason = sdk.machineReason;
    this.refusedMessageIds = sdk.refusedMessageIds;
  }

  /**
   * The exception for an error a binding threw or rejected with: the wasm runtime's `Error`
   * or a Tauri command's rejection object, both carrying the structured error. Null when
   * `error` carries no code.
   */
  static fromBindingError(error: unknown, operation?: string): FlareSdkException | null {
    if (error instanceof FlareSdkException) return error;
    if (!error || typeof error !== 'object') return null;
    const value = error as Record<string, unknown>;
    if (typeof value.code !== 'string' || !value.code) return null;
    const retryAfterMs = typeof value.retryAfterMs === 'number' ? value.retryAfterMs : undefined;
    return createSdkException(
      value.code,
      typeof value.message === 'string' ? value.message : value.code,
      typeof value.operation === 'string' ? value.operation : operation,
      retryAfterMs !== undefined ? { retryAfterMs: String(retryAfterMs) } : undefined,
      {
        reason: typeof value.reason === 'string' ? value.reason : undefined,
        retryable: value.retryable === true,
        httpStatus: typeof value.httpStatus === 'number' ? value.httpStatus : undefined,
        remoteCode: typeof value.remoteCode === 'number' ? value.remoteCode : undefined,
        machineReason: typeof value.machineReason === 'string' ? value.machineReason : undefined,
        refusedMessageIds: stringList(value.refusedMessageIds),
      },
    );
  }
}

/**
 * The server is shedding load (`OVERLOADED`, C `FLARE_ERR_OVERLOADED`). Retry on the same
 * access point after `retryAfterMs` when given; it is never an access point failure.
 */
export class OverloadedError extends FlareSdkException {
  /** How long the server asked to wait, when it said. */
  readonly retryAfterMs?: number;

  constructor(
    message: string,
    operation?: string,
    details?: Record<string, string>,
    sdk: FlareSdkErrorDetails = {},
  ) {
    super('OVERLOADED', message, operation, details, sdk);
    this.name = 'OverloadedError';
    const wait = Number(details?.retryAfterMs);
    this.retryAfterMs = Number.isFinite(wait) && wait >= 0 ? wait : undefined;
  }
}

/**
 * The app build is below the server minimum (`UPGRADE_REQUIRED`, C `FLARE_ERR_UPGRADE_REQUIRED`).
 * The session stays stopped (`actionRequired` / `upgradeRequired`) until the user upgrades.
 */
export class UpgradeRequiredError extends FlareSdkException {
  constructor(
    message: string,
    operation?: string,
    details?: Record<string, string>,
    sdk: FlareSdkErrorDetails = {},
  ) {
    super('UPGRADE_REQUIRED', message, operation, details, sdk);
    this.name = 'UpgradeRequiredError';
  }
}

/**
 * A non-idempotent request may or may not have reached the server (`NETWORK_UNCERTAIN`,
 * C `FLARE_ERR_NETWORK_UNCERTAIN`). Reconcile with the same Idempotency-Key before retrying.
 */
export class NetworkUncertainError extends FlareSdkException {
  constructor(
    message: string,
    operation?: string,
    details?: Record<string, string>,
    sdk: FlareSdkErrorDetails = {},
  ) {
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
  sdk: FlareSdkErrorDetails = {},
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

/** `value` when it is a non-empty list of strings, else undefined. */
function stringList(value: unknown): string[] | undefined {
  return Array.isArray(value) && value.length > 0 && value.every((item) => typeof item === 'string')
    ? [...value]
    : undefined;
}
