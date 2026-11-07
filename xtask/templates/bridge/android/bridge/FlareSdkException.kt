package com.flare.im.bridge

import org.json.JSONObject

/**
 * Stable Android error surfaced by the JNI/FFI bridge/adapter.
 *
 * An SDK failure carries the binding's structured error (`bindings/contract/errors.json`):
 * [code] is the core error code (`PERMISSION_DENIED`), [reason] the localized key
 * (`sdk.plugin.disabled`), [machineReason] Core's or the plugin's code (`PLUGIN_DISABLED`,
 * `CALL_FULL`), [httpStatus] and [remoteCode] Core's answer, [refusedMessageIds] the messages
 * the operation refused. Read these, never [message]. The failures a host handles by kind are
 * subclasses: [OverloadedException], [UpgradeRequiredException], [NetworkUncertainException].
 */
open class FlareSdkException(
    val code: String,
    override val message: String,
    val operation: String? = null,
    val details: Map<String, String> = emptyMap(),
    val reason: String? = null,
    val retryable: Boolean = false,
    val httpStatus: Int? = null,
    val remoteCode: Int? = null,
    val machineReason: String? = null,
    val refusedMessageIds: List<String>? = null,
) : RuntimeException(message) {
    companion object {
        /**
         * The exception for [code]: one of the typed subclasses when the code is theirs (core
         * name or contract name), else a plain [FlareSdkException].
         */
        fun create(
            code: String,
            message: String,
            operation: String? = null,
            details: Map<String, String> = emptyMap(),
            reason: String? = null,
            retryable: Boolean = false,
            httpStatus: Int? = null,
            remoteCode: Int? = null,
            machineReason: String? = null,
            refusedMessageIds: List<String>? = null,
            retryAfterMs: Long? = null,
        ): FlareSdkException = when (code) {
            "OVERLOADED", "overloaded" -> OverloadedException(
                message, operation, details, reason, retryable, httpStatus, remoteCode, machineReason, retryAfterMs,
            )
            "UPGRADE_REQUIRED", "upgradeRequired" -> UpgradeRequiredException(
                message, operation, details, reason, retryable, httpStatus, remoteCode, machineReason,
            )
            "NETWORK_UNCERTAIN", "networkUncertain" -> NetworkUncertainException(
                message, operation, details, reason, retryable, httpStatus, remoteCode, machineReason,
            )
            else -> FlareSdkException(
                code, message, operation, details, reason, retryable, httpStatus, remoteCode, machineReason,
                refusedMessageIds,
            )
        }

        /** The core code name of the typed C ABI codes (`FLARE_ERR_OVERLOADED` and so on). */
        private fun typedCode(nativeCode: Int): String? = when (nativeCode) {
            12 -> "OVERLOADED"
            13 -> "UPGRADE_REQUIRED"
            14 -> "NETWORK_UNCERTAIN"
            else -> null
        }

        /**
         * The error of a failed native call: the structured error the SDK wrote in
         * `details_json`, else the C ABI code alone (the core code name for the typed ones,
         * `native.<code>` otherwise).
         */
        fun fromNative(
            nativeCode: Int,
            message: String,
            operation: String?,
            detailsJson: String?,
            details: Map<String, String> = emptyMap(),
        ): FlareSdkException {
            val structured = detailsJson
                ?.takeIf { it.isNotBlank() }
                ?.let { runCatching { JSONObject(it) }.getOrNull() }
            fun string(name: String): String? =
                structured?.takeIf { it.has(name) }?.optString(name)?.takeIf { it.isNotEmpty() }
            fun int(name: String): Int? =
                structured?.takeIf { it.has(name) }?.opt(name)?.let { it as? Number }?.toInt()
            fun strings(name: String): List<String>? {
                val array = structured?.optJSONArray(name)?.takeIf { it.length() > 0 } ?: return null
                return (0 until array.length()).map { array.opt(it) as? String ?: return null }
            }
            return create(
                code = string("code") ?: typedCode(nativeCode) ?: "native.$nativeCode",
                message = message,
                operation = operation,
                details = details,
                reason = string("reason"),
                retryable = structured?.optBoolean("retryable", false) ?: false,
                httpStatus = int("httpStatus"),
                remoteCode = int("remoteCode"),
                machineReason = string("machineReason"),
                refusedMessageIds = strings("refusedMessageIds"),
                retryAfterMs = structured?.takeIf { it.has("retryAfterMs") }?.opt("retryAfterMs")
                    ?.let { it as? Number }?.toLong(),
            )
        }
    }
}

/**
 * The server is shedding load (C `FLARE_ERR_OVERLOADED`): retry on the same access point after
 * [retryAfterMs] when given. It is never an access point failure.
 */
class OverloadedException(
    message: String,
    operation: String? = null,
    details: Map<String, String> = emptyMap(),
    reason: String? = null,
    retryable: Boolean = false,
    httpStatus: Int? = null,
    remoteCode: Int? = null,
    machineReason: String? = null,
    /** How long the server asked to wait, when it said. */
    val retryAfterMs: Long? = null,
) : FlareSdkException("OVERLOADED", message, operation, details, reason, retryable, httpStatus, remoteCode, machineReason)

/**
 * The app build is below the server minimum (C `FLARE_ERR_UPGRADE_REQUIRED`); the session stays
 * stopped (`actionRequired` / `upgradeRequired`) until the user upgrades.
 */
class UpgradeRequiredException(
    message: String,
    operation: String? = null,
    details: Map<String, String> = emptyMap(),
    reason: String? = null,
    retryable: Boolean = false,
    httpStatus: Int? = null,
    remoteCode: Int? = null,
    machineReason: String? = null,
) : FlareSdkException("UPGRADE_REQUIRED", message, operation, details, reason, retryable, httpStatus, remoteCode, machineReason)

/**
 * A non-idempotent request may or may not have reached the server (C `FLARE_ERR_NETWORK_UNCERTAIN`):
 * reconcile with the same Idempotency-Key before retrying.
 */
class NetworkUncertainException(
    message: String,
    operation: String? = null,
    details: Map<String, String> = emptyMap(),
    reason: String? = null,
    retryable: Boolean = false,
    httpStatus: Int? = null,
    remoteCode: Int? = null,
    machineReason: String? = null,
) : FlareSdkException("NETWORK_UNCERTAIN", message, operation, details, reason, retryable, httpStatus, remoteCode, machineReason)
