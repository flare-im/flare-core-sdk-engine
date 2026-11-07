package com.flare.im.bridge

import com.flare.im.adapter.codec.wireDecodeResponse
import com.flare.im.adapter.codec.wireEncodeRequest
import com.flare.im.contract.ConnectTokenRefresher
import com.flare.im.contract.NativeBridge
import com.flare.im.contract.NativeCallDescriptor
import com.flare.im.contract.NativeCallMap
import com.flare.im.contract.SdkOperations
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

internal typealias NativeEventSink = (Int, Map<String, Any?>) -> Unit

/**
 * Routes the core's connect token requests (`FlareTokenRefreshCallback`, called on an SDK thread
 * through the JNI shim) to the refresher installed for that handle, and hands the answer back
 * through `complete` (`flare_sdk_complete_connect_token_refresh`) within the core's 20 second wait.
 */
internal class ConnectTokenRefreshRouter(
    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
    private val timeoutMillis: Long = 20_000,
) {
    private class Entry(
        val refresher: ConnectTokenRefresher,
        val complete: (requestId: Long, token: String?) -> Unit,
    )

    private val entries = ConcurrentHashMap<Long, Entry>()

    fun install(handle: Long, refresher: ConnectTokenRefresher, complete: (requestId: Long, token: String?) -> Unit) {
        entries[handle] = Entry(refresher, complete)
    }

    fun remove(handle: Long) {
        entries.remove(handle)
    }

    fun removeAll() {
        entries.clear()
    }

    /** Returns at once: the refresher runs on a coroutine, off the SDK thread that asked. */
    fun request(handle: Long, requestId: Long) {
        // No refresher any more: the core's request was dropped with it.
        val entry = entries[handle] ?: return
        scope.launch {
            val token = try {
                withTimeoutOrNull(timeoutMillis) { entry.refresher() }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Throwable) {
                null
            }
            entry.complete(requestId, token?.takeIf { it.isNotBlank() })
        }
    }
}

/**
 * Thin Kotlin bridge over the Flare Core C ABI via JNI.
 *
 * This bridge owns no IM behavior. It marshals platform values into JSON and
 * delegates to `libflare_im_core_sdk_ffi` through the JNI shim in `src/main/cpp`.
 */
class JniNativeBridge(
    private val libraryName: String = "flare_im_core_android_sdk_jni",
    /** `sdk.init` 时补进配置的应用版本信息（宿主在请求里显式给的值优先），见 [FlareAppInfo]。 */
    private val appInfo: FlareAppInfo? = null,
) : NativeBridge {
    private var handle: Long = 0
    private var released: Boolean = false
    private var eventSink: NativeEventSink? = null
    private var contractVersionChecked: Boolean = false

    init {
        System.loadLibrary(libraryName)
    }

    override suspend fun <T> invoke(descriptor: NativeCallDescriptor, request: Any?): T {
        if (descriptor.operation != SdkOperations.DIAGNOSTICS_FFI_CONTRACT_VERSION) {
            ensureContractVersion()
        }
        val value: Any? = when (descriptor.operation) {
            "sdk.create" -> createHandle()
            "sdk.dispose" -> {
                releaseHandle()
                null
            }
            "sdk.hard_reset" -> {
                if (handle != 0L) {
                    eventSinks.remove(handle)
                }
                tokenRefreshRouter.removeAll()
                nativeHardReset()
                handle = 0
                released = true
                null
            }
            "sdk.is_connected" -> nativeIsConnected(requireHandle())
            "sdk.session_active" -> nativeSessionActive(requireHandle())
            "diagnostics.sdk_version" -> mapOf("version" to nativeSdkVersion())
            "diagnostics.ffi_contract_version" -> mapOf("version" to nativeFfiContractVersion())
            "event.subscribe" -> mapOf("subscription" to nativeSubscribeEvents(requireHandle()))
            "event.subscribe_batch" -> mapOf("subscription" to nativeSubscribeEventsBatch(requireHandle()))
            "event.unsubscribe" -> {
                nativeUnsubscribeEvents(longField(request, "subscription"))
                null
            }
            "event.unsubscribe_all" -> {
                nativeUnsubscribeAllEvents()
                null
            }
            else -> invokeAsync(descriptor, request)
        }
        @Suppress("UNCHECKED_CAST")
        return value as T
    }

    /**
     * Installs (or, with null, removes) the host's connect token refresher. The core calls back on
     * an SDK thread; the refresher runs on a coroutine (IO) and its token goes back to the core.
     * Throwing, an empty token and the 20 second deadline all answer "none".
     */
    override suspend fun setConnectTokenRefresher(refresher: ConnectTokenRefresher?) {
        ensureContractVersion()
        val handle = requireHandle()
        if (refresher != null) {
            tokenRefreshRouter.install(handle, refresher) { requestId, token ->
                // FLARE_ERR_INVALID_PARAM: the core stopped waiting; nothing to retry.
                nativeCompleteConnectTokenRefresh(handle, requestId, token)
            }
        } else {
            tokenRefreshRouter.remove(handle)
        }
        try {
            submitAsync(NativeCallMap.SDK_SET_CONNECT_TOKEN_REFRESHER) { contextId ->
                nativeSetConnectTokenRefresher(handle, refresher != null, contextId)
            }
        } catch (error: Throwable) {
            if (refresher != null) tokenRefreshRouter.remove(handle)
            throw error
        }
    }

    /** Submits a callback-style C call that is not routed by descriptor; `start` returns the submit code. */
    private suspend fun submitAsync(descriptor: NativeCallDescriptor, start: (contextId: Long) -> Int): Any? =
        suspendCoroutine { continuation ->
            val contextId = nextContextId.getAndIncrement()
            pending[contextId] = PendingCall(descriptor, continuation)
            val submitCode = start(contextId)
            if (submitCode != 0) {
                pending.remove(contextId)
                continuation.resumeWithException(
                    FlareSdkException(
                        code = "native_submit_failed",
                        message = "Native C ABI submit failed for ${descriptor.operation} (submitCode=$submitCode).",
                        operation = descriptor.operation,
                        details = mapOf("submitCode" to submitCode.toString(), "cApi" to descriptor.cApi),
                    ),
                )
            }
        }

    private fun ensureContractVersion() {
        if (contractVersionChecked) {
            return
        }
        FfiContractVersionGuard.validate(nativeFfiContractVersion())
        contractVersionChecked = true
    }

    private fun createHandle(): Map<String, Any?> {
        if (handle == 0L || released) {
            handle = nativeCreate()
            released = false
            eventSink?.let { eventSinks[handle] = it }
        }
        if (handle == 0L) {
            throw FlareSdkException(
                code = "native_create_failed",
                message = "flare_sdk_create returned an invalid handle.",
                operation = "sdk.create",
            )
        }
        return mapOf("handle" to handle)
    }

    private fun releaseHandle() {
        if (handle != 0L && !released) {
            eventSinks.remove(handle)
            tokenRefreshRouter.remove(handle)
            nativeRelease(handle)
        }
        handle = 0
        released = true
    }

    internal fun registerEventSink(sink: NativeEventSink) {
        eventSink = sink
        if (handle != 0L && !released) {
            eventSinks[handle] = sink
        }
    }

    internal fun unregisterEventSink() {
        if (handle != 0L) {
            eventSinks.remove(handle)
        }
        eventSink = null
    }

    private fun requireHandle(): Long {
        if (handle == 0L || released) {
            createHandle()
        }
        return handle
    }

    private suspend fun invokeAsync(descriptor: NativeCallDescriptor, request: Any?): Any? =
        suspendCoroutine { continuation ->
            val contextId = nextContextId.getAndIncrement()
            pending[contextId] = PendingCall(descriptor, continuation)
            val submitCode = when {
                descriptor.transport == "message-dispatch-json" && descriptor.cApi == "flare_message_dispatch_json" -> {
                    nativeMessageDispatchJson(requireHandle(), descriptor.dispatchOp ?: "", encodeJson(request), contextId)
                }
                descriptor.transport == "capability-dispatch-json" && descriptor.cApi == "flare_capability_dispatch_json" -> {
                    nativeCapabilityDispatchJson(requireHandle(), descriptor.dispatchOp ?: "", encodeJson(request), contextId)
                }
                descriptor.transport == "contract-invoke-json" && descriptor.cApi == "flare_sdk_invoke_json" -> {
                    nativeSdkInvokeJson(requireHandle(), descriptor.operation, encodeJson(request), contextId)
                }
                // `media.*` 的 JSON dispatch（缓存、签名地址、下载位置、保存到本机……）：JNI 层没有单独接
                // flare_media_dispatch_json，走同一个 C 入口 flare_sdk_invoke_json —— 核心按 `media.` 前缀
                // 路由到同一份媒体 dispatch。此前这里落到 else 返回 -1，Android 上所有媒体 dispatch 调用都失败。
                descriptor.transport == "media-dispatch-json" && descriptor.cApi == "flare_media_dispatch_json" -> {
                    nativeSdkInvokeJson(requireHandle(), descriptor.operation, encodeJson(request), contextId)
                }
                descriptor.transport == "dispatch-json" && descriptor.cApi == "flare_message_build_json" -> {
                    nativeMessageBuildJson(requireHandle(), encodeJson(request), contextId)
                }
                descriptor.transport == "dispatch-json" && descriptor.cApi == "flare_message_dispatch_json" -> {
                    val map = request as? Map<*, *> ?: emptyMap<String, Any?>()
                    val op = map["op"]?.toString() ?: descriptor.dispatchOp ?: ""
                    nativeMessageDispatchJson(requireHandle(), op, encodeJson(request), contextId)
                }
                descriptor.cApi == "flare_sdk_init" -> nativeSdkInit(
                    requireHandle(),
                    encodeJson(appInfo?.fill(request) ?: request),
                    contextId,
                )
                descriptor.cApi == "flare_sdk_uninit" -> nativeSdkUninit(requireHandle(), contextId)
                descriptor.cApi == "flare_sdk_login" -> nativeSdkLogin(
                    requireHandle(),
                    stringField(request, "userId"),
                    stringField(request, "token"),
                    storeConfigJsonFromLogin(request),
                    contextId,
                )
                // 热启动的两半：此前没接，落到 else 返回 -1，「本地先出图、后台再连」从来没成功过。
                descriptor.cApi == "flare_sdk_prepare" -> nativeSdkPrepare(
                    requireHandle(),
                    stringField(request, "userId"),
                    storeConfigJsonFromLogin(request),
                    contextId,
                )
                descriptor.cApi == "flare_sdk_connect" -> nativeSdkConnect(
                    requireHandle(),
                    stringField(request, "userId"),
                    stringField(request, "token"),
                    contextId,
                )
                descriptor.cApi == "flare_sdk_logout" -> nativeSdkLogout(requireHandle(), contextId)
                descriptor.cApi == "flare_sdk_update_access_token" -> nativeSdkUpdateAccessToken(
                    requireHandle(),
                    stringField(request, "accessToken"),
                    stringField(request, "tenantId"),
                    contextId,
                )
                descriptor.cApi == "flare_sdk_current_user_id" -> nativeSdkCurrentUserId(requireHandle(), contextId)
                descriptor.cApi == "flare_sdk_disconnect" -> nativeSdkDisconnect(requireHandle(), contextId)
                descriptor.cApi == "flare_sdk_data_root" -> nativeSdkDataRoot(requireHandle(), contextId)
                else -> -1
            }
            if (submitCode != 0) {
                pending.remove(contextId)
                continuation.resumeWithException(
                    FlareSdkException(
                        code = "native_submit_failed",
                        message = "Native C ABI submit failed for ${descriptor.operation} (submitCode=$submitCode, cApi=${descriptor.cApi}).",
                        operation = descriptor.operation,
                        details = mapOf("submitCode" to submitCode.toString(), "cApi" to descriptor.cApi),
                    ),
                )
            }
        }

    private data class PendingCall(
        val descriptor: NativeCallDescriptor,
        val continuation: Continuation<Any?>,
    )

    private external fun nativeCreate(): Long
    private external fun nativeRelease(handle: Long)
    private external fun nativeHardReset()
    private external fun nativeIsConnected(handle: Long): Boolean
    private external fun nativeSessionActive(handle: Long): Boolean
    private external fun nativeSdkVersion(): String
    private external fun nativeFfiContractVersion(): String
    private external fun nativeMessageDispatchJson(handle: Long, op: String, requestJson: String, contextId: Long): Int
    private external fun nativeCapabilityDispatchJson(handle: Long, op: String, requestJson: String, contextId: Long): Int
    private external fun nativeSdkInvokeJson(handle: Long, apiId: String, requestJson: String, contextId: Long): Int
    private external fun nativeMessageBuildJson(handle: Long, requestJson: String, contextId: Long): Int
    private external fun nativeSdkInit(handle: Long, requestJson: String, contextId: Long): Int
    private external fun nativeSdkUninit(handle: Long, contextId: Long): Int
    private external fun nativeSdkLogin(handle: Long, userId: String, token: String, storeConfigJson: String, contextId: Long): Int
    private external fun nativeSdkPrepare(handle: Long, userId: String, storeConfigJson: String, contextId: Long): Int
    private external fun nativeSdkConnect(handle: Long, userId: String, token: String, contextId: Long): Int
    private external fun nativeSdkLogout(handle: Long, contextId: Long): Int
    private external fun nativeSdkUpdateAccessToken(handle: Long, accessToken: String, tenantId: String, contextId: Long): Int
    private external fun nativeSdkCurrentUserId(handle: Long, contextId: Long): Int
    private external fun nativeSdkDisconnect(handle: Long, contextId: Long): Int
    private external fun nativeSdkDataRoot(handle: Long, contextId: Long): Int
    private external fun nativeSetConnectTokenRefresher(handle: Long, enabled: Boolean, contextId: Long): Int
    private external fun nativeCompleteConnectTokenRefresh(handle: Long, requestId: Long, token: String?): Int
    private external fun nativeSubscribeEvents(handle: Long): Long
    private external fun nativeSubscribeEventsBatch(handle: Long): Long
    private external fun nativeUnsubscribeEvents(subscription: Long)
    private external fun nativeUnsubscribeAllEvents()

    companion object {
        private val nextContextId = AtomicLong(1)
        private val pending = ConcurrentHashMap<Long, PendingCall>()
        private val eventSinks = ConcurrentHashMap<Long, NativeEventSink>()
        private val tokenRefreshRouter = ConnectTokenRefreshRouter()

        /** Called by the JNI shim on an SDK thread when the core needs a new connect token. */
        @JvmStatic
        fun requestConnectToken(handle: Long, requestId: Long) {
            tokenRefreshRouter.request(handle, requestId)
        }

        @JvmStatic
        fun completeResult(contextId: Long, errorCode: Int, errorMessage: String?, errorDetailsJson: String?, resultJson: String?) {
            val pendingCall = pending.remove(contextId) ?: return
            if (errorCode != 0) {
                pendingCall.continuation.resumeWithException(
                    FlareSdkException.fromNative(
                        nativeCode = errorCode,
                        message = errorMessage ?: "Native C ABI call failed.",
                        operation = pendingCall.descriptor.operation,
                        detailsJson = errorDetailsJson,
                        details = mapOf("cApi" to pendingCall.descriptor.cApi),
                    ),
                )
                return
            }
            val value = when (pendingCall.descriptor.responseEncoding) {
                "unit" -> null
                "json", "json-object" -> decodeJsonValue(resultJson)
                else -> decodeJsonObject(resultJson)
            }
            pendingCall.continuation.resume(value)
        }

        @JvmStatic
        fun emitEvent(handle: Long, eventType: Int, eventJson: String?) {
            val sink = eventSinks[handle] ?: return
            sink(eventType, decodeJsonObject(eventJson))
        }

        /** 对象之外也有裸数组结果（`access.list_access_points`），按原样交给适配层解码。 */
        private fun decodeJsonValue(json: String?): Any? {
            val trimmed = json?.trimStart()
            if (trimmed == null || !trimmed.startsWith("[")) {
                return decodeJsonObject(json)
            }
            return wireDecodeResponse(jsonArrayToList(JSONArray(trimmed)))
        }

        private fun decodeJsonObject(json: String?): Map<String, Any?> {
            if (json.isNullOrBlank()) {
                return emptyMap()
            }
            val raw = jsonObjectToMap(JSONObject(json))
            @Suppress("UNCHECKED_CAST")
            return wireDecodeResponse(raw) as? Map<String, Any?> ?: emptyMap()
        }

        private fun jsonObjectToMap(value: JSONObject): Map<String, Any?> = buildMap {
            val keys = value.keys()
            while (keys.hasNext()) {
                val key = keys.next()
                put(key, jsonValue(value.get(key)))
            }
        }

        private fun jsonArrayToList(value: JSONArray): List<Any?> =
            List(value.length()) { index -> jsonValue(value.get(index)) }

        private fun jsonValue(value: Any?): Any? = when (value) {
            null, JSONObject.NULL -> null
            is JSONObject -> jsonObjectToMap(value)
            is JSONArray -> jsonArrayToList(value)
            else -> value
        }
    }

    private fun encodeJson(value: Any?): String = when (value) {
        null -> "{}"
        is Map<*, *> -> {
            @Suppress("UNCHECKED_CAST")
            val encoded = wireEncodeRequest(value as Map<String, Any?>) as? Map<*, *>
            JSONObject(encoded ?: value).toString()
        }
        is List<*> -> JSONArray(value).toString()
        is String -> value
        else -> JSONObject(mapOf("value" to value)).toString()
    }

    private fun storeConfigJsonFromLogin(request: Any?): String {
        val map = request as? Map<*, *> ?: return "{}"
        return map["storeConfigJson"]?.toString()?.takeIf { it.isNotBlank() } ?: "{}"
    }

    private fun stringField(value: Any?, name: String): String {
        val map = value as? Map<*, *> ?: return ""
        val field = map[name]
        if (field != null) return field.toString()
        return ""
    }

    private fun longField(value: Any?, name: String): Long {
        val map = value as? Map<*, *> ?: return 0
        val field = map[name]
        when (field) {
            is Number -> return field.toLong()
            is String -> return field.toLongOrNull() ?: 0
        }
        return 0
    }
}
