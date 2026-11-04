package com.flare.im.bridge

import com.flare.im.adapter.codec.wireDecodeResponse
import com.flare.im.adapter.codec.wireEncodeRequest
import com.flare.im.contract.NativeBridge
import com.flare.im.contract.NativeCallDescriptor
import com.flare.im.contract.SdkErrorCodes
import com.flare.im.contract.SdkOperations
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

internal typealias NativeEventSink = (Int, Map<String, Any?>) -> Unit

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
                nativeHardReset()
                handle = 0
                released = true
                null
            }
            "sdk.is_connected" -> nativeIsConnected(requireHandle())
            "sdk.session_active" -> nativeSessionActive(requireHandle())
            "connection.get_state" -> nativeConnectionState(requireHandle()).let { stateCode ->
                when (stateCode) {
                    0 -> com.flare.im.api.ConnectionState.DISCONNECTED
                    1 -> com.flare.im.api.ConnectionState.CONNECTING
                    2 -> com.flare.im.api.ConnectionState.CONNECTED
                    3 -> com.flare.im.api.ConnectionState.READY
                    4 -> com.flare.im.api.ConnectionState.RECONNECTING
                    else -> throw FlareSdkException(
                        SdkErrorCodes.INVALIDPARAMETER,
                        "invalid connection state code: $stateCode",
                        operation = "connection.get_state",
                        details = mapOf("field" to "stateCode"),
                    )
                }
            }
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
    private external fun nativeConnectionState(handle: Long): Int
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
    private external fun nativeSubscribeEvents(handle: Long): Long
    private external fun nativeSubscribeEventsBatch(handle: Long): Long
    private external fun nativeUnsubscribeEvents(subscription: Long)
    private external fun nativeUnsubscribeAllEvents()

    companion object {
        private val nextContextId = AtomicLong(1)
        private val pending = ConcurrentHashMap<Long, PendingCall>()
        private val eventSinks = ConcurrentHashMap<Long, NativeEventSink>()

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
                "json", "json-object" -> decodeJsonObject(resultJson)
                else -> decodeJsonObject(resultJson)
            }
            pendingCall.continuation.resume(value)
        }

        @JvmStatic
        fun emitEvent(handle: Long, eventType: Int, eventJson: String?) {
            val sink = eventSinks[handle] ?: return
            sink(eventType, decodeJsonObject(eventJson))
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
