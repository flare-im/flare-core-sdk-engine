package com.flare.im

import android.content.Context
import com.flare.im.adapter.DefaultFlareImClient
import com.flare.im.api.FlareImClient
import com.flare.im.bridge.FlareAppInfo
import com.flare.im.bridge.JniNativeBridge
import com.flare.im.contract.NativeBridge

/** Public entry for Android apps. */
object FlareCoreSdk {
    fun createClient(bridge: NativeBridge = JniNativeBridge()): FlareImClient =
        DefaultFlareImClient(bridge)

    /**
     * 推荐入口：从 [context] 自动采集应用版本名与构建号（`PackageManager`），`sdk.init` 时补进配置，
     * CONNECT 时上报给服务端。宿主在 init 请求里显式给的 `appVersion` / `appBuild` /
     * `distribution` 优先；渠道 SDK 不猜，需要时由宿主传。
     */
    fun createClient(context: Context): FlareImClient =
        DefaultFlareImClient(JniNativeBridge(appInfo = FlareAppInfo.fromContext(context)))

    fun createClientWithBridge(bridge: NativeBridge): FlareImClient =
        DefaultFlareImClient(bridge)
}
