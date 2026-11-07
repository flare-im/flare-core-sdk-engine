package com.flare.im

import android.content.Context
import com.flare.im.adapter.DefaultFlareImClient
import com.flare.im.api.FlareImClient
import com.flare.im.bridge.FlareAppInfo
import com.flare.im.bridge.JniNativeBridge
import com.flare.im.contract.NativeBridge
import com.flare.im.lifecycle.PlatformLifecycle
import com.flare.im.lifecycle.system

/** Public entry for Android apps. */
object FlareCoreSdk {
    fun createClient(bridge: NativeBridge = JniNativeBridge()): FlareImClient =
        DefaultFlareImClient(bridge)

    /**
     * 推荐入口：从 [context] 自动采集应用版本名、构建号与安装来源（`PackageManager`），`sdk.init` 时
     * 补进配置的 `app` 对象，CONNECT 时上报给服务端；宿主在 init 请求 `app` 对象里显式给的键优先。
     * `sdk.init` 成功后自动装上 [PlatformLifecycle]（默认网络与进程前后台），init 请求带
     * `installPlatformLifecycle = false` 时不装。
     */
    fun createClient(context: Context): FlareImClient =
        DefaultFlareImClient(
            JniNativeBridge(appInfo = FlareAppInfo.fromContext(context)),
            PlatformLifecycle.system(context),
        )

    fun createClientWithBridge(bridge: NativeBridge): FlareImClient =
        DefaultFlareImClient(bridge)
}
