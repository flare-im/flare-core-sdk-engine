package com.flare.im.bridge

import android.content.Context
import android.content.pm.PackageInfo
import android.content.pm.PackageManager
import android.os.Build

/**
 * 宿主应用的版本信息：随 `sdk.init` 交给引擎，引擎在 CONNECT 时上报
 * （`app_version` / `app_build` / `distribution`），服务端据此做版本兼容判断与排查。
 *
 * 用 [fromContext] 从 `PackageManager` 自动采集；宿主在 init 请求里显式给的 `appVersion` /
 * `appBuild` / `distribution` 一律优先，这里只补缺的项。
 */
data class FlareAppInfo(
    /** 版本名，对应 `versionName`。 */
    val appVersion: String? = null,
    /** 单调递增的整数构建号，对应 `longVersionCode`；服务端版本比较只用它。 */
    val appBuild: Long? = null,
    /** 分发渠道，如 `play`、`appgallery`、`direct`。安装来源 SDK 判断不准，默认不填，由宿主给。 */
    val distribution: String? = null,
) {
    /**
     * 把还没填的项补进 init 请求。引擎接受两种形状（与 `flare_sdk_init` 的解析一致）：
     * 裸配置补在顶层；带 `environment` / `sdkConfig` 的包装补进 `sdkConfig`。
     */
    fun fill(request: Any?): Map<String, Any?> {
        val root = stringKeyed(request)
        val wrapped = root.containsKey("sdkConfig") || root.containsKey("environment")
        val config = (if (wrapped) stringKeyed(root["sdkConfig"]) else root).toMutableMap()
        putIfMissing(config, "appVersion", appVersion)
        putIfMissing(config, "appBuild", appBuild)
        putIfMissing(config, "distribution", distribution)
        return if (wrapped) root + ("sdkConfig" to config) else config
    }

    companion object {
        /** 从 `PackageManager` 读本应用的 `versionName` / `longVersionCode`；读不到时各项留空。 */
        fun fromContext(context: Context): FlareAppInfo {
            val info = runCatching { packageInfo(context) }.getOrNull() ?: return FlareAppInfo()
            return FlareAppInfo(
                appVersion = info.versionName?.trim()?.takeIf { it.isNotEmpty() },
                appBuild = versionCode(info),
            )
        }

        private fun packageInfo(context: Context): PackageInfo {
            val manager = context.packageManager
            return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                manager.getPackageInfo(context.packageName, PackageManager.PackageInfoFlags.of(0))
            } else {
                @Suppress("DEPRECATION")
                manager.getPackageInfo(context.packageName, 0)
            }
        }

        private fun versionCode(info: PackageInfo): Long =
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
                info.longVersionCode
            } else {
                @Suppress("DEPRECATION")
                info.versionCode.toLong()
            }

        private fun stringKeyed(value: Any?): Map<String, Any?> =
            (value as? Map<*, *>)?.entries?.associate { (key, item) -> key.toString() to item }
                ?: emptyMap()

        private fun putIfMissing(config: MutableMap<String, Any?>, key: String, value: Any?) {
            if (value == null) return
            val existing = config[key]
            if (existing == null || (existing is String && existing.isBlank())) {
                config[key] = value
            }
        }
    }
}
