package com.flare.im.bridge

import android.content.Context
import android.content.pm.PackageInfo
import android.content.pm.PackageManager
import android.os.Build
import java.util.Locale

/**
 * 宿主应用的版本信息：随 `sdk.init` 写进配置的 `app` 对象（`version` / `build` / `distribution`），
 * 引擎在 CONNECT 时上报，服务端据此做版本兼容判断与排查。
 *
 * 用 [fromContext] 从 `PackageManager` 自动采集；宿主在 init 请求的 `app` 对象里显式给的
 * `version` / `build` / `distribution` 一律优先，这里只补缺的项；`channel` 不上报，也不自动补。
 */
data class FlareAppInfo(
    /** 版本名，对应 `versionName`。 */
    val appVersion: String? = null,
    /** 单调递增的整数构建号，对应 `longVersionCode`；服务端版本比较只用它。 */
    val appBuild: Long? = null,
    /**
     * 分发渠道：按安装来源判断（Google Play → `play`、华为应用市场 → `appgallery`），认不出的按 `direct`。
     * 宿主显式给的优先。
     */
    val distribution: String? = null,
    /** 系统语言（BCP-47）：更新说明按它选。App 内切语言的宿主显式传 `app.locale`。 */
    val locale: String? = null,
) {
    /**
     * 把还没填的项补进 init 请求配置的 `app` 对象。引擎接受两种形状（与 `flare_sdk_init` 的解析一致）：
     * 裸配置的 `app`；带 `environment` / `sdkConfig` 的包装补进 `sdkConfig.app`。
     * 什么都没有可补时不写 `app`：引擎拿到 `app` 会整体替换构建期的值。
     */
    fun fill(request: Any?): Map<String, Any?> {
        val root = stringKeyed(request)
        val wrapped = root.containsKey("sdkConfig") || root.containsKey("environment")
        val config = (if (wrapped) stringKeyed(root["sdkConfig"]) else root).toMutableMap()
        val app = stringKeyed(config["app"]).toMutableMap()
        putIfMissing(app, "version", appVersion)
        putIfMissing(app, "build", appBuild)
        putIfMissing(app, "distribution", distribution)
        putIfMissing(app, "locale", locale)
        if (app.isNotEmpty()) {
            config["app"] = app
        }
        return if (wrapped) root + ("sdkConfig" to config) else config
    }

    companion object {
        /**
         * 从 `PackageManager` 读本应用的 `versionName` / `longVersionCode` 与安装来源
         * （`getInstallSourceInfo`，不需要权限）；读不到时各项留空。
         */
        fun fromContext(context: Context): FlareAppInfo {
            val distribution = distributionOrDirect(runCatching { installerPackage(context) }.getOrNull())
            val locale = Locale.getDefault().toLanguageTag().takeUnless { it.isEmpty() || it == "und" }
            val info = runCatching { packageInfo(context) }.getOrNull()
                ?: return FlareAppInfo(distribution = distribution, locale = locale)
            return FlareAppInfo(
                appVersion = info.versionName?.trim()?.takeIf { it.isNotEmpty() },
                appBuild = versionCode(info),
                distribution = distribution,
                locale = locale,
            )
        }

        /**
         * 安装来源包名到分发渠道，只认得出的几种；adb（`com.android.shell`）、读不到的和其他商店都不填，
         * 交给构建期按渠道注入的 `app.distribution`（发布清单按它找制品，任意包名找不到）。
         */
        internal fun distributionOf(installer: String?): String? = when (installer?.trim()) {
            null, "", "com.android.shell" -> null
            "com.android.vending" -> "play"
            "com.huawei.appmarket" -> "appgallery"
            "com.google.android.packageinstaller", "com.android.packageinstaller" -> "direct"
            else -> null
        }

        /** 认不出安装来源时按直装算：发布清单按分发渠道找制品，留空就找不到。 */
        internal fun distributionOrDirect(installer: String?): String = distributionOf(installer) ?: "direct"

        private fun installerPackage(context: Context): String? {
            val manager = context.packageManager
            return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                manager.getInstallSourceInfo(context.packageName).installingPackageName
            } else {
                @Suppress("DEPRECATION")
                manager.getInstallerPackageName(context.packageName)
            }
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

        private fun putIfMissing(app: MutableMap<String, Any?>, key: String, value: Any?) {
            if (value == null) return
            val existing = app[key]
            if (existing == null || (existing is String && existing.isBlank())) {
                app[key] = value
            }
        }
    }
}
