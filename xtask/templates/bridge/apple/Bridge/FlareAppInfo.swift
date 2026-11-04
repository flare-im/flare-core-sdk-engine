import Foundation

/// 宿主应用的版本信息：随 `sdk.init` 交给引擎，引擎在 CONNECT 时上报
/// （`app_version` / `app_build` / `distribution`），服务端据此做版本兼容判断与排查。
///
/// 默认从 `Bundle.main` 自动采集；宿主在 init 请求里显式给的 `appVersion` / `appBuild` /
/// `distribution` 一律优先，这里只补缺的项。
public struct FlareAppInfo: Sendable, Equatable {
    /// 版本名，对应 `CFBundleShortVersionString`。
    public var appVersion: String?
    /// 单调递增的整数构建号，对应 `CFBundleVersion`；服务端版本比较只用它。
    public var appBuild: UInt64?
    /// 分发渠道，如 `appstore`、`direct`。安装来源 SDK 判断不准，默认不填，由宿主给。
    public var distribution: String?

    public init(appVersion: String? = nil, appBuild: UInt64? = nil, distribution: String? = nil) {
        self.appVersion = appVersion
        self.appBuild = appBuild
        self.distribution = distribution
    }

    /// 从 bundle 的 Info.plist 读版本名与构建号。
    ///
    /// `CFBundleVersion` 只有是纯数字时才当构建号：服务端按整数比较，`1.2.3` 这类写法换算不出
    /// 单调整数，不猜，留空由宿主经 `appBuild` 传入。
    public static func fromBundle(_ bundle: Bundle = .main) -> FlareAppInfo {
        let info = bundle.infoDictionary ?? [:]
        return FlareAppInfo(
            appVersion: nonBlank(info["CFBundleShortVersionString"] as? String),
            appBuild: (info["CFBundleVersion"] as? String).flatMap(parseBuildNumber)
        )
    }

    static func parseBuildNumber(_ raw: String) -> UInt64? {
        let trimmed = raw.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty, trimmed.allSatisfy({ $0.isASCII && $0.isNumber }) else {
            return nil
        }
        return UInt64(trimmed)
    }

    private static func nonBlank(_ value: String?) -> String? {
        guard let value = value?.trimmingCharacters(in: .whitespaces), !value.isEmpty else {
            return nil
        }
        return value
    }

    /// 把还没填的项补进 init 请求。引擎接受两种形状（与 `flare_sdk_init` 的解析一致）：
    /// 裸配置补在顶层；带 `environment` / `sdkConfig` 的包装补进 `sdkConfig`。
    func fill(_ request: Any?) -> [String: Any] {
        var root = FfiJson.asMap(request)
        let wrapped = root["sdkConfig"] != nil || root["environment"] != nil
        var config = wrapped ? FfiJson.asMap(root["sdkConfig"]) : root
        func setIfMissing(_ key: String, _ value: Any?) {
            guard let value else { return }
            if let existing = config[key], !(existing is NSNull) {
                if let text = existing as? String, text.trimmingCharacters(in: .whitespaces).isEmpty {
                    config[key] = value
                }
                return
            }
            config[key] = value
        }
        setIfMissing("appVersion", appVersion)
        setIfMissing("appBuild", appBuild)
        setIfMissing("distribution", distribution)
        if wrapped {
            root["sdkConfig"] = config
            return root
        }
        return config
    }
}
