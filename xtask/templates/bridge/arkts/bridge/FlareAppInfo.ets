// GENERATED. Do not edit by hand.
import { bundleManager } from '@kit.AbilityKit';

/**
 * 宿主应用的版本信息：随 `sdk.init` 交给引擎，引擎在 CONNECT 时上报
 * （`app_version` / `app_build` / `distribution`），服务端据此做版本兼容判断与排查。
 *
 * 默认用 [FlareAppInfo.fromBundle] 从本应用的 bundle 信息采集；宿主在 init 请求里显式给的
 * `appVersion` / `appBuild` / `distribution` 一律优先，这里只补缺的项。
 */
export class FlareAppInfo {
  /** 版本名，对应 `versionName`。 */
  appVersion?: string;
  /** 单调递增的整数构建号，对应 `versionCode`；服务端版本比较只用它。 */
  appBuild?: number;
  /** 分发渠道，如 `appgallery`、`direct`。安装来源 SDK 判断不准，默认不填，由宿主给。 */
  distribution?: string;

  constructor(appVersion?: string, appBuild?: number, distribution?: string) {
    this.appVersion = appVersion;
    this.appBuild = appBuild;
    this.distribution = distribution;
  }

  /** 读本应用的 `versionName` / `versionCode`；读不到时各项留空，SDK 不编造版本。 */
  static fromBundle(): FlareAppInfo {
    try {
      const info = bundleManager.getBundleInfoForSelfSync(
        bundleManager.BundleFlag.GET_BUNDLE_INFO_DEFAULT,
      );
      const name = info.versionName.trim();
      return new FlareAppInfo(
        name.length > 0 ? name : undefined,
        info.versionCode > 0 ? info.versionCode : undefined,
      );
    } catch (e) {
      return new FlareAppInfo();
    }
  }

  /**
   * 把还没填的项补进 init 请求。引擎接受两种形状（与 `flare_sdk_init` 的解析一致）：
   * 裸配置补在顶层；带 `environment` / `sdkConfig` 的包装补进 `sdkConfig`。
   */
  fill(request?: Object): Object {
    const root = FlareAppInfo.copyOf(request);
    const wrapped = root['sdkConfig'] !== undefined || root['environment'] !== undefined;
    const config = wrapped ? FlareAppInfo.copyOf(root['sdkConfig']) : root;
    FlareAppInfo.setIfMissing(config, 'appVersion', this.appVersion);
    FlareAppInfo.setIfMissing(config, 'appBuild', this.appBuild);
    FlareAppInfo.setIfMissing(config, 'distribution', this.distribution);
    if (wrapped) {
      root['sdkConfig'] = config;
    }
    return root;
  }

  private static copyOf(value?: Object): Record<string, Object> {
    const copy: Record<string, Object> = {};
    if (value === undefined || value === null || typeof value !== 'object') {
      return copy;
    }
    const source = value as Record<string, Object>;
    Object.keys(source).forEach((key: string) => {
      copy[key] = source[key];
    });
    return copy;
  }

  private static setIfMissing(config: Record<string, Object>, key: string, value?: Object): void {
    if (value === undefined) {
      return;
    }
    const existing = config[key];
    const blank = typeof existing === 'string' && (existing as string).trim().length === 0;
    if (existing === undefined || existing === null || blank) {
      config[key] = value;
    }
  }
}
