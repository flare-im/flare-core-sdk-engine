//! 更新中心的宿主 API：`update.*`。
//!
//! 登录前就可用：只依赖 `init` 建好的接入运行时，不依赖 IM 会话。预期内的失败（需要确认计费网络、
//! 强制版本不能跳过等）放在返回值的 `error` 里，不抛错；抛错只有配置问题和参数错误。

use std::sync::Arc;

use flare_access::release::{UpdateInstallPlan, UpdateResources, UpdateStatus};

use crate::runtime::update::UpdateRuntime;
use crate::shared::error::{ErrorCode, FlareError, Result};

use super::IMClient;

impl IMClient {
    pub(crate) async fn update_runtime(&self) -> Option<Arc<UpdateRuntime>> {
        self.access_runtime().await?.update()
    }

    async fn require_update(&self) -> Result<Arc<UpdateRuntime>> {
        self.update_runtime().await.ok_or_else(|| {
            FlareError::localized(
                ErrorCode::ConfigurationError,
                "the update center is not initialised; call init first",
            )
        })
    }

    /// 当前更新状态，不发网络请求。下载中的进度也在里面。`foreground` 时先按回前台处理，幂等：
    /// 同一次回前台再经 `lifecycle.set_app_state` 到一次不算第二次。
    pub async fn update_state(&self, foreground: bool) -> Result<UpdateStatus> {
        Ok(self.require_update().await?.state(foreground).await)
    }

    /// 选更新说明和封禁原因的语言，优先于 `app.locale`，重新 `init` 后还在。
    pub async fn set_update_locale(&self, locale: String) -> Result<UpdateStatus> {
        Ok(self.require_update().await?.set_locale(locale).await)
    }

    /// 本次运行的资源包，按 `id` 排序。进程启动时读定，运行中下载或撤回的包下次启动才生效。
    pub async fn update_resources(&self) -> Result<UpdateResources> {
        Ok(self.require_update().await?.resources().await)
    }

    /// 查更新，不等这一轮：要查就进 `checking`，放到后台跑，立刻返回。`manual` 不受 6 小时频率限制，
    /// 失败时写进 `error`。结果经 `update://state` 与 [`Self::update_state`] 取。
    pub async fn check_update(&self, manual: bool) -> Result<UpdateStatus> {
        Ok(self.require_update().await?.request_check(manual))
    }

    /// 下载安装包（只有 `kind=package` 走这里），等下载结束才返回。
    pub async fn download_update(&self, allow_metered: bool) -> Result<UpdateStatus> {
        Ok(self.require_update().await?.download(allow_metered).await)
    }

    /// 取安装计划，状态进 `installing`。平台包执行计划后用 [`Self::report_update_install`] 回报。
    pub async fn install_update(&self) -> Result<UpdateInstallPlan> {
        self.require_update().await?.install().await
    }

    /// 回报安装结果：`started`、`completed`、`cancelled`、`failed`。
    pub async fn report_update_install(
        &self,
        outcome: &str,
        code: Option<&str>,
    ) -> Result<UpdateStatus> {
        self.require_update()
            .await?
            .report_install(outcome, code)
            .await
    }

    /// 推荐版本推迟 24 小时。
    pub async fn defer_update(&self) -> Result<UpdateStatus> {
        Ok(self.require_update().await?.defer().await)
    }

    /// 跳过某个可选版本。
    pub async fn skip_update(&self, build: u64) -> Result<UpdateStatus> {
        Ok(self.require_update().await?.skip(build).await)
    }

    /// UI 真的弹出了提示。频率规则靠它计数。
    pub async fn mark_update_prompted(
        &self,
        build: Option<u64>,
        form: &str,
    ) -> Result<UpdateStatus> {
        self.require_update()
            .await?
            .mark_prompted(build, form)
            .await
    }
}
