//! 建连用的令牌来源：由会话层（`IMClient`）实现，链路状态机只管什么时候要、要几次。
//!
//! 两条规则来自历史教训：
//! - 只在网关**明确拒绝**之后才续期：断网时去换，刷新请求多半也失败，而一次性的刷新令牌失败后
//!   宿主不会再拿它重试，会话就此作废；
//! - 续期得到的令牌必须与被拒那枚**不同**才算换到：宿主刷新成功后往往另起任务把新令牌推进来，
//!   和写回时的值比会误判成「没换到」。
//!
//! SDK 托管令牌的签发也在这里（每次尝试建连前）：登录时不再先签发，离线冷启动时链路照常起来、
//! 显示等网络，联网后先签发再握手。

/// 一次建连前取令牌的结果。
#[derive(Debug)]
pub(crate) enum ConnectToken {
    Ready(String),
    /// 要走网络才能拿到令牌（SDK 托管的签发、临期换新），而这次没走通：按一次连接失败处理，
    /// 退避后下一轮再取；错误码是网络类的，`connect` 与宿主给令牌的路径一样报 `NETWORK_UNREACHABLE`。
    Unreachable(crate::shared::error::FlareError),
    /// 没有令牌、也换不到（签发被网关拒绝、会话已失效）：需要用户动作。
    Unavailable,
}

// 与驱动不同，令牌来源随 `LinkCommand::Start` 经通道交给状态机，通道句柄又被宿主状态（wasm 绑定
// 要求 `Sync`）持有，所以 wasm 上也必须 `Send + Sync`；实现只持弱引用的客户端，本就满足。
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub(crate) trait LinkTokenSource: Send + Sync {
    /// 建连前取令牌。SDK 托管形态下还没有令牌就先签发，令牌按服务端时钟已临期（30 秒内到期）就先
    /// 向网关换新；宿主托管形态原样返回（续期只在被拒之后）。
    async fn token_for_connect(&self) -> ConnectToken;

    /// 网关以鉴权原因拒绝了 `rejected`：续期一次。换到的令牌与 `rejected` 不同才返回。
    async fn renew_after_rejection(&self, rejected: &str) -> Option<String>;
}
