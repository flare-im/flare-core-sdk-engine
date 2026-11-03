//! 示例共用的开发 token 密钥获取。
//!
//! 这些示例要自己签一个 token 连服务端 —— 开源栈不含用户体系，这是「自带身份」
//! 模式下评估者拿到第一个可用 token 的方式（见 flare-core-server/QUICKSTART.md）。
//!
//! **不提供可用的默认密钥。** 早先几个示例写死 `"insecure-secret"`，而服务端从
//! 一开始就拒绝弱密钥，于是它们注定连不上；更糟的是客户端只会报「协商超时，
//! 请确认服务端已启动」—— 把人引去查服务和端口，真正的原因却是签名验不过。
//! 与其给一个签得出、用不了的默认值，不如在这里直接失败并说清楚去哪儿取。
//!
//! 取值顺序：
//!   1. `TOKEN_SECRET` / `ACCESS_GATEWAY_TOKEN_SECRET` / `FLARE_CORE_GATEWAY_TOKEN_SECRET`
//!   2. 同一工作区里 `flare-core-server/logs/.dev-token-secret`（`start_server.sh` 生成）
//!
//! 用法：
//! ```ignore
//! #[path = "dev_token.rs"]
//! mod dev_token;
//!
//! let secret = dev_token::require()?;
//! ```

use std::path::Path;

/// 工作区内 `start_server.sh` 生成的密钥文件。
fn dev_secret_file() -> Option<String> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent()?;
    Some(
        workspace_root
            .join("flare-core-server")
            .join("logs")
            .join(".dev-token-secret")
            .to_string_lossy()
            .to_string(),
    )
}

fn from_env() -> Option<String> {
    for key in [
        "TOKEN_SECRET",
        "ACCESS_GATEWAY_TOKEN_SECRET",
        "FLARE_CORE_GATEWAY_TOKEN_SECRET",
    ] {
        if let Ok(v) = std::env::var(key) {
            let t = v.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    None
}

fn from_file() -> Option<String> {
    let path = dev_secret_file()?;
    let content = std::fs::read_to_string(&path).ok()?;
    let t = content.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

/// 取开发 token 密钥；取不到就带着可执行的指引失败。
pub fn require() -> Result<String, Box<dyn std::error::Error>> {
    if let Some(s) = from_env().or_else(from_file) {
        return Ok(s);
    }
    Err(format!(
        "没有拿到 token 签名密钥，示例无法连上服务端。\n\
         \n\
         服务端不接受弱密钥，所以这里不会退回一个「签得出但用不了」的默认值 ——\n\
         那只会让你在客户端看到一句「协商超时」，然后去排查根本没问题的服务和端口。\n\
         \n\
         取其一即可：\n\
         \x20 1) 先起开源栈：cd flare-im-core && ./scripts/start_server.sh\n\
         \x20    它会把密钥写到 {}\n\
         \x20    示例会自动读取，无需额外设置。\n\
         \x20 2) 或显式指定：export TOKEN_SECRET=<你的强密钥>（与服务端同一把）",
        dev_secret_file().unwrap_or_else(|| "flare-core-server/logs/.dev-token-secret".into())
    )
    .into())
}
