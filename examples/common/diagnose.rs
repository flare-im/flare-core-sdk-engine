//! 把示例里最常见的失败翻译成人能直接照做的一句话。
//!
//! 为什么需要它：示例出错时默认打印的是 `?` 冒泡上来的 Debug 结构体，例如
//!
//! ```text
//! Error: Localized { code: ConnectionFailed, reason: "connect failed
//! primary=ws://localhost:60051 ... Connection refused (os error 61)",
//! details: None, params: None, timestamp: ... }
//! ```
//!
//! 对写这份代码的人它信息量足够；对第一次跑示例的人，它既不说人话，也不说
//! 下一步该做什么——而这恰恰是评估者看到的第一个输出。
//!
//! 与 `dev_token.rs` 同一个立场：宁可在出错时多说一句「去哪儿、做什么」，
//! 也不要让人对着一个结构体猜。这里只做翻译，不吞错误——原文照旧打印。

#![allow(dead_code)]

/// 按错误内容给出可照做的提示；认不出来就返回 None（不硬编故事）。
pub fn hint_for(err: &str) -> Option<&'static str> {
    let lower = err.to_lowercase();

    if lower.contains("connection refused") || lower.contains("connection_failed") {
        return Some(
            "服务端没在跑。先在 flare-im-core 里执行：\n\
             \x20   docker compose -f deploy/docker-compose.yml up -d consul redis postgres nats rustfs\n\
             \x20   ./scripts/start_server.sh\n\
             \x20 起完用 ./scripts/check_services.sh 确认全部就绪，再回来跑这个示例。",
        );
    }

    // 密钥对不上时服务端只会拒绝握手，客户端看到的却是「超时」——
    // 不点破的话，人会去查网络和端口，而真正的原因是签名验不过。
    if lower.contains("unauthorized")
        || lower.contains("invalid token")
        || lower.contains("401")
        || lower.contains("handshake")
    {
        return Some(
            "握手被拒，通常是签名密钥或 issuer 与服务端不一致。\n\
             \x20 密钥取自 flare-core-server/logs/.dev-token-secret（start_server.sh 生成），\n\
             \x20 issuer 需与 config/services/api-gateway.toml 里的 token_issuer 相同。",
        );
    }

    if lower.contains("no such file") && lower.contains("dev-token-secret") {
        return Some("还没起过服务端：logs/.dev-token-secret 由 ./scripts/start_server.sh 生成。");
    }

    None
}

/// 在 `main` 末尾包一层：原样返回错误，但先把可照做的提示打到 stderr。
///
/// 用法：
/// ```ignore
/// #[path = "common/diagnose.rs"]
/// mod diagnose;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     diagnose::explain(run().await)
/// }
/// ```
pub fn explain<T, E: std::fmt::Debug>(result: Result<T, E>) -> Result<T, E> {
    if let Err(e) = &result
        && let Some(hint) = hint_for(&format!("{e:?}"))
    {
        eprintln!("\n提示：{hint}\n");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_connection_refused() {
        let raw = "Localized { code: ConnectionFailed, reason: \"connect failed \
                   primary=ws://localhost:60051: IO error: Connection refused (os error 61)\" }";
        assert!(hint_for(raw).unwrap().contains("start_server.sh"));
    }

    #[test]
    fn stays_quiet_on_unknown_errors() {
        // 认不出来就闭嘴：编一句误导性的提示比不给提示更糟。
        assert!(hint_for("some unrelated failure").is_none());
    }
}
