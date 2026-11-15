//! SDK 托管令牌的刷新令牌放进宿主的安全存储。
//!
//! 刷新令牌原来只在内存里：进程被杀之后再被推送或系统在后台拉起，没有界面、也没有可用的令牌，
//! 只能重新签发，而生产部署的签发要应用凭据，客户端拿不出来，于是后台唤醒连不上。放进宿主的安全
//! 存储（就是给加密本地库存密钥的那一个，钥匙串、Keystore 等）之后，无界面的冷启动凭它换出连接
//! 令牌。按用户分开存；每次换新都写回轮换后的那一枚；登出、换账号、服务端拒绝时删掉。
//!
//! 宿主没有给安全存储（以普通本地库登录）时什么都不存，行为与以前一样：重启后重新签发。

use std::sync::Arc;
use std::time::Duration;

use crate::platform::ports::storage::{SecureKeyDescriptor, SecureKeyStore, SecureSecret};
use crate::shared::util::timeout;

/// 安全存储里这一项的用途名。
const REFRESH_TOKEN_PURPOSE: &str = "refresh_token.v1";
/// 读写宿主安全存储的时限：它跨进宿主（钥匙串、Keystore），不能让建连一直等。
const VAULT_IO_CAP: Duration = Duration::from_secs(2);

/// 一个登录会话用的刷新令牌保管处：宿主的安全存储，加上加密本地库用的同一组命名空间与租户。
#[derive(Clone)]
pub(crate) struct RefreshTokenVault {
    store: Arc<dyn SecureKeyStore>,
    namespace: String,
    tenant_id: String,
}

impl RefreshTokenVault {
    // 只有加密本地库登录（`lifecycle-sqlite`）才有宿主安全存储可放刷新令牌。
    #[cfg_attr(not(feature = "lifecycle-sqlite"), allow(dead_code))]
    pub fn new(store: Arc<dyn SecureKeyStore>, namespace: &str, tenant_id: &str) -> Self {
        Self {
            store,
            namespace: namespace.to_owned(),
            tenant_id: tenant_id.to_owned(),
        }
    }

    fn descriptor(&self, user_id: &str) -> SecureKeyDescriptor {
        SecureKeyDescriptor::new(
            self.namespace.as_str(),
            self.tenant_id.as_str(),
            user_id,
            REFRESH_TOKEN_PURPOSE,
        )
    }

    /// 这个用户存着的刷新令牌；没有、读不出或超时都为 `None`。
    pub async fn load(&self, user_id: &str) -> Option<String> {
        let descriptor = self.descriptor(user_id);
        match timeout(VAULT_IO_CAP, self.store.get_secret(&descriptor)).await {
            Ok(Ok(Some(secret))) => String::from_utf8(secret.into_bytes())
                .ok()
                .filter(|token| !token.trim().is_empty()),
            Ok(Ok(None)) => None,
            Ok(Err(error)) => {
                tracing::warn!(%error, "reading the stored refresh token failed");
                None
            }
            Err(_) => {
                tracing::warn!("reading the stored refresh token timed out");
                None
            }
        }
    }

    /// 写入（覆盖）这个用户的刷新令牌。写不进去只记日志：下次冷启动退回重新签发。
    pub async fn save(&self, user_id: &str, token: &str) {
        let Ok(secret) = SecureSecret::new(token.as_bytes().to_vec()) else {
            return;
        };
        let descriptor = self.descriptor(user_id);
        match timeout(VAULT_IO_CAP, self.store.put_secret(&descriptor, secret)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "storing the refresh token failed"),
            Err(_) => tracing::warn!("storing the refresh token timed out"),
        }
    }

    /// 删掉这个用户的刷新令牌（登出、换账号、服务端拒绝了它）。
    pub async fn clear(&self, user_id: &str) {
        let descriptor = self.descriptor(user_id);
        match timeout(VAULT_IO_CAP, self.store.delete_secret(&descriptor)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "deleting the stored refresh token failed"),
            Err(_) => tracing::warn!("deleting the stored refresh token timed out"),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::platform::ports::storage::VolatileSecureKeyStore;

    /// 按用户分开存，覆盖写，删了就没有；与加密本地库的密钥不是同一项。
    #[tokio::test]
    async fn refresh_tokens_are_kept_per_user() {
        let store = Arc::new(VolatileSecureKeyStore::new());
        let vault = RefreshTokenVault::new(store.clone(), "flare-im-core-sdk", "0");
        assert_eq!(vault.load("alice").await, None);

        vault.save("alice", "r1").await;
        vault.save("alice", "r2").await;
        vault.save("bob", "b1").await;
        assert_eq!(vault.load("alice").await.as_deref(), Some("r2"));
        assert_eq!(vault.load("bob").await.as_deref(), Some("b1"));
        let database_key = SecureKeyDescriptor::local_database("flare-im-core-sdk", "0", "alice");
        assert!(store.get_secret(&database_key).await.unwrap().is_none());

        vault.clear("alice").await;
        assert_eq!(vault.load("alice").await, None);
        assert_eq!(vault.load("bob").await.as_deref(), Some("b1"));
        let other_tenant = RefreshTokenVault::new(store, "flare-im-core-sdk", "t2");
        assert_eq!(other_tenant.load("bob").await, None, "租户也分开");
    }
}
