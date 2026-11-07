//! 安装 ID（§4.5）：随机 UUID，灰度分桶用。首次使用时生成，只存在本地全局存储里，不关联设备标识。

use rand::RngCore;

use super::{AccessStore, keys};

/// 读安装 ID，没有就生成并写入。写失败时本次进程照常用生成的值，下次启动再换一个：
/// 分桶结果只影响灰度顺序，不值得为此让更新检查失败。
pub async fn install_id(store: &dyn AccessStore) -> String {
    if let Ok(Some(bytes)) = store.get(keys::INSTALL_ID).await
        && let Ok(text) = String::from_utf8(bytes)
    {
        let text = text.trim();
        if !text.is_empty() {
            return text.to_owned();
        }
    }
    let id = random_uuid();
    let _ = store.put(keys::INSTALL_ID, id.as_bytes()).await;
    id
}

/// UUID v4 的文本形式。
fn random_uuid() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryAccessStore;

    #[tokio::test]
    async fn the_id_is_generated_once_and_survives() {
        let store = MemoryAccessStore::new();
        let first = install_id(&store).await;
        assert_eq!(first.len(), 36);
        assert_eq!(&first[14..15], "4");
        assert_eq!(install_id(&store).await, first);
    }

    #[tokio::test]
    async fn a_failed_write_still_yields_an_id() {
        let store = MemoryAccessStore::new();
        store.set_fail_writes(true);
        assert_eq!(install_id(&store).await.len(), 36);
    }
}
