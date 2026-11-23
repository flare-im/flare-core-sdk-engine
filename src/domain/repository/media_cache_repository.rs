use async_trait::async_trait;

use crate::domain::MediaCacheEntryVo;
use crate::shared::error::Result;

/// 媒体文件本地缓存端口：持久化 file_id 与落盘路径对照（通常由 SQLite + 文件系统实现）。
#[async_trait]
pub trait MediaCacheStore: Send + Sync {
    /// Remember a local upload source by its assigned remote ID, independently of cached bytes.
    async fn remember_upload_source(&self, _file_id: &str, _path: &str) -> Result<()> {
        Ok(())
    }

    async fn upload_source(&self, _file_id: &str) -> Result<Option<String>> {
        Ok(None)
    }

    /// 返回仍存在于磁盘上的缓存；文件缺失时清理脏行并返回 `None`。
    async fn get_cached(&self, file_id: &str) -> Result<Option<MediaCacheEntryVo>>;

    /// 将字节写入缓存目录并 upsert 对照表。
    async fn put_bytes(
        &self,
        file_id: &str,
        data: &[u8],
        mime_type: &str,
    ) -> Result<MediaCacheEntryVo>;

    /// 删除记录；若本地文件存在则一并删除（忽略文件删除错误）。
    async fn remove(&self, file_id: &str) -> Result<()>;

    /// 写入缓存前的暂存目录（与缓存根同一文件系统，暂存文件可直接改名进缓存）。
    async fn staging_dir(&self) -> Result<std::path::PathBuf>;

    /// 把磁盘上的文件登记进缓存：`move_source` 为真时改名搬入（暂存文件），否则复制一份。
    async fn put_file(
        &self,
        file_id: &str,
        source: &std::path::Path,
        mime_type: &str,
        move_source: bool,
    ) -> Result<MediaCacheEntryVo>;
}
