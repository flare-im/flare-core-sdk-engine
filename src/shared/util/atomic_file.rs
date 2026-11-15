//! 原子替换一个小文件：先写同目录下的临时文件，再改名盖上去。读的一方（可能是另一个进程，例如
//! iOS 通知扩展）要么读到旧的，要么读到新的，不会读到写了一半的。
//!
//! 只用于丢了也能重建的状态（通知快照、到达回执队列、心跳学习表），所以不做 fsync：断电后最坏是
//! 回到上一版或者文件不在，比每次写都等闪存落盘便宜得多。浏览器里没有文件系统，只在原生端编译。

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// 把 `bytes` 原子地写成 `path`，父目录不存在时先建上。失败时不留临时文件。
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Err(std::io::Error::other("the target has no parent directory"));
    };
    std::fs::create_dir_all(parent)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    let temp = parent.join(format!(".{name}.{}", uuid::Uuid::new_v4().simple()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(bytes)?;
        drop(file);
        std::fs::rename(&temp, path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

/// 删掉 `path`；本来就不在不算错。
pub(crate) fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 写入盖掉旧内容，不留临时文件；删一个不存在的文件不算错。
    #[test]
    fn writes_replace_the_file_and_leave_no_temporaries() {
        let dir = std::env::temp_dir().join(format!("flare-atomic-{}", uuid::Uuid::new_v4()));
        let path = dir.join("nested").join("state.json");
        write_atomic(&path, b"{\"v\":1}").unwrap();
        write_atomic(&path, b"{\"v\":2}").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"v\":2}");
        let entries: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(entries.len(), 1, "{entries:?}");
        remove_if_present(&path).unwrap();
        remove_if_present(&path).unwrap();
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
