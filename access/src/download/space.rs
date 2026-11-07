//! 查磁盘剩余空间。查不到时返回 `None`，由调用方放行：写满时下载自己会失败。

use std::path::Path;

/// 这个目录所在卷对普通进程可用的字节数。
#[cfg(unix)]
pub fn available_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `path` 是有效的 C 字符串，`stats` 是 statvfs 要写入的缓冲区，全零是合法初值。
    let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
    let result = unsafe { libc::statvfs(path.as_ptr(), &mut stats) };
    (result == 0).then(|| (stats.f_bavail as u64).saturating_mul(stats.f_frsize as u64))
}

#[cfg(windows)]
pub fn available_bytes(path: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            directory: *const u16,
            free_to_caller: *mut u64,
            total: *mut u64,
            total_free: *mut u64,
        ) -> i32;
    }

    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut free = 0u64;
    // SAFETY: `wide` 以 0 结尾；后两个输出参数可以为空。
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut free,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    (ok != 0).then_some(free)
}

#[cfg(not(any(unix, windows)))]
pub fn available_bytes(_path: &Path) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_existing_directory_reports_space_and_a_missing_one_does_not() {
        let directory = tempfile::tempdir().unwrap();
        #[cfg(any(unix, windows))]
        assert!(available_bytes(directory.path()).is_some_and(|bytes| bytes > 0));
        assert_eq!(available_bytes(&directory.path().join("missing")), None);
    }
}
