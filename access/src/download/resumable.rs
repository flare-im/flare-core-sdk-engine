//! 可续传下载。一次只下载一个文件，调用方负责不并发。
//!
//! 半成品留在 `{file}.part`：断网、换地址、杀进程之后都从它接着下，已有内容先过一遍哈希，
//! 这样最终校验的是整份文件，不依赖上次运行的内存状态。

use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

use super::space::available_bytes;
use super::verify::{VerifiedArtifact, file_sha256, hex};
use crate::release::DownloadSpec;
use crate::release::codes;

/// 进度事件的最小间隔，避免刷 UI。
pub const MIN_PROGRESS_INTERVAL_MS: i64 = 200;
/// 开始前要求的剩余空间：包大小的这么多倍（下载一份，安装时系统还要一份）。
pub const SPACE_FACTOR: u64 = 2;
pub(super) const READ_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("host unreachable")]
    Unreachable,
    #[error("request timed out")]
    Timeout,
    #[error("TLS verification failed")]
    Tls,
    #[error("server answered {0}")]
    Http(u16),
    /// 连接中途断了，或服务端不能从要求的位置接着发。
    #[error("transfer was interrupted")]
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DownloadError {
    #[error("not enough disk space: {available} bytes free, {required} required")]
    InsufficientSpace { required: u64, available: u64 },
    #[error("every download address failed: {0:?}")]
    Failed(Option<TransportError>),
    #[error("downloaded file does not match the manifest hash")]
    HashMismatch,
    #[error("local file error: {0}")]
    Io(String),
}

impl DownloadError {
    /// 跨端稳定的错误码。
    pub fn code(&self) -> &'static str {
        match self {
            DownloadError::InsufficientSpace { .. } => codes::INSUFFICIENT_SPACE,
            DownloadError::Failed(_) | DownloadError::Io(_) => codes::DOWNLOAD_FAILED,
            DownloadError::HashMismatch => codes::HASH_MISMATCH,
        }
    }
}

fn local(error: io::Error) -> DownloadError {
    DownloadError::Io(error.to_string())
}

/// 下载用的 HTTP 能力，由引擎注入。与 [`HttpFetch`](crate::bootstrap::HttpFetch) 分开：
/// 那边是限长读进内存的小文档，这里是几十 MB 的流，不能有总时限。
#[async_trait::async_trait]
pub trait ArtifactTransport: Send + Sync {
    /// 从 `offset` 开始请求。非 2xx 返回 [`TransportError::Http`]。
    async fn open(&self, url: &str, offset: u64)
    -> Result<Box<dyn ArtifactStream>, TransportError>;
}

#[async_trait::async_trait]
pub trait ArtifactStream: Send {
    /// 响应实际从哪里开始：服务端不支持 Range 时是 0。
    fn start(&self) -> u64;

    /// 下一块，结束时返回 `None`。
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, TransportError>;
}

/// 下载过程的通知。
pub trait DownloadObserver: Send {
    fn progress(&mut self, received: u64, total: u64);

    /// 字节收齐，开始校验。
    fn verifying(&mut self);
}

/// 进度节流。
#[derive(Debug, Clone, Default)]
pub struct ProgressGate {
    last_ms: Option<i64>,
}

impl ProgressGate {
    pub fn allow(&mut self, now_ms: i64) -> bool {
        if self
            .last_ms
            .is_some_and(|last| now_ms - last < MIN_PROGRESS_INTERVAL_MS)
        {
            return false;
        }
        self.last_ms = Some(now_ms);
        true
    }
}

/// 半成品文件：写入的同时累积哈希。
struct Partial {
    file: File,
    hasher: Sha256,
    received: u64,
}

impl Partial {
    /// 打开半成品，已有内容进哈希。比目标还长的文件不可能是这个版本的，从头来。
    async fn open(path: &Path, size_bytes: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .await?;
        let received = file.metadata().await?.len();
        let mut partial = Self {
            file,
            hasher: Sha256::new(),
            received,
        };
        if received > size_bytes {
            partial.restart().await?;
            return Ok(partial);
        }
        // 读到文件尾，之后的写入正好接在后面。
        let mut buffer = vec![0u8; READ_CHUNK_BYTES];
        loop {
            let read = partial.file.read(&mut buffer).await?;
            if read == 0 {
                return Ok(partial);
            }
            partial.hasher.update(&buffer[..read]);
        }
    }

    async fn restart(&mut self) -> io::Result<()> {
        self.file.set_len(0).await?;
        self.file.seek(SeekFrom::Start(0)).await?;
        self.hasher = Sha256::new();
        self.received = 0;
        Ok(())
    }

    async fn append(&mut self, chunk: &[u8]) -> io::Result<()> {
        self.file.write_all(chunk).await?;
        self.hasher.update(chunk);
        self.received += chunk.len() as u64;
        Ok(())
    }

    /// 落盘并给出整份文件的哈希。
    async fn finish(mut self) -> io::Result<String> {
        self.file.flush().await?;
        self.file.sync_all().await?;
        Ok(hex(&self.hasher.finalize()))
    }
}

fn ensure_space(directory: &Path, size_bytes: u64, received: u64) -> Result<(), DownloadError> {
    let Some(available) = available_bytes(directory) else {
        return Ok(());
    };
    let required = size_bytes.saturating_mul(SPACE_FACTOR);
    // 半成品已经占着盘了，算进已有空间。
    if available.saturating_add(received) < required {
        return Err(DownloadError::InsufficientSpace {
            required,
            available,
        });
    }
    Ok(())
}

struct Job<'a> {
    total: u64,
    gate: ProgressGate,
    now_ms: &'a (dyn Fn() -> i64 + Send + Sync),
    observer: &'a mut dyn DownloadObserver,
}

impl Job<'_> {
    /// 问一个地址，接着往半成品里写。收齐返回 `Ok`。
    async fn pump(
        &mut self,
        transport: &dyn ArtifactTransport,
        url: &str,
        partial: &mut Partial,
    ) -> Result<(), DownloadError> {
        let attempt = |error| DownloadError::Failed(Some(error));
        let mut stream = transport
            .open(url, partial.received)
            .await
            .map_err(attempt)?;
        if stream.start() != partial.received {
            // 服务端不支持 Range：只能从头收。其它起点对不上手里的内容，换地址。
            if stream.start() != 0 {
                return Err(attempt(TransportError::Interrupted));
            }
            partial.restart().await.map_err(local)?;
        }
        while let Some(chunk) = stream.next_chunk().await.map_err(attempt)? {
            if partial.received + chunk.len() as u64 > self.total {
                // 比清单说的长：这个地址上不是这个文件。
                partial.restart().await.map_err(local)?;
                return Err(DownloadError::HashMismatch);
            }
            partial.append(&chunk).await.map_err(local)?;
            if self.gate.allow((self.now_ms)()) {
                self.observer.progress(partial.received, self.total);
            }
        }
        if partial.received == self.total {
            Ok(())
        } else {
            Err(attempt(TransportError::Interrupted))
        }
    }
}

/// 上次已经下完的文件：校验通过就不用再下。
async fn finished_before(destination: &Path, spec: &DownloadSpec) -> Option<VerifiedArtifact> {
    let actual = file_sha256(destination).await.ok()?;
    (actual == spec.sha256).then(|| VerifiedArtifact::new(destination.to_path_buf(), spec))
}

/// 下载 `spec` 到 `directory`，校验通过后返回 [`VerifiedArtifact`]。
///
/// 地址按顺序轮换：一个地址有进展就重新计数，所有地址连续失败一轮才放弃。
pub async fn download(
    transport: &dyn ArtifactTransport,
    spec: &DownloadSpec,
    directory: &Path,
    now_ms: &(dyn Fn() -> i64 + Send + Sync),
    observer: &mut dyn DownloadObserver,
) -> Result<VerifiedArtifact, DownloadError> {
    fs::create_dir_all(directory).await.map_err(local)?;
    let destination = directory.join(&spec.file_name);
    if let Some(artifact) = finished_before(&destination, spec).await {
        return Ok(artifact);
    }
    let partial_path: PathBuf = directory.join(format!("{}.part", spec.file_name));
    let mut partial = Partial::open(&partial_path, spec.size_bytes)
        .await
        .map_err(local)?;
    ensure_space(directory, spec.size_bytes, partial.received)?;

    let mut job = Job {
        total: spec.size_bytes,
        gate: ProgressGate::default(),
        now_ms,
        observer,
    };
    let mut failures = 0;
    let mut next = 0;
    let mut last_error = None;
    while partial.received < spec.size_bytes {
        if failures >= spec.urls.len() {
            // 半成品要留给下次续传：写缓冲落到文件里再放手。
            partial.file.flush().await.map_err(local)?;
            return Err(DownloadError::Failed(last_error));
        }
        let url = &spec.urls[next % spec.urls.len()];
        next += 1;
        let before = partial.received;
        match job.pump(transport, url, &mut partial).await {
            Ok(()) => {}
            Err(DownloadError::Failed(error)) => {
                last_error = error;
                failures = if partial.received > before {
                    0
                } else {
                    failures + 1
                };
            }
            Err(error) => {
                drop(partial);
                let _ = fs::remove_file(&partial_path).await;
                return Err(error);
            }
        }
    }
    job.observer.progress(partial.received, spec.size_bytes);
    job.observer.verifying();
    let actual = partial.finish().await.map_err(local)?;
    if actual != spec.sha256 {
        // 内容不对，留着只会让下次续传接着错。
        let _ = fs::remove_file(&partial_path).await;
        return Err(DownloadError::HashMismatch);
    }
    fs::rename(&partial_path, &destination)
        .await
        .map_err(local)?;
    Ok(VerifiedArtifact::new(destination, spec))
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::*;
    use crate::download::verify_existing;

    const URL_A: &str = "https://dl-a.example.com/a.apk";
    const URL_B: &str = "https://dl-b.example.net/a.apk";

    fn digest(bytes: &[u8]) -> String {
        hex(&Sha256::digest(bytes))
    }

    fn spec(content: &[u8]) -> DownloadSpec {
        DownloadSpec {
            urls: vec![URL_A.into(), URL_B.into()],
            sha256: digest(content),
            size_bytes: content.len() as u64,
            signing_cert_sha256: Some("cert".into()),
            build: 20400,
            file_name: "2.4.0-20400.apk".into(),
        }
    }

    #[derive(Clone)]
    enum Serve {
        /// 支持 Range，发到 `limit` 为止，之后断开。
        Range {
            limit: usize,
        },
        /// 忽略 Range，从头发。
        Whole,
        Down,
    }

    #[derive(Default)]
    struct FakeCdn {
        content: Vec<u8>,
        plans: Mutex<HashMap<String, VecDeque<Serve>>>,
        opened: Mutex<Vec<(String, u64)>>,
    }

    impl FakeCdn {
        fn new(content: &[u8]) -> Self {
            Self {
                content: content.to_vec(),
                ..Self::default()
            }
        }

        fn plan(&self, url: &str, serves: Vec<Serve>) {
            self.plans
                .lock()
                .unwrap()
                .insert(url.to_owned(), serves.into());
        }
    }

    struct FakeStream {
        start: u64,
        chunks: VecDeque<Vec<u8>>,
        interrupted: bool,
    }

    #[async_trait::async_trait]
    impl ArtifactStream for FakeStream {
        fn start(&self) -> u64 {
            self.start
        }

        async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            match self.chunks.pop_front() {
                Some(chunk) => Ok(Some(chunk)),
                None if self.interrupted => Err(TransportError::Interrupted),
                None => Ok(None),
            }
        }
    }

    #[async_trait::async_trait]
    impl ArtifactTransport for FakeCdn {
        async fn open(
            &self,
            url: &str,
            offset: u64,
        ) -> Result<Box<dyn ArtifactStream>, TransportError> {
            self.opened.lock().unwrap().push((url.to_owned(), offset));
            let serve = self
                .plans
                .lock()
                .unwrap()
                .get_mut(url)
                .and_then(VecDeque::pop_front)
                .unwrap_or(Serve::Range {
                    limit: self.content.len(),
                });
            let (start, end, interrupted) = match serve {
                Serve::Down => return Err(TransportError::Unreachable),
                Serve::Whole => (0, self.content.len(), false),
                Serve::Range { limit } => (offset as usize, limit, limit < self.content.len()),
            };
            Ok(Box::new(FakeStream {
                start: start as u64,
                chunks: self.content[start..end]
                    .chunks(4)
                    .map(<[u8]>::to_vec)
                    .collect(),
                interrupted,
            }))
        }
    }

    #[derive(Default)]
    struct Recorder {
        progress: Vec<(u64, u64)>,
        verifying: usize,
    }

    impl DownloadObserver for Recorder {
        fn progress(&mut self, received: u64, total: u64) {
            self.progress.push((received, total));
        }

        fn verifying(&mut self) {
            self.verifying += 1;
        }
    }

    async fn run(
        cdn: &FakeCdn,
        spec: &DownloadSpec,
        directory: &Path,
    ) -> (Result<VerifiedArtifact, DownloadError>, Recorder) {
        let clock = AtomicI64::new(0);
        let now = || clock.fetch_add(50, Ordering::SeqCst);
        let mut recorder = Recorder::default();
        let result = download(cdn, spec, directory, &now, &mut recorder).await;
        (result, recorder)
    }

    #[tokio::test]
    async fn a_clean_download_is_verified_and_replans_as_an_install() {
        let directory = tempfile::tempdir().unwrap();
        let content = b"0123456789abcdef";
        let cdn = FakeCdn::new(content);
        let spec = spec(content);
        let (result, recorder) = run(&cdn, &spec, directory.path()).await;
        let artifact = result.unwrap();
        assert_eq!(std::fs::read(artifact.path()).unwrap(), content);
        assert_eq!(recorder.verifying, 1);
        assert_eq!(recorder.progress.last(), Some(&(16, 16)));
        assert!(!directory.path().join("2.4.0-20400.apk.part").exists());

        let reread = verify_existing(&artifact.record()).await.unwrap();
        assert_eq!(reread, artifact);
        let plan = serde_json::to_value(artifact.install_plan()).unwrap();
        assert_eq!(plan["kind"], "installPackage");
        assert_eq!(plan["build"], 20400);
        assert_eq!(plan["signingCertSha256"], "cert");
    }

    #[tokio::test]
    async fn a_dropped_transfer_resumes_on_the_next_address_from_where_it_stopped() {
        let directory = tempfile::tempdir().unwrap();
        let content = b"0123456789abcdef";
        let cdn = FakeCdn::new(content);
        cdn.plan(URL_A, vec![Serve::Range { limit: 8 }]);
        let spec = spec(content);
        let (result, _) = run(&cdn, &spec, directory.path()).await;
        assert!(result.is_ok());
        assert_eq!(
            *cdn.opened.lock().unwrap(),
            vec![(URL_A.to_owned(), 0), (URL_B.to_owned(), 8)]
        );
    }

    #[tokio::test]
    async fn a_new_run_continues_the_partial_file_of_the_last_one() {
        let directory = tempfile::tempdir().unwrap();
        let content = b"0123456789abcdef";
        let cdn = FakeCdn::new(content);
        std::fs::write(directory.path().join("2.4.0-20400.apk.part"), &content[..8]).unwrap();
        let spec = spec(content);
        let (result, _) = run(&cdn, &spec, directory.path()).await;
        assert!(result.is_ok());
        assert_eq!(*cdn.opened.lock().unwrap(), vec![(URL_A.to_owned(), 8)]);
    }

    #[tokio::test]
    async fn a_server_without_range_support_restarts_the_file() {
        let directory = tempfile::tempdir().unwrap();
        let content = b"0123456789abcdef";
        let cdn = FakeCdn::new(content);
        cdn.plan(URL_A, vec![Serve::Whole]);
        std::fs::write(directory.path().join("2.4.0-20400.apk.part"), &content[..8]).unwrap();
        let spec = spec(content);
        let (result, _) = run(&cdn, &spec, directory.path()).await;
        assert_eq!(
            std::fs::read(result.unwrap().path()).unwrap(),
            content.to_vec()
        );
    }

    #[tokio::test]
    async fn tampered_content_is_rejected_and_leaves_nothing_behind() {
        let directory = tempfile::tempdir().unwrap();
        let cdn = FakeCdn::new(b"0123456789abcdeX");
        let spec = spec(b"0123456789abcdef");
        let (result, _) = run(&cdn, &spec, directory.path()).await;
        assert_eq!(result.unwrap_err(), DownloadError::HashMismatch);
        assert!(!directory.path().join("2.4.0-20400.apk").exists());
        assert!(!directory.path().join("2.4.0-20400.apk.part").exists());
    }

    #[tokio::test]
    async fn giving_up_needs_a_full_round_of_failures_and_keeps_the_partial_file() {
        let directory = tempfile::tempdir().unwrap();
        let content = b"0123456789abcdef";
        let cdn = FakeCdn::new(content);
        cdn.plan(URL_A, vec![Serve::Range { limit: 4 }, Serve::Down]);
        cdn.plan(URL_B, vec![Serve::Down]);
        let spec = spec(content);
        let (result, _) = run(&cdn, &spec, directory.path()).await;
        assert_eq!(
            result.unwrap_err(),
            DownloadError::Failed(Some(TransportError::Unreachable))
        );
        assert_eq!(
            std::fs::read(directory.path().join("2.4.0-20400.apk.part")).unwrap(),
            content[..4].to_vec()
        );
    }

    #[tokio::test]
    async fn a_file_finished_earlier_is_reused_and_a_tampered_one_is_not_trusted() {
        let directory = tempfile::tempdir().unwrap();
        let content = b"0123456789abcdef";
        let cdn = FakeCdn::new(content);
        let spec = spec(content);
        let artifact = run(&cdn, &spec, directory.path()).await.0.unwrap();
        cdn.opened.lock().unwrap().clear();
        run(&cdn, &spec, directory.path()).await.0.unwrap();
        assert!(cdn.opened.lock().unwrap().is_empty());

        std::fs::write(artifact.path(), b"tampered").unwrap();
        assert_eq!(
            verify_existing(&artifact.record()).await.unwrap_err(),
            DownloadError::HashMismatch
        );
    }

    #[test]
    fn progress_is_throttled_to_two_hundred_milliseconds() {
        let mut gate = ProgressGate::default();
        assert!(gate.allow(0));
        assert!(!gate.allow(199));
        assert!(gate.allow(200));
        assert!(!gate.allow(399));
    }
}
