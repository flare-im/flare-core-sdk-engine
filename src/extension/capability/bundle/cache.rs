//! The native web bundle cache: content addressed, verified before it is
//! unpacked, bounded, and pruned after the catalog stops naming a bundle.
//!
//! - A bundle lives at `{root}/{sha256}/`, its files at their bundle paths and
//!   its file list in `{root}/{sha256}/.index.json`. The digest names exact
//!   bytes, so an entry never changes: rolling back to an older plugin
//!   version finds its bundle again without a download.
//! - The `.fwb` file is downloaded from the catalog's mirror addresses in
//!   order, checked for size and SHA-256 before the archive is read
//!   ([`unpack_archive`]), and every path is checked once more against the
//!   allowlist and the cache directory just before it is written.
//! - Unpacking goes to a temporary directory renamed into place, so a
//!   half-written bundle is never served.
//! - At most [`BUNDLE_CACHE_MAX_BYTES`] of unpacked files are kept; the least
//!   recently used go first, never one an open page uses.
//! - After a successful catalog read, a bundle no catalog entry names any more
//!   is deleted [`UNREFERENCED_GRACE_MS`] later ([`BundleCache::mark_referenced`]).
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use flare_plugin_protocol::bundle::{
    BundleError, BundleFile, BundleManifest, MAX_WEB_BUNDLE_BYTES, WebBundleDecl, is_bundle_path,
    is_https_url, is_sha256_hex, unpack_archive,
};
use serde::{Deserialize, Serialize};

/// Unpacked bytes the cache keeps (design 8.3).
pub const BUNDLE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// How long a bundle the catalog no longer names is kept.
pub const UNREFERENCED_GRACE_MS: i64 = 24 * 60 * 60 * 1000;
const INDEX_FILE: &str = ".index.json";
const STATE_FILE: &str = "cache.json";

/// Why a bundle could not be made available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheError {
    /// No mirror answered with the bytes (network, status, size cap).
    Unavailable,
    /// The bytes did not pass the archive checks; the code is
    /// [`BundleError::code`]. The digest mismatch is `digest_mismatch`.
    Rejected(&'static str),
    /// The archive's `bundle.json` names another bundle or bridge.
    Mismatch,
    /// The cache directory could not be written.
    Storage,
}

impl From<BundleError> for CacheError {
    fn from(error: BundleError) -> Self {
        Self::Rejected(error.code())
    }
}

/// A bundle ready to serve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CachedBundle {
    pub sha256: String,
    /// The directory holding the files at their bundle paths.
    pub root: PathBuf,
    pub manifest: BundleManifest,
    /// Every file, `bundle.json` included, with its size, digest and content type.
    pub files: Vec<BundleFile>,
    pub unpacked_size: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Entry {
    unpacked_size: u64,
    last_used_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    unreferenced_since_ms: Option<i64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CacheState {
    entries: BTreeMap<String, Entry>,
}

/// Fetches the bytes of a `.fwb` address. Implemented over HTTPS for the app
/// ([`HttpBundleFetcher`]) and in memory by tests.
#[async_trait::async_trait]
pub trait BundleFetcher: Send + Sync {
    /// At most `max_bytes`; more is `Rejected("size_mismatch")`.
    async fn fetch(&self, url: &str, max_bytes: u64) -> Result<Vec<u8>, CacheError>;
}

pub struct BundleCache {
    root: PathBuf,
    state: Mutex<Option<CacheState>>,
    /// Bundles an open page serves from: never evicted or pruned.
    pinned: Mutex<HashMap<String, usize>>,
    /// One download per digest at a time.
    downloads: tokio::sync::Mutex<()>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl BundleCache {
    pub fn new(root: PathBuf) -> Self {
        Self::with_clock(root, Arc::new(now_ms))
    }

    pub fn with_clock(root: PathBuf, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        Self {
            root,
            state: Mutex::new(None),
            pinned: Mutex::new(HashMap::new()),
            downloads: tokio::sync::Mutex::new(()),
            clock,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The bundle of `sha256` if it is in the cache, marked used.
    pub fn lookup(&self, sha256: &str) -> Option<CachedBundle> {
        if !is_sha256_hex(sha256) {
            return None;
        }
        let dir = self.root.join(sha256);
        let raw = std::fs::read(dir.join(INDEX_FILE)).ok()?;
        let mut cached: CachedBundle = serde_json::from_slice(&raw).ok()?;
        if cached.sha256 != sha256 {
            return None;
        }
        cached.root = dir;
        let now = (self.clock)();
        self.with_state(|state| {
            let entry = state.entries.entry(sha256.to_owned()).or_default();
            entry.unpacked_size = cached.unpacked_size;
            entry.last_used_ms = now;
        });
        Some(cached)
    }

    /// The bundle of `decl`, from the cache or downloaded from its mirror
    /// addresses in order, checked and unpacked.
    pub async fn ensure(
        &self,
        decl: &WebBundleDecl,
        fetcher: &dyn BundleFetcher,
    ) -> Result<CachedBundle, CacheError> {
        if let Some(cached) = self.lookup(&decl.sha256) {
            return Ok(cached);
        }
        let _one = self.downloads.lock().await;
        if let Some(cached) = self.lookup(&decl.sha256) {
            return Ok(cached);
        }
        let mut last = CacheError::Unavailable;
        for url in decl.urls.iter().filter(|url| is_https_url(url)) {
            let bytes = match fetcher
                .fetch(url, decl.size.min(MAX_WEB_BUNDLE_BYTES))
                .await
            {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::debug!(target: "flare_sdk.plugin", %url, ?error, "web bundle mirror failed");
                    // A refusal seen on an earlier mirror says more than a
                    // later mirror being unreachable.
                    if !matches!(last, CacheError::Rejected(_)) {
                        last = error;
                    }
                    continue;
                }
            };
            match self.install(decl, &bytes) {
                Ok(cached) => return Ok(cached),
                // Bytes that fail the digest may be a broken mirror: try the next.
                Err(CacheError::Rejected(code)) => {
                    tracing::warn!(target: "flare_sdk.plugin", %url, code, "web bundle refused");
                    last = CacheError::Rejected(code);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last)
    }

    /// Checks `bytes` against `decl` (size, then digest, then the archive) and
    /// unpacks them into the cache.
    pub fn install(&self, decl: &WebBundleDecl, bytes: &[u8]) -> Result<CachedBundle, CacheError> {
        let unpacked = unpack_archive(bytes, &decl.sha256, decl.size)?;
        if unpacked.manifest.id != decl.id || unpacked.manifest.bridge != decl.bridge {
            return Err(CacheError::Mismatch);
        }
        std::fs::create_dir_all(&self.root).map_err(|_| CacheError::Storage)?;
        let target = self.root.join(&decl.sha256);
        let staging = self.root.join(format!(
            ".staging-{}-{}",
            decl.sha256,
            uuid::Uuid::new_v4().simple()
        ));
        let written = (|| {
            std::fs::create_dir_all(&staging).map_err(|_| CacheError::Storage)?;
            for (path, content) in &unpacked.files {
                let file = contained_path(&staging, path).ok_or(CacheError::Rejected(
                    BundleError::InvalidPath(path.clone()).code(),
                ))?;
                if let Some(parent) = file.parent() {
                    std::fs::create_dir_all(parent).map_err(|_| CacheError::Storage)?;
                }
                std::fs::write(&file, content).map_err(|_| CacheError::Storage)?;
            }
            let cached = CachedBundle {
                sha256: decl.sha256.clone(),
                root: target.clone(),
                manifest: unpacked.manifest.clone(),
                files: unpacked.index.clone(),
                unpacked_size: unpacked.unpacked_size,
            };
            let index = serde_json::to_vec(&cached).map_err(|_| CacheError::Storage)?;
            std::fs::write(staging.join(INDEX_FILE), index).map_err(|_| CacheError::Storage)?;
            Ok(cached)
        })();
        let cached = match written {
            Ok(cached) => cached,
            Err(error) => {
                let _ = std::fs::remove_dir_all(&staging);
                return Err(error);
            }
        };
        if std::fs::rename(&staging, &target).is_err() {
            // Another process put the same digest there first: same bytes.
            let _ = std::fs::remove_dir_all(&staging);
            if !target.join(INDEX_FILE).is_file() {
                return Err(CacheError::Storage);
            }
        }
        let now = (self.clock)();
        self.with_state(|state| {
            state.entries.insert(
                decl.sha256.clone(),
                Entry {
                    unpacked_size: cached.unpacked_size,
                    last_used_ms: now,
                    unreferenced_since_ms: None,
                },
            );
        });
        self.evict(Some(&decl.sha256));
        Ok(cached)
    }

    /// Keep `sha256` from eviction and pruning while a page uses it.
    pub fn pin(&self, sha256: &str) {
        *self
            .pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(sha256.to_owned())
            .or_default() += 1;
    }

    pub fn unpin(&self, sha256: &str) {
        let mut pinned = self.pinned.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = pinned.get_mut(sha256) {
            *count -= 1;
            if *count == 0 {
                pinned.remove(sha256);
            }
        }
    }

    fn is_pinned(&self, sha256: &str) -> bool {
        self.pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(sha256)
    }

    /// After a successful catalog read: `referenced` are the digests some
    /// catalog entry names. Others start their grace period; one past it is
    /// deleted. A bundle named again keeps living.
    pub fn mark_referenced(&self, referenced: &BTreeSet<String>) {
        let now = (self.clock)();
        let expired: Vec<String> = self.with_state(|state| {
            let mut expired = Vec::new();
            for (sha256, entry) in state.entries.iter_mut() {
                if referenced.contains(sha256) {
                    entry.unreferenced_since_ms = None;
                    continue;
                }
                let since = *entry.unreferenced_since_ms.get_or_insert(now);
                if now.saturating_sub(since) >= UNREFERENCED_GRACE_MS {
                    expired.push(sha256.clone());
                }
            }
            expired
        });
        for sha256 in expired {
            if !self.is_pinned(&sha256) {
                self.remove(&sha256);
            }
        }
    }

    /// Unpacked bytes held now.
    pub fn used_bytes(&self) -> u64 {
        self.with_state(|state| state.entries.values().map(|e| e.unpacked_size).sum())
    }

    /// Digests held now.
    pub fn digests(&self) -> BTreeSet<String> {
        self.with_state(|state| state.entries.keys().cloned().collect())
    }

    fn evict(&self, keep: Option<&str>) {
        loop {
            let victim = self.with_state(|state| {
                let used: u64 = state.entries.values().map(|e| e.unpacked_size).sum();
                if used <= BUNDLE_CACHE_MAX_BYTES {
                    return None;
                }
                state
                    .entries
                    .iter()
                    .filter(|(sha256, _)| Some(sha256.as_str()) != keep)
                    .filter(|(sha256, _)| !self.is_pinned(sha256))
                    .min_by_key(|(_, entry)| entry.last_used_ms)
                    .map(|(sha256, _)| sha256.clone())
            });
            match victim {
                Some(sha256) => self.remove(&sha256),
                None => return,
            }
        }
    }

    fn remove(&self, sha256: &str) {
        let _ = std::fs::remove_dir_all(self.root.join(sha256));
        self.with_state(|state| {
            state.entries.remove(sha256);
        });
    }

    /// Runs `f` on the state (loaded on first use) and saves it.
    fn with_state<T>(&self, f: impl FnOnce(&mut CacheState) -> T) -> T {
        let mut guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let state = guard.get_or_insert_with(|| self.load());
        let out = f(state);
        if let Ok(raw) = serde_json::to_vec(state) {
            let _ = std::fs::create_dir_all(&self.root);
            let _ = std::fs::write(self.root.join(STATE_FILE), raw);
        }
        out
    }

    /// The saved state, joined with what is on disk: a directory without an
    /// index is a leftover and goes, an index without a state entry is
    /// adopted, a state entry without a directory is dropped.
    fn load(&self) -> CacheState {
        let saved: CacheState = std::fs::read(self.root.join(STATE_FILE))
            .ok()
            .and_then(|raw| serde_json::from_slice(&raw).ok())
            .unwrap_or_default();
        let mut state = CacheState::default();
        let Ok(dirs) = std::fs::read_dir(&self.root) else {
            return state;
        };
        for dir in dirs.flatten() {
            let name = dir.file_name().to_string_lossy().into_owned();
            if !dir.path().is_dir() {
                continue;
            }
            if !is_sha256_hex(&name) {
                // Staging directories of an interrupted install.
                let _ = std::fs::remove_dir_all(dir.path());
                continue;
            }
            let index = std::fs::read(dir.path().join(INDEX_FILE))
                .ok()
                .and_then(|raw| serde_json::from_slice::<CachedBundle>(&raw).ok())
                .filter(|cached| cached.sha256 == name);
            match index {
                Some(cached) => {
                    let mut entry = saved.entries.get(&name).cloned().unwrap_or_default();
                    entry.unpacked_size = cached.unpacked_size;
                    state.entries.insert(name, entry);
                }
                None => {
                    let _ = std::fs::remove_dir_all(dir.path());
                }
            }
        }
        state
    }
}

/// `path` under `dir`, or `None` when the path is not a bundle path or would
/// leave `dir`: the second check before a file is written.
pub fn contained_path(dir: &Path, path: &str) -> Option<PathBuf> {
    if !is_bundle_path(path) {
        return None;
    }
    let relative = Path::new(path);
    if !relative
        .components()
        .all(|component| matches!(component, Component::Normal(_)))
    {
        return None;
    }
    let joined = dir.join(relative);
    joined.starts_with(dir).then_some(joined)
}

/// Downloads over HTTPS: no cookies, no credentials, redirects limited,
/// stopped past the declared size.
pub struct HttpBundleFetcher {
    client: reqwest::Client,
}

impl HttpBundleFetcher {
    pub fn new() -> Option<Self> {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(5))
            .connect_timeout(std::time::Duration::from_secs(10))
            .read_timeout(std::time::Duration::from_secs(30))
            .https_only(true)
            .build()
            .ok()
            .map(|client| Self { client })
    }
}

#[async_trait::async_trait]
impl BundleFetcher for HttpBundleFetcher {
    async fn fetch(&self, url: &str, max_bytes: u64) -> Result<Vec<u8>, CacheError> {
        use futures_util::StreamExt;
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|_| CacheError::Unavailable)?;
        if !response.status().is_success() {
            return Err(CacheError::Unavailable);
        }
        // More bytes than the catalog declared are not this bundle.
        if response.content_length().is_some_and(|len| len > max_bytes) {
            return Err(CacheError::Rejected("size_mismatch"));
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| CacheError::Unavailable)?;
            if body.len() as u64 + chunk.len() as u64 > max_bytes {
                return Err(CacheError::Rejected("size_mismatch"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
