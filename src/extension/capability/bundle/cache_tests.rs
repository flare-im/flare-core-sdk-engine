use super::*;
use flare_plugin_protocol::bundle::{sha256_hex, write_archive};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

fn manifest(id: &str) -> BundleManifest {
    BundleManifest {
        id: id.into(),
        entry: "index.html".into(),
        bridge: 1,
        permissions: Default::default(),
        operations: Default::default(),
        capabilities: Default::default(),
        locales: vec!["en".into()],
    }
}

/// A bundle of `id` whose script holds `filler` bytes.
fn archive(id: &str, filler: usize) -> Vec<u8> {
    let mut files = BTreeMap::new();
    files.insert(
        "index.html".to_owned(),
        b"<!doctype html><script src=\"assets/app.js\"></script>".to_vec(),
    );
    files.insert("assets/app.js".to_owned(), vec![b'x'; filler.max(1)]);
    write_archive(&manifest(id), &files).unwrap()
}

fn decl(id: &str, bytes: &[u8], urls: &[&str]) -> WebBundleDecl {
    WebBundleDecl {
        id: id.into(),
        sha256: sha256_hex(bytes),
        size: bytes.len() as u64,
        bridge: 1,
        digital_goods: false,
        urls: urls.iter().map(|url| (*url).to_owned()).collect(),
        files: None,
        permissions: Default::default(),
    }
}

/// Mirrors by address; counts fetches.
struct Mirrors {
    by_url: HashMap<String, Vec<u8>>,
    fetches: AtomicUsize,
}

#[async_trait::async_trait]
impl BundleFetcher for Mirrors {
    async fn fetch(&self, url: &str, max_bytes: u64) -> Result<Vec<u8>, CacheError> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        match self.by_url.get(url) {
            Some(bytes) if bytes.len() as u64 <= max_bytes => Ok(bytes.clone()),
            _ => Err(CacheError::Unavailable),
        }
    }
}

fn mirrors(entries: &[(&str, &[u8])]) -> Mirrors {
    Mirrors {
        by_url: entries
            .iter()
            .map(|(url, bytes)| ((*url).to_owned(), bytes.to_vec()))
            .collect(),
        fetches: AtomicUsize::new(0),
    }
}

fn temp_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "flare-bundle-cache-{name}-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let _ = std::fs::remove_dir_all(&root);
    root
}

#[tokio::test]
async fn a_bundle_is_verified_unpacked_and_then_served_from_the_cache() {
    let root = temp_root("serve");
    let cache = BundleCache::new(root.clone());
    let bytes = archive("board", 64);
    let decl = decl("board", &bytes, &["https://m1.example.com/board.fwb"]);
    let fetcher = mirrors(&[("https://m1.example.com/board.fwb", &bytes)]);
    let cached = cache.ensure(&decl, &fetcher).await.unwrap();
    assert_eq!(cached.root, root.join(&decl.sha256));
    assert!(cached.root.join("index.html").is_file());
    assert!(cached.root.join("assets/app.js").is_file());
    assert!(cached.files.iter().any(|file| file.path == "bundle.json"));
    // Again: from the cache, no download.
    let again = cache.ensure(&decl, &fetcher).await.unwrap();
    assert_eq!(again, cached);
    assert_eq!(fetcher.fetches.load(Ordering::SeqCst), 1);
    // A new process finds it on disk.
    let restarted = BundleCache::new(root.clone());
    assert_eq!(restarted.lookup(&decl.sha256).unwrap().manifest.id, "board");
    let _ = std::fs::remove_dir_all(root);
}

/// Review Focus 6: the digest the catalog carries is the one checked, before
/// anything is unpacked; a mirror with other bytes is skipped for the next.
#[tokio::test]
async fn bytes_that_do_not_match_the_digest_are_refused_before_unpacking() {
    let root = temp_root("digest");
    let cache = BundleCache::new(root.clone());
    let genuine = archive("board", 64);
    let tampered = archive("board", 65);
    let decl = decl(
        "board",
        &genuine,
        &[
            "https://bad.example.com/b.fwb",
            "https://good.example.com/b.fwb",
        ],
    );
    let only_bad = mirrors(&[("https://bad.example.com/b.fwb", &tampered)]);
    let error = cache.ensure(&decl, &only_bad).await.unwrap_err();
    assert!(matches!(
        error,
        CacheError::Rejected("size_mismatch" | "digest_mismatch")
    ));
    assert!(!root.join(&decl.sha256).exists(), "nothing was unpacked");
    // Same size, other bytes: the digest refuses it.
    let mut flipped = genuine.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 1;
    assert_eq!(
        cache.install(&decl, &flipped).unwrap_err(),
        CacheError::Rejected("digest_mismatch")
    );
    // Failover to the next mirror.
    let both = mirrors(&[
        ("https://bad.example.com/b.fwb", &flipped),
        ("https://good.example.com/b.fwb", &genuine),
    ]);
    assert!(cache.ensure(&decl, &both).await.is_ok());
    assert_eq!(both.fetches.load(Ordering::SeqCst), 2);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn a_bundle_naming_another_id_is_refused() {
    let root = temp_root("mismatch");
    let cache = BundleCache::new(root.clone());
    let bytes = archive("other", 8);
    assert_eq!(
        cache
            .install(&decl("board", &bytes, &[]), &bytes)
            .unwrap_err(),
        CacheError::Mismatch
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn paths_are_checked_again_before_writing() {
    let dir = Path::new("/cache/abc");
    assert_eq!(
        contained_path(dir, "assets/app.js"),
        Some(dir.join("assets/app.js"))
    );
    for hostile in [
        "../escape.js",
        "assets/../../x.js",
        "/etc/passwd",
        "assets\\x.js",
        "Assets/x.js",
        ".hidden",
        "",
    ] {
        assert_eq!(contained_path(dir, hostile), None, "{hostile}");
    }
}

/// Rollback: the tenant goes back to an older plugin version; its bundle's
/// digest is still in the cache, so it opens without a download.
#[tokio::test]
async fn rolling_back_finds_the_older_bundle_in_the_cache() {
    let root = temp_root("rollback");
    let cache = BundleCache::new(root.clone());
    let v1 = archive("board", 10);
    let v2 = archive("board", 20);
    let d1 = decl("board", &v1, &["https://m.example.com/v1.fwb"]);
    let d2 = decl("board", &v2, &["https://m.example.com/v2.fwb"]);
    let fetcher = mirrors(&[
        ("https://m.example.com/v1.fwb", &v1),
        ("https://m.example.com/v2.fwb", &v2),
    ]);
    cache.ensure(&d1, &fetcher).await.unwrap();
    cache.ensure(&d2, &fetcher).await.unwrap();
    // The catalog of v2 no longer names v1, but within the grace period it stays.
    cache.mark_referenced(&[d2.sha256.clone()].into());
    let back = cache.ensure(&d1, &fetcher).await.unwrap();
    assert_eq!(back.sha256, d1.sha256);
    assert_eq!(fetcher.fetches.load(Ordering::SeqCst), 2, "no new download");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn unreferenced_bundles_go_after_the_grace_period_unless_a_page_uses_them() {
    let root = temp_root("prune");
    let now = Arc::new(AtomicI64::new(1_000));
    let clock = {
        let now = now.clone();
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let cache = BundleCache::with_clock(root.clone(), clock);
    let a = archive("a", 8);
    let b = archive("b", 8);
    let (da, db) = (decl("a", &a, &[]), decl("b", &b, &[]));
    cache.install(&da, &a).unwrap();
    cache.install(&db, &b).unwrap();
    cache.mark_referenced(&BTreeSet::new());
    now.fetch_add(UNREFERENCED_GRACE_MS - 1, Ordering::SeqCst);
    cache.mark_referenced(&BTreeSet::new());
    assert_eq!(cache.digests().len(), 2, "still in the grace period");
    cache.pin(&db.sha256);
    now.fetch_add(1, Ordering::SeqCst);
    cache.mark_referenced(&BTreeSet::new());
    assert!(cache.lookup(&da.sha256).is_none(), "pruned");
    assert!(cache.lookup(&db.sha256).is_some(), "an open page keeps it");
    // Named again: the grace period starts over.
    cache.unpin(&db.sha256);
    cache.mark_referenced(&[db.sha256.clone()].into());
    now.fetch_add(UNREFERENCED_GRACE_MS * 2, Ordering::SeqCst);
    cache.mark_referenced(&[db.sha256.clone()].into());
    assert!(cache.lookup(&db.sha256).is_some());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn the_least_recently_used_bundles_go_past_64_mib() {
    let root = temp_root("lru");
    let now = Arc::new(AtomicI64::new(1));
    let clock = {
        let now = now.clone();
        Arc::new(move || now.fetch_add(1, Ordering::SeqCst))
    };
    let cache = BundleCache::with_clock(root.clone(), clock);
    // Each bundle unpacks to just over 3 MiB: about 21 fit.
    let mut digests = Vec::new();
    for i in 0..24 {
        let bytes = archive(&format!("b{i}"), 3 * 1024 * 1024);
        let d = decl(&format!("b{i}"), &bytes, &[]);
        if i == 1 {
            cache.pin(&d.sha256);
        }
        cache.install(&d, &bytes).unwrap();
        digests.push(d.sha256);
    }
    assert!(cache.used_bytes() <= BUNDLE_CACHE_MAX_BYTES);
    assert!(cache.lookup(&digests[0]).is_none(), "oldest evicted");
    assert!(cache.lookup(&digests[1]).is_some(), "pinned kept");
    assert!(cache.lookup(&digests[23]).is_some(), "newest kept");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn leftovers_of_an_interrupted_install_are_cleaned_on_load() {
    let root = temp_root("leftover");
    std::fs::create_dir_all(root.join(".staging-x")).unwrap();
    std::fs::create_dir_all(root.join("a".repeat(64))).unwrap();
    let cache = BundleCache::new(root.clone());
    assert!(cache.digests().is_empty());
    assert!(!root.join(".staging-x").exists());
    assert!(!root.join("a".repeat(64)).exists());
    let _ = std::fs::remove_dir_all(root);
}
