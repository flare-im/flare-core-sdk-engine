//! 资源包：假地址和内存存储，走真实的校验与存取。

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::json;

use super::*;
use crate::bootstrap::{FetchError, FetchResponse};
use crate::manifest::Rollout;
use crate::store::MemoryAccessStore;

const BOOTSTRAP_ID: &str = "b7f3c9a1e2d4b6a8";
const INSTALL_ID: &str = "8f14e45f-ceea-467f-a8d2-6f1c2c1e7a11";
const BUILD: u64 = 20400;

fn body(kind: &str, marker: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({ "schema": RESOURCE_SCHEMA, "kind": kind, "marker": marker }))
        .unwrap()
}

fn primary_url(id: &str, version: u64) -> String {
    format!("https://cdn-a.example.com/{id}-{version}.json")
}

fn backup_url(id: &str, version: u64) -> String {
    format!("https://cdn-b.example.net/{id}-{version}.json")
}

fn pack(id: &str, kind: &str, version: u64, content: &[u8]) -> ResourcePack {
    ResourcePack {
        id: id.into(),
        kind: kind.into(),
        pack_version: version,
        min_build: 0,
        url: vec![primary_url(id, version), backup_url(id, version)],
        sha256: sha256_hex(content),
        size_bytes: content.len() as u64,
    }
}

fn release(id: &str, build: u64, packs: Vec<ResourcePack>) -> ReleaseEntry {
    ReleaseEntry {
        release_id: id.into(),
        version: "2.4.0".into(),
        build,
        published_at: None,
        rollout: Rollout {
            basis_points: 10_000,
            salt: id.into(),
            halted: false,
        },
        notes: BTreeMap::new(),
        artifacts: BTreeMap::new(),
        resource_packs: packs,
    }
}

fn manifest(releases: Vec<ReleaseEntry>) -> ReleaseManifest {
    let mut manifest = ReleaseManifest::new(
        BOOTSTRAP_ID,
        "android",
        "stable",
        1,
        "2026-11-04T08:00:00Z",
        "2026-11-18T08:00:00Z",
    );
    manifest.releases = releases;
    manifest
}

fn input(build: Option<u64>) -> PackInput {
    PackInput {
        current_build: build,
        install_id: INSTALL_ID.into(),
    }
}

fn versions(packs: &[&ResourcePack]) -> Vec<(String, u64)> {
    packs
        .iter()
        .map(|pack| (pack.id.clone(), pack.pack_version))
        .collect()
}

#[test]
fn the_highest_version_of_an_id_wins_across_releases() {
    let manifest = manifest(vec![
        release(
            "rel_a",
            20300,
            vec![pack("strings-main", "strings", 3, b"a")],
        ),
        release(
            "rel_b",
            20400,
            vec![
                pack("strings-main", "strings", 5, b"b"),
                pack("emoji-main", "emoji", 1, b"c"),
            ],
        ),
    ]);
    assert_eq!(
        versions(&select_packs(&manifest, &input(Some(BUILD)))),
        vec![("emoji-main".to_owned(), 1), ("strings-main".to_owned(), 5)]
    );
}

#[test]
fn halted_and_missed_releases_do_not_count() {
    let mut halted = release("rel_a", 20500, vec![pack("a", "strings", 9, b"a")]);
    halted.rollout.halted = true;
    let mut missed = release("rel_b", 20400, vec![pack("a", "strings", 8, b"b")]);
    missed.rollout.basis_points = 0;
    let full = release("rel_c", 20300, vec![pack("a", "strings", 7, b"c")]);
    let manifest = manifest(vec![halted, missed, full]);
    assert_eq!(
        versions(&select_packs(&manifest, &input(Some(BUILD)))),
        vec![("a".to_owned(), 7)]
    );
}

#[test]
fn a_pack_for_a_later_build_is_skipped_and_an_unknown_build_proves_nothing() {
    let mut later = pack("a", "strings", 9, b"a");
    later.min_build = BUILD + 1;
    let mut exact = pack("b", "strings", 1, b"b");
    exact.min_build = BUILD;
    let free = pack("c", "strings", 1, b"c");
    let manifest = manifest(vec![release("rel_a", 20400, vec![later, exact, free])]);
    assert_eq!(
        versions(&select_packs(&manifest, &input(Some(BUILD)))),
        vec![("b".to_owned(), 1), ("c".to_owned(), 1)]
    );
    assert_eq!(
        versions(&select_packs(&manifest, &input(None))),
        vec![("c".to_owned(), 1)]
    );
}

#[test]
fn kinds_this_version_cannot_read_are_ignored() {
    let manifest = manifest(vec![release(
        "rel_a",
        20400,
        vec![
            pack("a", "hologram", 1, b"a"),
            pack("b", "strings", 1, b"b"),
            pack("c", "tokens", 1, b"c"),
            pack("d", "emoji", 1, b"d"),
            pack("e", "help", 1, b"e"),
        ],
    )]);
    let selected = select_packs(&manifest, &input(Some(BUILD)));
    assert_eq!(selected.len(), 4);
    assert!(selected.iter().all(|pack| pack.id != "a"));
}

fn held(id: &str, digest: &str) -> HeldPack {
    HeldPack {
        id: id.into(),
        sha256: digest.into(),
    }
}

#[test]
fn the_selection_decides_by_hash_alone() {
    let same = pack("same", "strings", 2, b"same");
    let changed = pack("changed", "strings", 1, b"new");
    let fresh = pack("fresh", "strings", 1, b"fresh");
    let selected = vec![&changed, &fresh, &same];
    let stored = vec![
        held("same", &same.sha256),
        held("changed", "0".repeat(64).as_str()),
        held("withdrawn-b", "1".repeat(64).as_str()),
        held("withdrawn-a", "2".repeat(64).as_str()),
    ];
    let plan = plan_packs(&selected, &stored);
    assert_eq!(plan.fetch, vec![&changed, &fresh]);
    assert_eq!(plan.remove, vec!["withdrawn-a", "withdrawn-b"]);
}

#[test]
fn a_bad_body_is_named_by_what_is_wrong_with_it() {
    let good = body("strings", "x");
    let entry = pack("a", "strings", 1, &good);
    assert_eq!(
        verify_pack(&entry, &good).unwrap().as_bytes(),
        good.as_slice()
    );

    let mut short = good.clone();
    short.pop();
    assert_eq!(verify_pack(&entry, &short), Err(PackError::SizeMismatch));

    let mut flipped = good.clone();
    flipped[0] ^= 1;
    assert_eq!(verify_pack(&entry, &flipped), Err(PackError::HashMismatch));

    let bad: [(&[u8], PackError); 6] = [
        (b"{not json", PackError::Malformed),
        (b"\xff\xfe", PackError::Malformed),
        (b"[]", PackError::WrongSchema),
        (
            br#"["flare.resource.v1","strings"]"#,
            PackError::WrongSchema,
        ),
        (br#"{"schema":"flare.resource.v2"}"#, PackError::WrongSchema),
        (
            br#"{"schema":"flare.resource.v1","kind":"tokens"}"#,
            PackError::WrongKind,
        ),
    ];
    for (text, expected) in bad {
        let entry = pack("a", "strings", 1, text);
        assert_eq!(verify_pack(&entry, text), Err(expected), "{text:?}");
    }
}

#[derive(Default)]
struct FakeCdn {
    routes: Mutex<HashMap<String, Result<FetchResponse, FetchError>>>,
    log: Mutex<Vec<FetchRequest>>,
}

impl FakeCdn {
    fn serve(&self, url: String, content: &[u8]) {
        self.routes.lock().unwrap().insert(
            url,
            Ok(FetchResponse {
                status: 200,
                etag: None,
                body: content.to_vec(),
            }),
        );
    }

    fn fail(&self, url: String, error: FetchError) {
        self.routes.lock().unwrap().insert(url, Err(error));
    }

    fn asked(&self) -> Vec<String> {
        let log = self.log.lock().unwrap();
        log.iter().map(|request| request.url.clone()).collect()
    }
}

#[async_trait::async_trait]
impl HttpFetch for FakeCdn {
    async fn fetch(&self, request: &FetchRequest) -> Result<FetchResponse, FetchError> {
        self.log.lock().unwrap().push(request.clone());
        let route = self.routes.lock().unwrap().get(&request.url).cloned();
        route.unwrap_or(Ok(FetchResponse {
            status: 404,
            etag: None,
            body: Vec::new(),
        }))
    }
}

async fn sync(
    store: &MemoryAccessStore,
    cdn: &FakeCdn,
    manifest: &ReleaseManifest,
    metered: bool,
) -> PackSyncReport {
    sync_packs(
        store,
        cdn,
        BOOTSTRAP_ID,
        manifest,
        &input(Some(BUILD)),
        metered,
    )
    .await
    .unwrap()
}

async fn loaded(store: &MemoryAccessStore) -> Vec<StoredPack> {
    load_packs(store, BOOTSTRAP_ID, Some(BUILD)).await.unwrap()
}

#[tokio::test]
async fn a_pack_is_fetched_verified_and_stored_under_its_id() {
    let content = body("strings", "x");
    let entry = pack("strings-main", "strings", 1, &content);
    let manifest = manifest(vec![release("rel_a", 20400, vec![entry.clone()])]);
    let cdn = FakeCdn::default();
    cdn.serve(primary_url("strings-main", 1), &content);
    let store = MemoryAccessStore::new();

    let report = sync(&store, &cdn, &manifest, false).await;
    assert_eq!(report.fetched, vec!["strings-main"]);
    assert_eq!(
        store.keys(),
        vec![format!("resource/{BOOTSTRAP_ID}/strings-main")]
    );
    let request = cdn.log.lock().unwrap()[0].clone();
    assert_eq!(request.timeout_ms, RESOURCE_FETCH_TIMEOUT_MS);
    assert_eq!(request.max_body_bytes as u64, entry.size_bytes);

    let packs = loaded(&store).await;
    assert_eq!(packs.len(), 1);
    assert_eq!(packs[0].content.as_bytes(), content.as_slice());
    assert_eq!(packs[0].pack_version, 1);

    let again = sync(&store, &cdn, &manifest, false).await;
    assert_eq!(again, PackSyncReport::default());
    assert_eq!(cdn.asked().len(), 1, "哈希相同不重下");
}

#[tokio::test]
async fn a_dead_or_lying_url_falls_over_to_the_next() {
    let content = body("strings", "x");
    let manifest = manifest(vec![release(
        "rel_a",
        20400,
        vec![pack("a", "strings", 1, &content)],
    )]);

    let dead = FakeCdn::default();
    dead.fail(primary_url("a", 1), FetchError::Timeout);
    dead.serve(backup_url("a", 1), &content);
    let store = MemoryAccessStore::new();
    assert_eq!(
        sync(&store, &dead, &manifest, false).await.fetched,
        vec!["a"]
    );
    assert_eq!(dead.asked().len(), 2);

    let lying = FakeCdn::default();
    lying.serve(primary_url("a", 1), &body("strings", "y"));
    lying.serve(backup_url("a", 1), &content);
    let store = MemoryAccessStore::new();
    assert_eq!(
        sync(&store, &lying, &manifest, false).await.fetched,
        vec!["a"]
    );
    assert_eq!(
        loaded(&store).await[0].content.as_bytes(),
        content.as_slice()
    );
}

#[tokio::test]
async fn one_failed_pack_leaves_the_others_and_the_next_round_retries() {
    let first = body("strings", "x");
    let second = body("emoji", "y");
    let manifest = manifest(vec![release(
        "rel_a",
        20400,
        vec![
            pack("a", "strings", 1, &first),
            pack("b", "emoji", 1, &second),
        ],
    )]);
    let cdn = FakeCdn::default();
    cdn.serve(primary_url("b", 1), &second);
    let store = MemoryAccessStore::new();

    let report = sync(&store, &cdn, &manifest, false).await;
    assert_eq!(report.fetched, vec!["b"]);
    assert_eq!(report.failed, vec![("a".to_owned(), "http.404".to_owned())]);

    cdn.serve(primary_url("a", 1), &first);
    let report = sync(&store, &cdn, &manifest, false).await;
    assert_eq!(report.fetched, vec!["a"]);
    assert_eq!(loaded(&store).await.len(), 2);
}

#[tokio::test]
async fn an_oversized_pack_is_never_requested() {
    let mut entry = pack("a", "strings", 1, b"x");
    entry.size_bytes = MAX_RESOURCE_PACK_BYTES + 1;
    let manifest = manifest(vec![release("rel_a", 20400, vec![entry])]);
    let cdn = FakeCdn::default();
    let store = MemoryAccessStore::new();
    let report = sync(&store, &cdn, &manifest, false).await;
    assert_eq!(report.failed, vec![("a".to_owned(), "tooLarge".to_owned())]);
    assert!(cdn.asked().is_empty());
}

#[tokio::test]
async fn a_metered_network_removes_but_does_not_download() {
    let old = body("strings", "old");
    let cdn = FakeCdn::default();
    cdn.serve(primary_url("old", 1), &old);
    let store = MemoryAccessStore::new();
    let first = manifest(vec![release(
        "rel_a",
        20400,
        vec![pack("old", "strings", 1, &old)],
    )]);
    sync(&store, &cdn, &first, false).await;

    let content = body("strings", "new");
    cdn.serve(primary_url("new", 1), &content);
    let second = manifest(vec![release(
        "rel_b",
        20400,
        vec![pack("new", "strings", 1, &content)],
    )]);
    let report = sync(&store, &cdn, &second, true).await;
    assert_eq!(report.removed, vec!["old"]);
    assert_eq!(report.postponed, vec!["new"]);
    assert!(store.keys().is_empty());

    let report = sync(&store, &cdn, &second, false).await;
    assert_eq!(report.fetched, vec!["new"]);
}

#[tokio::test]
async fn a_lower_version_replaces_a_withdrawn_one() {
    let bad = body("strings", "bad");
    let good = body("strings", "good");
    let cdn = FakeCdn::default();
    cdn.serve(primary_url("a", 5), &bad);
    cdn.serve(primary_url("a", 3), &good);
    let store = MemoryAccessStore::new();
    let first = manifest(vec![release(
        "rel_a",
        20400,
        vec![pack("a", "strings", 5, &bad)],
    )]);
    sync(&store, &cdn, &first, false).await;

    let second = manifest(vec![release(
        "rel_b",
        20300,
        vec![pack("a", "strings", 3, &good)],
    )]);
    let report = sync(&store, &cdn, &second, false).await;
    assert_eq!(report.fetched, vec!["a"]);
    let packs = loaded(&store).await;
    assert_eq!(packs[0].pack_version, 3);
    assert_eq!(packs[0].content.as_bytes(), good.as_slice());
}

#[tokio::test]
async fn a_failed_replacement_keeps_the_stored_pack_for_the_next_check() {
    let old = body("strings", "old");
    let cdn = FakeCdn::default();
    cdn.serve(primary_url("a", 1), &old);
    let store = MemoryAccessStore::new();
    let first = manifest(vec![release(
        "rel_a",
        20400,
        vec![pack("a", "strings", 1, &old)],
    )]);
    sync(&store, &cdn, &first, false).await;

    let second = manifest(vec![release(
        "rel_a",
        20400,
        vec![pack("a", "strings", 2, &body("strings", "new"))],
    )]);
    let report = sync(&store, &cdn, &second, false).await;
    assert_eq!(report.failed.len(), 1);
    assert_eq!(loaded(&store).await[0].pack_version, 1);
}

#[tokio::test]
async fn a_failed_write_is_reported_and_the_other_packs_go_on() {
    let content = body("strings", "x");
    let manifest = manifest(vec![release(
        "rel_a",
        20400,
        vec![pack("a", "strings", 1, &content)],
    )]);
    let cdn = FakeCdn::default();
    cdn.serve(primary_url("a", 1), &content);
    let store = MemoryAccessStore::new();
    store.set_fail_writes(true);
    let report = sync(&store, &cdn, &manifest, false).await;
    assert!(report.fetched.is_empty());
    assert_eq!(report.failed.len(), 1);
}

async fn stored(store: &MemoryAccessStore, entry: &ResourcePack, content: &[u8]) {
    let record = StoredPack::new(entry, String::from_utf8(content.to_vec()).unwrap());
    store
        .put(
            &keys::resource_pack(BOOTSTRAP_ID, &entry.id),
            &serde_json::to_vec(&record).unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn loading_sorts_by_id_and_stays_inside_the_bootstrap() {
    let store = MemoryAccessStore::new();
    let content = body("strings", "x");
    stored(&store, &pack("b", "strings", 1, &content), &content).await;
    stored(&store, &pack("a", "strings", 1, &content), &content).await;
    store
        .put(&keys::resource_pack("other", "c"), b"{}")
        .await
        .unwrap();
    let ids: Vec<String> = loaded(&store)
        .await
        .into_iter()
        .map(|pack| pack.id)
        .collect();
    assert_eq!(ids, vec!["a", "b"]);
    assert_eq!(store.len(), 3, "别的应用的分区不动");
}

#[tokio::test]
async fn a_spoiled_pack_is_deleted_on_load() {
    let store = MemoryAccessStore::new();
    let content = body("strings", "x");
    let entry = pack("a", "strings", 1, &content);
    let mut tampered = StoredPack::new(&entry, String::from_utf8(content.clone()).unwrap());
    tampered.content.push(' ');
    store
        .put(
            &keys::resource_pack(BOOTSTRAP_ID, "a"),
            &serde_json::to_vec(&tampered).unwrap(),
        )
        .await
        .unwrap();
    store
        .put(&keys::resource_pack(BOOTSTRAP_ID, "b"), b"{not json")
        .await
        .unwrap();
    // 记录里的 id 和键对不上：搬错了位置。
    store
        .put(
            &keys::resource_pack(BOOTSTRAP_ID, "c"),
            &serde_json::to_vec(&StoredPack::new(
                &entry,
                String::from_utf8(content).unwrap(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();

    assert!(loaded(&store).await.is_empty());
    assert!(store.is_empty());
}

#[tokio::test]
async fn min_build_is_judged_again_on_load() {
    let store = MemoryAccessStore::new();
    let content = body("strings", "x");
    let mut entry = pack("a", "strings", 1, &content);
    entry.min_build = BUILD + 1;
    stored(&store, &entry, &content).await;
    assert!(loaded(&store).await.is_empty());
    assert!(store.is_empty());
}

#[test]
fn the_host_gets_the_original_text_under_the_wire_names() {
    let content = body("strings", "x");
    let record = StoredPack::new(
        &pack("a", "strings", 7, &content),
        String::from_utf8(content.clone()).unwrap(),
    );
    let resources = UpdateResources {
        packs: vec![UpdateResourcePack::from(record)],
    };
    assert_eq!(
        serde_json::to_value(&resources).unwrap(),
        json!({ "packs": [{
            "id": "a",
            "kind": "strings",
            "packVersion": 7,
            "contentJson": String::from_utf8(content).unwrap()
        }] })
    );
}
