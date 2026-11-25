use flare_im_core_sdk::storage::{SeqRange, TimelineCoverageStore};

fn pairs(coverage: &flare_im_core_sdk::storage::TimelineCoverage) -> Vec<(u64, u64)> {
    coverage
        .ranges()
        .iter()
        .map(|range| (range.from, range.to))
        .collect()
}

async fn add(store: &dyn TimelineCoverageStore, conversation_id: &str, from: u64, to: u64) {
    store
        .add(conversation_id, SeqRange::new(from, to).expect("range"))
        .await
        .expect("add range");
}

async fn read(store: &dyn TimelineCoverageStore, conversation_id: &str) -> Vec<(u64, u64)> {
    pairs(
        &store
            .coverage(conversation_id)
            .await
            .expect("read coverage"),
    )
}

pub(crate) async fn assert_store_contract(store: &dyn TimelineCoverageStore) {
    assert_eq!(read(store, "c1").await, vec![], "no range before any proof");

    add(store, "c1", 10, 20).await;
    add(store, "c1", 30, 40).await;
    assert_eq!(read(store, "c1").await, vec![(10, 20), (30, 40)]);

    add(store, "c1", 21, 29).await;
    assert_eq!(
        read(store, "c1").await,
        vec![(10, 40)],
        "adjacent ranges merge"
    );

    add(store, "c1", 1, 5).await;
    add(store, "c1", 50, 60).await;
    assert_eq!(read(store, "c1").await, vec![(1, 5), (10, 40), (50, 60)]);

    add(store, "c1", 4, 55).await;
    assert_eq!(
        read(store, "c1").await,
        vec![(1, 60)],
        "one range overlapping three merges them all"
    );

    add(store, "c1", 7, 9).await;
    assert_eq!(
        read(store, "c1").await,
        vec![(1, 60)],
        "a contained range changes nothing"
    );

    add(store, "c2", 9_000_000_000_000, 9_000_000_000_100).await;
    assert_eq!(
        read(store, "c2").await,
        vec![(9_000_000_000_000, 9_000_000_000_100)],
        "large seqs round-trip"
    );
    assert_eq!(
        read(store, "c1").await,
        vec![(1, 60)],
        "conversations are apart"
    );

    store.clear("c1").await.expect("clear");
    assert_eq!(read(store, "c1").await, vec![]);
    assert_eq!(
        read(store, "c2").await,
        vec![(9_000_000_000_000, 9_000_000_000_100)]
    );

    add(store, "c1", 3, 4).await;
    assert_eq!(
        read(store, "c1").await,
        vec![(3, 4)],
        "a cleared conversation starts over"
    );
}
