use super::*;
use crate::store::db::{CounterCacheRuntime, VectorRuntimeRegistry};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_hot_counter_returns_each_linearized_value_once() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for _ in 0..256 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            db.increment_integer_string_async("hot-counter", 1)
                .await
                .unwrap()
        }));
    }

    let mut values = Vec::with_capacity(tasks.len());
    for task in tasks {
        values.push(task.await.unwrap());
    }
    values.sort_unstable();
    assert_eq!(values, (1..=256).collect::<Vec<_>>());
    assert_eq!(
        db.get_string("hot-counter").unwrap().as_deref(),
        Some("256")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn structural_set_does_not_allow_an_older_counter_merge_to_land_after_it() {
    let db = Arc::new(test_db());
    db.increment_integer_string_async("set-race", 1)
        .await
        .unwrap();

    let mut increments = Vec::new();
    for _ in 0..128 {
        let db = db.clone();
        increments.push(tokio::spawn(async move {
            db.increment_integer_string_async("set-race", 1)
                .await
                .unwrap()
        }));
    }
    db.set_string_bytes_async(
        "set-race".to_string(),
        b"1000".to_vec(),
        SetExpiration::Clear,
        SetCondition::Always,
        false,
    )
    .await
    .unwrap();
    for increment in increments {
        increment.await.unwrap();
    }

    let after_set = db
        .increment_integer_string_async("set-race", 1)
        .await
        .unwrap();
    assert!(after_set >= 1001);
    assert_eq!(
        db.get_string("set-race").unwrap(),
        Some(after_set.to_string())
    );
}

#[tokio::test]
async fn ttl_counter_uses_strict_path_and_does_not_resurrect_after_expiry() {
    let db = test_db();
    db.insert_string("ttl-counter".to_string(), "1".to_string(), Some(20))
        .unwrap();

    assert_eq!(
        db.increment_integer_string_async("ttl-counter", 1)
            .await
            .unwrap(),
        2
    );
    assert!(db.ttl_millis_readonly("ttl-counter").unwrap() > 0);

    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        db.increment_integer_string_async("ttl-counter", 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(db.get_string("ttl-counter").unwrap().as_deref(), Some("1"));
}

#[tokio::test]
async fn arbitrary_increment_stays_correct_when_operand_aggregation_would_overflow() {
    let db = test_db();
    db.insert_string_ref("wide-delta", &i64::MIN.to_string())
        .unwrap();

    assert_eq!(
        db.increment_integer_string_async("wide-delta", i64::MAX)
            .await
            .unwrap(),
        -1
    );
    assert_eq!(
        db.increment_integer_string_async("wide-delta", 1)
            .await
            .unwrap(),
        0
    );
    assert_eq!(db.get_string("wide-delta").unwrap().as_deref(), Some("0"));
}

#[tokio::test]
async fn transaction_commit_invalidates_a_warm_counter_cache() {
    let db = test_db();
    assert_eq!(
        db.increment_integer_string_async("txn-counter", 1)
            .await
            .unwrap(),
        1
    );

    let txn = db.transactional_view().unwrap();
    txn.insert_string_ref("txn-counter", "100").unwrap();
    txn.commit_transaction_async().await.unwrap();

    assert_eq!(
        db.increment_integer_string_async("txn-counter", 1)
            .await
            .unwrap(),
        101
    );
}

#[tokio::test]
async fn flushdb_invalidates_warm_counter_cache() {
    let db = test_db();
    db.increment_integer_string_async("flush-counter", 1)
        .await
        .unwrap();

    db.flushdb_async().await.unwrap();
    assert_eq!(
        db.increment_integer_string_async("flush-counter", 1)
            .await
            .unwrap(),
        1
    );

    db.clear_async().await.unwrap();
    assert_eq!(
        db.increment_integer_string_async("flush-counter", 1)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn counter_merge_publishes_watch_mutation() {
    let db = test_db();
    let snapshot = db.watch_version_snapshot("watched-counter").unwrap();

    db.increment_integer_string_async("watched-counter", 1)
        .await
        .unwrap();

    assert!(
        db.watch_version_changed("watched-counter", snapshot.0, snapshot.1)
            .unwrap()
    );
    db.release_watch("watched-counter");
}

#[tokio::test]
async fn cross_db_copy_invalidates_the_target_counter_cache() {
    let root = test_root("onedis-shared-counter-cache-test");
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let store = KvStore::new(db_path, wal_dir, 1);
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    let tracker = Arc::new(KeyMutationTracker::default());
    let vectors = Arc::new(VectorRuntimeRegistry::default());
    let counters = Arc::new(CounterCacheRuntime::default());
    let db0 = Db::new_with_mutation_tracker_and_vector_runtimes(
        0,
        store.clone(),
        version_counter.clone(),
        ttl_manager.clone(),
        tracker.clone(),
        vectors.clone(),
        counters.clone(),
    );
    let db1 = Db::new_with_mutation_tracker_and_vector_runtimes(
        1,
        store,
        version_counter,
        ttl_manager,
        tracker,
        vectors,
        counters,
    );

    db0.insert_string_ref("source-counter", "40").unwrap();
    assert_eq!(
        db1.increment_integer_string_async("target-counter", 1)
            .await
            .unwrap(),
        1
    );
    assert!(
        db0.copy_key_to_db_async(1, "source-counter", "target-counter", true)
            .await
            .unwrap()
    );
    assert_eq!(
        db1.increment_integer_string_async("target-counter", 1)
            .await
            .unwrap(),
        41
    );
}
