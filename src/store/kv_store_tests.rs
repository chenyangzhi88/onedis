use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn test_store() -> KvStore {
    let unique = format!(
        "onedis-kv-store-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target"))
        .join("onedis-test-data")
        .join(unique);
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    KvStore::new(db_path, wal_dir, 1)
}

#[test]
fn test_put_get_delete() {
    let store = test_store();
    store.put_raw(b"key1", b"val1").unwrap();
    assert_eq!(store.get_raw(b"key1").unwrap(), Some(b"val1".to_vec()));
    assert!(store.delete_key(b"key1").unwrap());
    assert_eq!(store.get_raw(b"key1").unwrap(), None);
}

#[test]
fn test_write_batch_atomic() {
    let store = test_store();
    let mut batch = WriteBatch::new();
    (batch.put(b"a", b"1")).expect("write batch append invariant violated");
    (batch.put(b"b", b"2")).expect("write batch append invariant violated");
    store.write_batch(&batch).unwrap();
    assert_eq!(store.get_raw(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(store.get_raw(b"b").unwrap(), Some(b"2".to_vec()));
}

#[tokio::test]
async fn async_raw_blob_observe_multi_get_and_compare_write_paths_work() {
    let store = test_store();

    assert_eq!(
        store.multi_get_raw(&[]).unwrap(),
        Vec::<Option<Vec<u8>>>::new()
    );
    assert_eq!(
        store.multi_get_raw_async(&[]).await.unwrap(),
        Vec::<Option<Vec<u8>>>::new()
    );

    store.put_raw(b"a", b"1").unwrap();
    store.put_raw(b"b", b"2").unwrap();
    store.blob_put_raw(b"blob:sync", b"blob-value").unwrap();
    store
        .blob_put_raw_async(b"blob:async", b"blob-async")
        .await
        .unwrap();

    assert_eq!(
        store.get_raw_async(b"a").await.unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(store.get_raw_bytes(b"b").unwrap().unwrap().as_ref(), b"2");
    assert_eq!(
        store
            .get_raw_bytes_async(b"blob:async")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"blob-async"
    );
    assert!(store.contains_key(b"blob:sync").unwrap());
    assert!(store.contains_key_async(b"blob:async").await.unwrap());

    let observed = store.get_raw_observed_async(b"a").await.unwrap();
    assert_eq!(observed.value().map(Bytes::as_ref), Some(&b"1"[..]));
    let state = store.observe_raw_key_state_async(b"missing").await.unwrap();
    assert!(!state.exists());

    store.put_raw(b"a", b"changed").unwrap();
    let failures_before_conflict = store.storage_health().failure_count();
    let mut stale_batch = WriteBatch::new();
    (stale_batch.put(b"stale-cas", b"must-not-write"))
        .expect("write batch append invariant violated");
    assert!(matches!(
        store
            .compare_and_write_batch_async(&[observed.condition()], &stale_batch)
            .await,
        Err(Status::ConditionFailed(_))
    ));
    assert!(store.storage_health().is_healthy());
    assert_eq!(
        store.storage_health().failure_count(),
        failures_before_conflict
    );
    assert_eq!(store.get_raw(b"stale-cas").unwrap(), None);
    store.put_raw(b"a", b"1").unwrap();

    let keys = vec![b"a".to_vec(), b"missing".to_vec(), b"b".to_vec()];
    assert_eq!(
        store.multi_get_raw(&keys).unwrap(),
        vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]
    );
    assert_eq!(
        store.multi_get_raw_async(&keys).await.unwrap(),
        vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]
    );

    let mut ok_batch = WriteBatch::new();
    (ok_batch.put(b"cas", b"ok")).expect("write batch append invariant violated");
    store
        .compare_and_write_batch_async(
            &[CompareCondition::with_expected(b"a", Some(b"1".to_vec()))],
            &ok_batch,
        )
        .await
        .unwrap();
    assert_eq!(store.get_raw(b"cas").unwrap(), Some(b"ok".to_vec()));

    let mut failed_batch = WriteBatch::new();
    (failed_batch.put(b"cas", b"bad")).expect("write batch append invariant violated");
    assert!(
        store
            .compare_and_write_batch_async(
                &[CompareCondition::with_expected(
                    b"a",
                    Some(b"wrong".to_vec())
                )],
                &failed_batch,
            )
            .await
            .is_err()
    );
    assert_eq!(store.get_raw(b"cas").unwrap(), Some(b"ok".to_vec()));
}

#[tokio::test]
async fn range_scan_visit_delete_range_and_direct_batches_cover_sync_and_async() {
    let store = test_store();
    for idx in 0..5 {
        store
            .put_raw(format!("p:{idx}").as_bytes(), format!("v{idx}").as_bytes())
            .unwrap();
    }
    store.put_raw(b"q:0", b"out").unwrap();

    let scan = store.scan_prefix_raw(b"p:").unwrap();
    assert_eq!(scan.len(), 5);
    assert!(scan.iter().all(|(key, _)| key.starts_with(b"p:")));

    let async_scan = store.scan_prefix_raw_async(b"p:").await.unwrap();
    assert_eq!(async_scan.len(), 5);

    let limited = store
        .scan_range_raw_limited(b"p:", Some(b"p;".to_vec()), 3)
        .unwrap();
    assert_eq!(limited.len(), 3);
    assert_eq!(
        store
            .scan_range_raw_limited_async(b"p:", Some(b"p;".to_vec()), 2)
            .await
            .unwrap()
            .len(),
        2
    );

    assert_eq!(
        store
            .scan_range_raw_keys_at_ordinals(b"p:", Some(b"p;".to_vec()), &[0, 2, 4],)
            .unwrap(),
        vec![b"p:0".to_vec(), b"p:2".to_vec(), b"p:4".to_vec()]
    );
    assert_eq!(
        store
            .scan_range_raw_keys_at_ordinals_async(b"p:", Some(b"p;".to_vec()), &[1, 3],)
            .await
            .unwrap(),
        vec![b"p:1".to_vec(), b"p:3".to_vec()]
    );
    assert!(
        store
            .scan_range_raw_keys_at_ordinals_async(b"p:", Some(b"p;".to_vec()), &[],)
            .await
            .unwrap()
            .is_empty()
    );

    let visited = store
        .scan_range_raw_visit_async(b"p:", Some(b"p;".to_vec()), 10, |key, _| key != b"p:2")
        .await
        .unwrap();
    assert_eq!(visited, 3);
    assert_eq!(
        store
            .scan_range_raw_visit_async(b"p:", Some(b"p;".to_vec()), 0, |_, _| true)
            .await
            .unwrap(),
        0
    );

    let mut direct = WriteBatch::new();
    (direct.put(b"direct:sync", b"1")).expect("write batch append invariant violated");
    store.write_batch_direct(&direct).unwrap();
    assert_eq!(store.get_raw(b"direct:sync").unwrap(), Some(b"1".to_vec()));

    let mut direct_async = WriteBatch::new();
    (direct_async.put(b"direct:async", b"2")).expect("write batch append invariant violated");
    store.write_batch_direct_async(direct_async).await.unwrap();
    assert_eq!(store.get_raw(b"direct:async").unwrap(), Some(b"2".to_vec()));

    let mut async_batch = WriteBatch::new();
    (async_batch.put(b"async:put", b"3")).expect("write batch append invariant violated");
    (async_batch.delete(b"direct:sync")).expect("write batch append invariant violated");
    store.write_batch_async(&async_batch).await.unwrap();
    assert_eq!(store.get_raw(b"async:put").unwrap(), Some(b"3".to_vec()));
    assert_eq!(store.get_raw(b"direct:sync").unwrap(), None);

    store.delete_range(b"p:", b"p;").unwrap();
    assert!(store.scan_prefix_raw(b"p:").unwrap().is_empty());
    assert_eq!(store.get_raw(b"q:0").unwrap(), Some(b"out".to_vec()));
    assert!(!store.delete_key(b"missing").unwrap());
}

#[tokio::test]
async fn transaction_commit_discard_scan_and_batch_paths_work() {
    let store = test_store();
    store.put_raw(b"base", b"old").unwrap();

    let txn = store.begin_transaction().unwrap();
    assert!(txn.is_transactional());
    assert!(!store.is_transactional());
    txn.put_raw(b"base", b"new").unwrap();
    txn.put_raw(b"txn:1", b"a").unwrap();
    txn.put_raw(b"txn:2", b"b").unwrap();
    assert_eq!(txn.get_raw(b"base").unwrap(), Some(b"new".to_vec()));
    assert_eq!(store.get_raw(b"base").unwrap(), Some(b"old".to_vec()));
    assert!(txn.contains_key(b"txn:1").unwrap());
    assert_eq!(
        txn.multi_get_raw(&[b"txn:1".to_vec(), b"missing".to_vec()])
            .unwrap(),
        vec![Some(b"a".to_vec()), None]
    );
    txn.commit_transaction().unwrap();
    txn.commit_transaction().unwrap();
    assert_eq!(store.get_raw(b"base").unwrap(), Some(b"new".to_vec()));

    let txn = store.begin_transaction().unwrap();
    txn.put_raw(b"discarded", b"value").unwrap();
    txn.discard_transaction();
    txn.discard_transaction();
    assert_eq!(store.get_raw(b"discarded").unwrap(), None);

    let txn = store.begin_transaction().unwrap();
    let mut batch = WriteBatch::new();
    (batch.put(b"batched", b"value")).expect("write batch append invariant violated");
    (batch.delete(b"base")).expect("write batch append invariant violated");
    txn.write_batch(&batch).unwrap();
    txn.commit_transaction_async().await.unwrap();
    txn.commit_transaction_async().await.unwrap();
    assert_eq!(store.get_raw(b"batched").unwrap(), Some(b"value".to_vec()));
    assert_eq!(store.get_raw(b"base").unwrap(), None);

    let view = txn.non_transactional_view();
    assert!(!view.is_transactional());
    assert_eq!(view.get_raw(b"batched").unwrap(), Some(b"value".to_vec()));
}

#[tokio::test]
async fn transaction_async_read_observe_and_commit_paths_work() {
    let store = test_store();
    let txn = store.begin_transaction().unwrap();
    txn.put_raw(b"async:txn", b"value").unwrap();
    assert_eq!(
        txn.get_raw_async(b"async:txn").await.unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(
        txn.get_raw_bytes_async(b"async:txn")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"value"
    );
    assert!(txn.contains_key_async(b"async:txn").await.unwrap());
    assert_eq!(
        txn.multi_get_raw_async(&[b"async:txn".to_vec(), b"missing".to_vec()])
            .await
            .unwrap(),
        vec![Some(b"value".to_vec()), None]
    );
    let observed = txn.get_raw_observed_async(b"async:txn").await.unwrap();
    assert_eq!(observed.value().map(Bytes::as_ref), Some(&b"value"[..]));
    assert!(
        txn.observe_raw_key_state_async(b"async:txn")
            .await
            .unwrap()
            .exists()
    );
    txn.commit_transaction_async().await.unwrap();
    assert_eq!(
        store.get_raw(b"async:txn").unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn multi_table_transaction_conflict_is_atomic() {
    let store = test_store();
    let db0 = store.for_db_index(0);
    let db1 = store.for_db_index(1);
    db1.put_raw(b"source", b"old").unwrap();

    let transaction = db1.begin_transaction().unwrap();
    let target = transaction.for_db_index(0);
    assert_eq!(
        transaction.get_raw(b"source").unwrap(),
        Some(b"old".to_vec())
    );
    target.put_raw(b"target", b"old").unwrap();
    transaction.delete_key(b"source").unwrap();

    db1.put_raw(b"source", b"new").unwrap();
    assert!(transaction.commit_transaction().is_err());
    assert_eq!(db0.get_raw(b"target").unwrap(), None);
    assert_eq!(db1.get_raw(b"source").unwrap(), Some(b"new".to_vec()));
}

#[tokio::test]
async fn multi_table_transaction_async_commit_is_atomic() {
    let store = test_store();
    let db0 = store.for_db_index(0);
    let db1 = store.for_db_index(1);
    db1.put_raw(b"source", b"old").unwrap();

    let transaction = db1.begin_transaction().unwrap();
    let target = transaction.for_db_index(0);
    assert_eq!(
        transaction.get_raw_async(b"source").await.unwrap(),
        Some(b"old".to_vec())
    );
    target.put_raw(b"target", b"old").unwrap();
    transaction.delete_key(b"source").unwrap();

    db1.put_raw(b"source", b"new").unwrap();
    assert!(transaction.commit_transaction_async().await.is_err());
    assert_eq!(db0.get_raw(b"target").unwrap(), None);
    assert_eq!(db1.get_raw(b"source").unwrap(), Some(b"new".to_vec()));
}

#[tokio::test]
async fn transaction_async_scans_visits_delete_range_and_compare_write_are_isolated_until_commit() {
    let store = test_store();
    store.put_raw(b"txnscan:0", b"old").unwrap();
    store.put_raw(b"txnscan:outside", b"outside").unwrap();

    let txn = store.begin_transaction().unwrap();
    txn.put_raw(b"txnscan:0", b"v0").unwrap();
    txn.put_raw(b"txnscan:1", b"v1").unwrap();
    txn.put_raw(b"txnscan:2", b"v2").unwrap();
    txn.put_raw(b"txnscan:stop", b"stop").unwrap();

    let prefix_entries = txn.scan_prefix_raw_async(b"txnscan:").await.unwrap();
    assert!(
        prefix_entries
            .iter()
            .any(|(key, value)| key == b"txnscan:1" && value == b"v1")
    );

    assert!(
        txn.scan_range_raw_limited(b"txnscan:", Some(b"txnscan;".to_vec()), 0)
            .unwrap()
            .is_empty()
    );
    let range_entries = txn
        .scan_range_raw_limited(b"txnscan:", Some(b"txnscan;".to_vec()), 2)
        .unwrap();
    assert_eq!(range_entries.len(), 2);
    let async_range_entries = txn
        .scan_range_raw_limited_async(b"txnscan:", Some(b"txnscan;".to_vec()), 3)
        .await
        .unwrap();
    assert_eq!(async_range_entries.len(), 3);
    assert_eq!(
            txn.scan_range_raw_keys_at_ordinals_async(
                b"txnscan:",
                Some(b"txnscan;".to_vec()),
                &[0, 2],
            )
            .await.unwrap(),
            vec![b"txnscan:0".to_vec(), b"txnscan:2".to_vec()]
        );

    let visited = txn
        .scan_range_raw_visit_async(b"txnscan:", Some(b"txnscan;".to_vec()), 10, |key, _| {
            key != b"txnscan:stop"
        })
        .await
        .unwrap();
    assert!(visited >= 4);
    assert_eq!(
        txn.scan_range_raw_visit_async(b"txnscan:", Some(b"txnscan;".to_vec()), 2, |_, _| { true })
            .await
            .unwrap(),
        2
    );

    let mut compare_batch = WriteBatch::new();
    (compare_batch.put(b"txnscan:compare", b"ok")).expect("write batch append invariant violated");
    txn.compare_and_write_batch_async(
        &[CompareCondition::with_expected(
            b"txnscan:0",
            Some(b"v0".to_vec()),
        )],
        &compare_batch,
    )
    .await
    .unwrap();
    assert_eq!(
        txn.get_raw(b"txnscan:compare").unwrap(),
        Some(b"ok".to_vec())
    );
    assert_eq!(store.get_raw(b"txnscan:compare").unwrap(), None);

    let observed = txn.get_raw_observed_async(b"txnscan:0").await.unwrap();
    txn.put_raw(b"txnscan:0", b"changed-after-observe").unwrap();
    let mut stale_batch = WriteBatch::new();
    (stale_batch.put(b"txnscan:stale-cas", b"must-not-write"))
        .expect("write batch append invariant violated");
    assert!(matches!(
        txn.compare_and_write_batch_async(&[observed.condition()], &stale_batch)
            .await,
        Err(Status::ConditionFailed(_))
    ));
    assert_eq!(txn.get_raw(b"txnscan:stale-cas").unwrap(), None);
    txn.put_raw(b"txnscan:0", b"v0").unwrap();

    txn.delete_range(b"txnscan:1", b"txnscan:3").unwrap();
    assert_eq!(txn.get_raw(b"txnscan:1").unwrap(), None);
    assert_eq!(txn.get_raw(b"txnscan:2").unwrap(), None);
    txn.commit_transaction_async().await.unwrap();

    assert_eq!(store.get_raw(b"txnscan:0").unwrap(), Some(b"v0".to_vec()));
    assert_eq!(store.get_raw(b"txnscan:1").unwrap(), None);
    assert_eq!(store.get_raw(b"txnscan:2").unwrap(), None);
    assert_eq!(
        store.get_raw(b"txnscan:compare").unwrap(),
        Some(b"ok".to_vec())
    );
    assert_eq!(
        store.get_raw(b"txnscan:outside").unwrap(),
        Some(b"outside".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_store_handles_concurrent_writes_and_integer_merge_paths() {
    let store = Arc::new(test_store());
    let wrote = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for task_id in 0..8 {
        let store = store.clone();
        let wrote = wrote.clone();
        tasks.push(tokio::spawn(async move {
            for item in 0..25 {
                let key = format!("concurrent:{task_id}:{item}");
                store.put_raw(key.as_bytes(), b"value").unwrap();
                assert!(store.contains_key_async(key.as_bytes()).await.unwrap());
                wrote.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(wrote.load(Ordering::Relaxed), 200);
    assert_eq!(store.scan_prefix_raw(b"concurrent:").unwrap().len(), 200);

    store.merge_raw(b"counter", &5i64.to_be_bytes()).unwrap();
    store.merge_raw(b"counter", &7i64.to_be_bytes()).unwrap();
    store
        .merge_raw_async(b"counter", &(-2i64).to_be_bytes())
        .await
        .unwrap();
    let encoded = store.get_raw(b"counter").unwrap().unwrap();
    assert_eq!(encoded[0..8], 0u64.to_be_bytes());
    assert_eq!(encoded[16], OnedisIntegerMergeOperator::TYPE_STRING);
    assert_eq!(&encoded[17..], b"10");

    let mut existing = OnedisIntegerMergeOperator::encode_string(9, 12345);
    store.put_raw(b"counter:ttl", &existing).unwrap();
    store
        .merge_raw(b"counter:ttl", &1i64.to_be_bytes())
        .unwrap();
    existing = store.get_raw(b"counter:ttl").unwrap().unwrap();
    assert_eq!(
        u64::from_be_bytes(existing[0..8].try_into().unwrap()),
        12345
    );
    assert_eq!(&existing[17..], b"10");
}

#[test]
fn prefix_bound_and_merge_operator_error_edges_are_covered() {
    assert_eq!(prefix_exclusive_upper_bound(b"abc"), Some(b"abd".to_vec()));
    assert_eq!(prefix_exclusive_upper_bound(&[0xFF, 0xFF]), None);

    let op = OnedisIntegerMergeOperator;
    assert_eq!(op.name(), "onedis_integer");
    assert!(OnedisIntegerMergeOperator::decode_operand(b"short", "operand").is_err());
    assert!(OnedisIntegerMergeOperator::decode_existing(b"short").is_err());

    let mut wrong_type = OnedisIntegerMergeOperator::encode_string(1, 0);
    wrong_type[16] = 99;
    assert!(OnedisIntegerMergeOperator::decode_existing(&wrong_type).is_err());

    let mut invalid_utf8 = OnedisIntegerMergeOperator::encode_string(1, 0);
    invalid_utf8[17] = 0xFF;
    assert!(OnedisIntegerMergeOperator::decode_existing(&invalid_utf8).is_err());

    let mut not_integer = OnedisIntegerMergeOperator::encode_string(1, 0);
    not_integer.truncate(17);
    not_integer.extend_from_slice(b"nan");
    assert!(OnedisIntegerMergeOperator::decode_existing(&not_integer).is_err());

    assert!(
        op.partial_merge(b"k", &i64::MAX.to_be_bytes(), &1i64.to_be_bytes())
            .is_err()
    );
    assert!(
        op.full_merge(
            b"k",
            Some(&OnedisIntegerMergeOperator::encode_string(i64::MAX, 0)),
            &[&1i64.to_be_bytes()]
        )
        .is_err()
    );

    let hash_field_key = b"\x80\xffh\0hash\0\0\0\0\0\0\0\0\x01field";
    assert_eq!(
        op.full_merge(hash_field_key, Some(b"9"), &[&2i64.to_be_bytes()])
            .unwrap(),
        Some(b"11".to_vec())
    );
    assert!(op.full_merge(hash_field_key, Some(b"nan"), &[]).is_err());
}
