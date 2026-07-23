#[cfg(test)]
mod tests {
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
        store.put_raw(b"key1", b"val1");
        assert_eq!(store.get_raw(b"key1"), Some(b"val1".to_vec()));
        assert!(store.delete_key(b"key1"));
        assert_eq!(store.get_raw(b"key1"), None);
    }

    #[test]
    fn test_write_batch_atomic() {
        let store = test_store();
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"1");
        batch.put(b"b", b"2");
        store.write_batch(&batch);
        assert_eq!(store.get_raw(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get_raw(b"b"), Some(b"2".to_vec()));
    }

    #[tokio::test]
    async fn async_raw_blob_observe_multi_get_and_compare_write_paths_work() {
        let store = test_store();

        assert_eq!(store.multi_get_raw(&[]), Vec::<Option<Vec<u8>>>::new());
        assert_eq!(
            store.multi_get_raw_async(&[]).await,
            Vec::<Option<Vec<u8>>>::new()
        );

        store.put_raw(b"a", b"1");
        store.put_raw(b"b", b"2");
        store.blob_put_raw(b"blob:sync", b"blob-value");
        store.blob_put_raw_async(b"blob:async", b"blob-async").await;

        assert_eq!(store.get_raw_async(b"a").await, Some(b"1".to_vec()));
        assert_eq!(store.get_raw_bytes(b"b").unwrap().as_ref(), b"2");
        assert_eq!(
            store
                .get_raw_bytes_async(b"blob:async")
                .await
                .unwrap()
                .as_ref(),
            b"blob-async"
        );
        assert!(store.contains_key(b"blob:sync"));
        assert!(store.contains_key_async(b"blob:async").await);

        let observed = store.get_raw_observed_async(b"a").await;
        assert_eq!(observed.value().map(Bytes::as_ref), Some(&b"1"[..]));
        let state = store.observe_raw_key_state_async(b"missing").await;
        assert!(!state.exists());

        store.put_raw(b"a", b"changed");
        let mut stale_batch = WriteBatch::new();
        stale_batch.put(b"stale-cas", b"must-not-write");
        assert!(matches!(
            store
                .compare_and_write_batch_async(&[observed.condition()], &stale_batch)
                .await,
            Err(Status::ConditionFailed(_))
        ));
        assert_eq!(store.get_raw(b"stale-cas"), None);
        store.put_raw(b"a", b"1");

        let keys = vec![b"a".to_vec(), b"missing".to_vec(), b"b".to_vec()];
        assert_eq!(
            store.multi_get_raw(&keys),
            vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]
        );
        assert_eq!(
            store.multi_get_raw_async(&keys).await,
            vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]
        );

        let mut ok_batch = WriteBatch::new();
        ok_batch.put(b"cas", b"ok");
        store
            .compare_and_write_batch_async(
                &[CompareCondition::with_expected(
                    b"a".to_vec(),
                    Some(b"1".to_vec()),
                )],
                &ok_batch,
            )
            .await
            .unwrap();
        assert_eq!(store.get_raw(b"cas"), Some(b"ok".to_vec()));

        let mut failed_batch = WriteBatch::new();
        failed_batch.put(b"cas", b"bad");
        assert!(
            store
                .compare_and_write_batch_async(
                    &[CompareCondition::with_expected(
                        b"a".to_vec(),
                        Some(b"wrong".to_vec())
                    )],
                    &failed_batch,
                )
                .await
                .is_err()
        );
        assert_eq!(store.get_raw(b"cas"), Some(b"ok".to_vec()));
    }

    #[tokio::test]
    async fn range_scan_visit_delete_range_and_direct_batches_cover_sync_and_async() {
        let store = test_store();
        for idx in 0..5 {
            store.put_raw(format!("p:{idx}").as_bytes(), format!("v{idx}").as_bytes());
        }
        store.put_raw(b"q:0", b"out");

        let scan = store.scan_prefix_raw(b"p:");
        assert_eq!(scan.len(), 5);
        assert!(scan.iter().all(|(key, _)| key.starts_with(b"p:")));

        let async_scan = store.scan_prefix_raw_async(b"p:").await;
        assert_eq!(async_scan.len(), 5);

        let limited = store.scan_range_raw_limited(b"p:", Some(b"p;".to_vec()), 3);
        assert_eq!(limited.len(), 3);
        assert_eq!(
            store
                .scan_range_raw_limited_async(b"p:", Some(b"p;".to_vec()), 2)
                .await
                .len(),
            2
        );

        let visited = store
            .scan_range_raw_visit_async(b"p:", Some(b"p;".to_vec()), 10, |key, _| key != b"p:2")
            .await;
        assert_eq!(visited, 3);
        assert_eq!(
            store
                .scan_range_raw_visit_async(b"p:", Some(b"p;".to_vec()), 0, |_, _| true)
                .await,
            0
        );

        let mut direct = WriteBatch::new();
        direct.put(b"direct:sync", b"1");
        store.write_batch_direct(&direct);
        assert_eq!(store.get_raw(b"direct:sync"), Some(b"1".to_vec()));

        let mut direct_async = WriteBatch::new();
        direct_async.put(b"direct:async", b"2");
        store.write_batch_direct_async(direct_async).await;
        assert_eq!(store.get_raw(b"direct:async"), Some(b"2".to_vec()));

        let mut async_batch = WriteBatch::new();
        async_batch.put(b"async:put", b"3");
        async_batch.delete(b"direct:sync");
        store.write_batch_async(&async_batch).await;
        assert_eq!(store.get_raw(b"async:put"), Some(b"3".to_vec()));
        assert_eq!(store.get_raw(b"direct:sync"), None);

        store.delete_range(b"p:", b"p;");
        assert!(store.scan_prefix_raw(b"p:").is_empty());
        assert_eq!(store.get_raw(b"q:0"), Some(b"out".to_vec()));
        assert!(!store.delete_key(b"missing"));
    }

    #[tokio::test]
    async fn transaction_commit_discard_scan_and_batch_paths_work() {
        let store = test_store();
        store.put_raw(b"base", b"old");

        let txn = store.begin_transaction().unwrap();
        assert!(txn.is_transactional());
        assert!(!store.is_transactional());
        txn.put_raw(b"base", b"new");
        txn.put_raw(b"txn:1", b"a");
        txn.put_raw(b"txn:2", b"b");
        assert_eq!(txn.get_raw(b"base"), Some(b"new".to_vec()));
        assert_eq!(store.get_raw(b"base"), Some(b"old".to_vec()));
        assert!(txn.contains_key(b"txn:1"));
        assert_eq!(
            txn.multi_get_raw(&[b"txn:1".to_vec(), b"missing".to_vec()]),
            vec![Some(b"a".to_vec()), None]
        );
        txn.commit_transaction().unwrap();
        txn.commit_transaction().unwrap();
        assert_eq!(store.get_raw(b"base"), Some(b"new".to_vec()));

        let txn = store.begin_transaction().unwrap();
        txn.put_raw(b"discarded", b"value");
        txn.discard_transaction();
        txn.discard_transaction();
        assert_eq!(store.get_raw(b"discarded"), None);

        let txn = store.begin_transaction().unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"batched", b"value");
        batch.delete(b"base");
        txn.write_batch(&batch);
        txn.commit_transaction_async().await.unwrap();
        txn.commit_transaction_async().await.unwrap();
        assert_eq!(store.get_raw(b"batched"), Some(b"value".to_vec()));
        assert_eq!(store.get_raw(b"base"), None);

        let view = txn.non_transactional_view();
        assert!(!view.is_transactional());
        assert_eq!(view.get_raw(b"batched"), Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn transaction_async_read_observe_and_commit_paths_work() {
        let store = test_store();
        let txn = store.begin_transaction().unwrap();
        txn.put_raw(b"async:txn", b"value");
        assert_eq!(
            txn.get_raw_async(b"async:txn").await,
            Some(b"value".to_vec())
        );
        assert_eq!(
            txn.get_raw_bytes_async(b"async:txn")
                .await
                .unwrap()
                .as_ref(),
            b"value"
        );
        assert!(txn.contains_key_async(b"async:txn").await);
        assert_eq!(
            txn.multi_get_raw_async(&[b"async:txn".to_vec(), b"missing".to_vec()])
                .await,
            vec![Some(b"value".to_vec()), None]
        );
        let observed = txn.get_raw_observed_async(b"async:txn").await;
        assert_eq!(observed.value().map(Bytes::as_ref), Some(&b"value"[..]));
        assert!(txn
            .observe_raw_key_state_async(b"async:txn")
            .await
            .exists());
        txn.commit_transaction_async().await.unwrap();
        assert_eq!(store.get_raw(b"async:txn"), Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn transaction_async_scans_visits_delete_range_and_compare_write_are_isolated_until_commit()
     {
        let store = test_store();
        store.put_raw(b"txnscan:0", b"old");
        store.put_raw(b"txnscan:outside", b"outside");

        let txn = store.begin_transaction().unwrap();
        txn.put_raw(b"txnscan:0", b"v0");
        txn.put_raw(b"txnscan:1", b"v1");
        txn.put_raw(b"txnscan:2", b"v2");
        txn.put_raw(b"txnscan:stop", b"stop");

        let prefix_entries = txn.scan_prefix_raw_async(b"txnscan:").await;
        assert!(
            prefix_entries
                .iter()
                .any(|(key, value)| key == b"txnscan:1" && value == b"v1")
        );

        assert!(
            txn.scan_range_raw_limited(b"txnscan:", Some(b"txnscan;".to_vec()), 0)
                .is_empty()
        );
        let range_entries = txn.scan_range_raw_limited(b"txnscan:", Some(b"txnscan;".to_vec()), 2);
        assert_eq!(range_entries.len(), 2);
        let async_range_entries = txn
            .scan_range_raw_limited_async(b"txnscan:", Some(b"txnscan;".to_vec()), 3)
            .await;
        assert_eq!(async_range_entries.len(), 3);

        let visited = txn
            .scan_range_raw_visit_async(b"txnscan:", Some(b"txnscan;".to_vec()), 10, |key, _| {
                key != b"txnscan:stop"
            })
            .await;
        assert!(visited >= 4);
        assert_eq!(
            txn.scan_range_raw_visit_async(b"txnscan:", Some(b"txnscan;".to_vec()), 2, |_, _| {
                true
            })
            .await,
            2
        );

        let mut compare_batch = WriteBatch::new();
        compare_batch.put(b"txnscan:compare", b"ok");
        txn.compare_and_write_batch_async(
            &[CompareCondition::with_expected(
                b"txnscan:0".to_vec(),
                Some(b"v0".to_vec()),
            )],
            &compare_batch,
        )
        .await
        .unwrap();
        assert_eq!(txn.get_raw(b"txnscan:compare"), Some(b"ok".to_vec()));
        assert_eq!(store.get_raw(b"txnscan:compare"), None);

        let observed = txn.get_raw_observed_async(b"txnscan:0").await;
        txn.put_raw(b"txnscan:0", b"changed-after-observe");
        let mut stale_batch = WriteBatch::new();
        stale_batch.put(b"txnscan:stale-cas", b"must-not-write");
        assert!(matches!(
            txn.compare_and_write_batch_async(&[observed.condition()], &stale_batch)
                .await,
            Err(Status::ConditionFailed(_))
        ));
        assert_eq!(txn.get_raw(b"txnscan:stale-cas"), None);
        txn.put_raw(b"txnscan:0", b"v0");

        txn.delete_range(b"txnscan:1", b"txnscan:3");
        assert_eq!(txn.get_raw(b"txnscan:1"), None);
        assert_eq!(txn.get_raw(b"txnscan:2"), None);
        txn.commit_transaction_async().await.unwrap();

        assert_eq!(store.get_raw(b"txnscan:0"), Some(b"v0".to_vec()));
        assert_eq!(store.get_raw(b"txnscan:1"), None);
        assert_eq!(store.get_raw(b"txnscan:2"), None);
        assert_eq!(store.get_raw(b"txnscan:compare"), Some(b"ok".to_vec()));
        assert_eq!(store.get_raw(b"txnscan:outside"), Some(b"outside".to_vec()));
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
                    store.put_raw(key.as_bytes(), b"value");
                    assert!(store.contains_key_async(key.as_bytes()).await);
                    wrote.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(wrote.load(Ordering::Relaxed), 200);
        assert_eq!(store.scan_prefix_raw(b"concurrent:").len(), 200);

        store.merge_raw(b"counter", &5i64.to_be_bytes());
        store.merge_raw(b"counter", &7i64.to_be_bytes());
        store
            .merge_raw_async(b"counter", &(-2i64).to_be_bytes())
            .await;
        let encoded = store.get_raw(b"counter").unwrap();
        assert_eq!(encoded[0..8], 0u64.to_be_bytes());
        assert_eq!(encoded[16], OnedisIntegerMergeOperator::TYPE_STRING);
        assert_eq!(&encoded[17..], b"10");

        let mut existing = OnedisIntegerMergeOperator::encode_string(9, 12345);
        store.put_raw(b"counter:ttl", &existing);
        store.merge_raw(b"counter:ttl", &1i64.to_be_bytes());
        existing = store.get_raw(b"counter:ttl").unwrap();
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
    }
}
