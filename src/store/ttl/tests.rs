    use super::*;
    use crate::store::db::Db;
    use std::sync::atomic::Ordering;

    fn test_store() -> KvStore {
        let unique = format!(
            "onedis-ttl-test-{}",
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

    fn regular_meta(expire_ms: u64, version: u64, type_tag: u8) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&expire_ms.to_be_bytes());
        raw.extend_from_slice(&version.to_be_bytes());
        raw.push(type_tag);
        raw.extend_from_slice(b"payload");
        raw
    }

    fn list_meta(expire_ms: u64, version: u64) -> Vec<u8> {
        let mut raw = Vec::with_capacity(36);
        raw.extend_from_slice(&LIST_META_MAGIC);
        raw.extend_from_slice(&expire_ms.to_be_bytes());
        raw.extend_from_slice(&version.to_be_bytes());
        raw.extend_from_slice(&0i64.to_be_bytes());
        raw.extend_from_slice(&1i64.to_be_bytes());
        raw
    }

    fn stream_meta(expire_ms: u64, version: u64) -> Vec<u8> {
        let mut raw = Vec::with_capacity(52);
        raw.extend_from_slice(&STREAM_META_MAGIC);
        raw.extend_from_slice(&expire_ms.to_be_bytes());
        raw.extend_from_slice(&version.to_be_bytes());
        raw.extend_from_slice(&0u64.to_be_bytes());
        raw.extend_from_slice(&0u64.to_be_bytes());
        raw.extend_from_slice(&0u64.to_be_bytes());
        raw.extend_from_slice(&0u64.to_be_bytes());
        raw
    }

    // -------------------------------------------------------------- TTL keys

    #[test]
    fn ttl_index_key_round_trips() {
        let key = ttl_index_key(1234, 7, "session");
        assert_eq!(parse_ttl_index_key(&key), Some((1234, 7, "session".into())));
    }

    #[test]
    fn ttl_index_key_orders_by_db_then_expire_ms() {
        let early = ttl_index_key(100, 0, "a");
        let later = ttl_index_key(200, 0, "a");
        let next_db = ttl_index_key(50, 1, "a");

        assert!(early < later);
        assert!(later < next_db);
    }

    #[test]
    fn ttl_db_expire_upper_bound_excludes_future_deadlines() {
        let upper = ttl_db_expire_upper_bound(0, 100);
        assert!(ttl_index_key(100, 0, "a") < upper);
        assert!(ttl_index_key(101, 0, "a") >= upper);
    }

    #[test]
    fn ttl_config_rejects_zero_values() {
        assert!(
            std::panic::catch_unwind(|| {
                TtlManager::new(
                    test_store(),
                    TtlConfig {
                        sweep_interval_ms: 0,
                        batch_size: 1,
                    },
                )
            })
            .is_err()
        );
        assert!(
            std::panic::catch_unwind(|| {
                TtlManager::new(
                    test_store(),
                    TtlConfig {
                        sweep_interval_ms: 1,
                        batch_size: 0,
                    },
                )
            })
            .is_err()
        );
    }

    // -------------------------------------------------------- VersionCounter

    #[test]
    fn version_counter_monotonic() {
        let vc = VersionCounter::new();
        let first = vc.next();
        let second = vc.next();
        let third = vc.next();
        assert!(first < second && second < third);
    }

    #[test]
    fn version_counter_observe() {
        let vc = VersionCounter::new();
        vc.observe(100);
        let first = vc.next();
        assert!(first > 100);
        vc.observe(50); // should not downgrade
        assert!(vc.next() > first);
    }

    #[test]
    fn version_counter_allocates_monotonically_without_persistence_callback() {
        let vc = VersionCounter::new();
        let first = vc.next();
        assert!(first > 0);
        let second = vc.next();
        assert!(second > first);
        assert_eq!(vc.current(), second);
    }

    // --------------------------------------------------------- MetaHeader

    #[test]
    fn decode_regular_meta_header() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1234u64.to_be_bytes()); // expire_ms
        raw.extend_from_slice(&42u64.to_be_bytes()); // version
        raw.push(TYPE_HASH); // type_tag
        raw.extend_from_slice(&[0u8; 16]); // dummy bincode

        let h = decode_meta_header(&raw).unwrap();
        assert_eq!(h.expire_ms, 1234);
        assert_eq!(h.version, 42);
        assert_eq!(h.type_tag, TYPE_HASH);
    }

    #[test]
    fn decode_list_meta_header() {
        let mut raw = Vec::with_capacity(36);
        raw.extend_from_slice(&LIST_META_MAGIC);
        raw.extend_from_slice(&999u64.to_be_bytes()); // expire_ms
        raw.extend_from_slice(&7u64.to_be_bytes()); // version
        raw.extend_from_slice(&(-3i64).to_be_bytes()); // head
        raw.extend_from_slice(&5i64.to_be_bytes()); // tail

        let h = decode_meta_header(&raw).unwrap();
        assert_eq!(h.expire_ms, 999);
        assert_eq!(h.version, 7);
        assert_eq!(h.type_tag, TYPE_LIST);
    }

    #[test]
    fn too_short_returns_none() {
        assert!(decode_meta_header(&[0u8; 10]).is_none());
    }

    #[test]
    fn stream_meta_and_patch_expire_ms_cover_all_meta_shapes() {
        let stream = stream_meta(10, 3);
        assert_eq!(
            decode_meta_header(&stream),
            Some(MetaHeader {
                expire_ms: 10,
                version: 3,
                type_tag: TYPE_STREAM,
            })
        );

        let patched_regular = patch_meta_expire_ms(&regular_meta(1, 2, TYPE_SET), 99).unwrap();
        assert_eq!(decode_meta_header(&patched_regular).unwrap().expire_ms, 99);

        let patched_list = patch_meta_expire_ms(&list_meta(1, 2), 88).unwrap();
        assert_eq!(decode_meta_header(&patched_list).unwrap().expire_ms, 88);

        let patched_stream = patch_meta_expire_ms(&stream, 77).unwrap();
        assert_eq!(decode_meta_header(&patched_stream).unwrap().expire_ms, 77);

        assert!(patch_meta_expire_ms(&[1, 2, 3, 4], 55).is_none());
    }

    // ----------------------------------------------------- sub-key ranges

    #[test]
    fn sub_key_in_range() {
        let encoding = TtlKeyEncoding::current();
        let start = sub_key_range_start(encoding, 0, &HASH_FIELD_NS, "k", 5);
        let end = sub_key_range_end(encoding, 0, &HASH_FIELD_NS, "k", 5);

        // A sub-key for version 5 + field "abc" must be in [start, end)
        let mut mid = start.clone();
        mid.extend_from_slice(b"abc");
        assert!(mid >= start && mid < end);

        // Version 6 is out of range
        let v6 = sub_key_range_start(encoding, 0, &HASH_FIELD_NS, "k", 6);
        assert!(v6 >= end);
    }

    #[test]
    fn nul_key_owner_encoding_cannot_overlap_plain_key_range() {
        let encoding = TtlKeyEncoding::current();
        let plain_start = sub_key_range_start(encoding, 0, &HASH_FIELD_NS, "a", 1);
        let plain_end = sub_key_range_end(encoding, 0, &HASH_FIELD_NS, "a", 1);
        let mut nested_key = b"a\0".to_vec();
        nested_key.extend_from_slice(&1u64.to_be_bytes());
        nested_key.extend_from_slice(b"x");
        let nested_key = String::from_utf8(nested_key).unwrap();
        let nested_start =
            sub_key_range_start(encoding, 0, &HASH_FIELD_NS, &nested_key, 2);

        assert!(nested_start < plain_start || nested_start >= plain_end);
    }

    #[test]
    fn delete_batch_string_is_noop() {
        let mut batch = WriteBatch::new();
        delete_sub_keys_to_batch_with_encoding(
            &mut batch,
            TtlKeyEncoding::current(),
            0,
            "k",
            1,
            TYPE_STRING,
        );
        assert_eq!(batch.count(), 0);
    }

    #[test]
    fn delete_batch_hash_one_range() {
        let mut batch = WriteBatch::new();
        delete_sub_keys_to_batch_with_encoding(
            &mut batch,
            TtlKeyEncoding::current(),
            0,
            "k",
            1,
            TYPE_HASH,
        );
        assert_eq!(batch.count(), 2);
    }

    #[test]
    fn delete_batch_zset_two_ranges() {
        let mut batch = WriteBatch::new();
        delete_sub_keys_to_batch_with_encoding(
            &mut batch,
            TtlKeyEncoding::current(),
            0,
            "k",
            1,
            TYPE_SORTED_SET,
        );
        assert_eq!(batch.count(), 2);
    }

    #[test]
    fn delete_batch_covers_list_stream_json_vector_and_unknown_types() {
        let cases = [
            (TYPE_LIST, 1),
            (TYPE_STREAM, 4),
            (TYPE_JSON, 2),
            (TYPE_VECTOR, 6),
            (99, 0),
        ];
        for (type_tag, expected_count) in cases {
            let mut batch = WriteBatch::new();
            delete_sub_keys_to_batch_with_encoding(
                &mut batch,
                TtlKeyEncoding::current(),
                2,
                "key",
                9,
                type_tag,
            );
            assert_eq!(batch.count(), expected_count, "type_tag={type_tag}");
        }
    }

    #[tokio::test]
    async fn version_counter_concurrent_allocations_are_unique() {
        let vc = std::sync::Arc::new(VersionCounter::new());
        let first = vc.next();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let vc = vc.clone();
            tasks.push(tokio::spawn(async move { vc.next() }));
        }
        let mut versions = Vec::new();
        for task in tasks {
            versions.push(task.await.unwrap());
        }
        versions.sort_unstable();
        versions.dedup();
        assert_eq!(versions.len(), 16);
        assert!(vc.current() >= first + 16);
    }

    #[test]
    fn ttl_manager_add_remove_db_rebuild_and_parse_edges_work() {
        let store = test_store();
        let db0_store = store.for_db_index(0);
        let db1_store = store.for_db_index(1);
        let manager = TtlManager::new(store.clone(), TtlConfig::default());

        manager.add(0, 0, "ignored".to_string());
        assert_eq!(manager.index_size(), 0);

        manager.add(100, 0, "db0-a".to_string());
        manager.add(200, 0, "db0-b".to_string());
        manager.add(150, 1, "db1-a".to_string());
        assert_eq!(manager.index_size(), 3);

        let mut db0_deletes = WriteBatch::new();
        manager.remove_db_to_batch(&mut db0_deletes, 0);
        assert_eq!(db0_deletes.count(), 2);
        db0_store.write_batch(&db0_deletes);
        assert_eq!(manager.index_size(), 1);

        let mut known = WriteBatch::new();
        manager.remove_known_to_batch(&mut known, 0, 1, "ignored");
        assert_eq!(known.count(), 0);
        manager.remove_known_to_batch(&mut known, 150, 1, "db1-a");
        assert_eq!(known.count(), 1);
        db1_store.write_batch(&known);
        assert_eq!(manager.index_size(), 0);

        assert!(parse_ttl_index_key(b"not-a-ttl-key").is_none());
        let mut short = ttl_db_prefix(0);
        short.push(1);
        assert!(parse_ttl_index_key(&short).is_none());
        let mut invalid_utf8 = ttl_index_key(1, 0, "ok");
        *invalid_utf8.last_mut().unwrap() = 0xff;
        assert!(parse_ttl_index_key(&invalid_utf8).is_none());

        let mut owner_key = crate::store::db::version_owner_prefix(0);
        owner_key.extend_from_slice(&99u64.to_be_bytes());
        db0_store.put_raw(&owner_key, b"owner");

        let vc = VersionCounter::new();
        manager.rebuild_from_store(2, &vc);
        assert_eq!(vc.current(), 99);
    }

    #[tokio::test]
    async fn ttl_manager_async_remove_rebuild_index_size_and_empty_sweep_work() {
        let store = test_store();
        let db0_store = store.for_db_index(0);
        let manager = TtlManager::new(
            store.clone(),
            TtlConfig {
                sweep_interval_ms: 10,
                batch_size: 2,
            },
        );
        manager.add(100, 0, "a".to_string());
        manager.add(200, 0, "b".to_string());
        assert_eq!(manager.index_size_async().await, 2);

        let mut deletes = WriteBatch::new();
        manager.remove_db_to_batch_async(&mut deletes, 0).await;
        assert_eq!(deletes.count(), 2);
        db0_store.write_batch(&deletes);
        assert_eq!(manager.index_size_async().await, 0);

        let mut owner_key = crate::store::db::version_owner_prefix(0);
        owner_key.extend_from_slice(&30u64.to_be_bytes());
        db0_store.put_raw(&owner_key, b"owner");
        let vc = VersionCounter::new();
        manager.rebuild_from_store_async(1, &vc).await;
        assert_eq!(vc.current(), 30);
        assert!(!manager.sweep_once_async().await);
        assert_eq!(manager.stats().sweep_cycles.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn ttl_expire_observer_runs_only_after_committed_delete() {
        let store = test_store();
        let db_store = store.for_db_index(0);
        let manager = TtlManager::new(store, TtlConfig::default());
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed_from_hook = observed.clone();
        manager.set_expire_observer(Arc::new(move |db_index, key, type_tag, version| {
            observed_from_hook.lock().unwrap().push((
                db_index,
                key.to_string(),
                type_tag,
                version,
            ));
        }));
        let expired = now_ms().saturating_sub(1);
        db_store.put_raw(
            &main_key(0, "vector-expired"),
            &regular_meta(expired, 77, TYPE_VECTOR),
        );
        manager.add(expired, 0, "vector-expired".to_string());

        assert!(!manager.sweep_once_async().await);
        assert!(db_store.get_raw(&main_key(0, "vector-expired")).is_none());
        assert_eq!(
            *observed.lock().unwrap(),
            vec![(0, "vector-expired".to_string(), TYPE_VECTOR, 77)]
        );
    }

    #[tokio::test]
    async fn ttl_scan_is_deadline_bounded_and_rotates_between_databases() {
        let store = test_store();
        let db0_store = store.for_db_index(0);
        let db1_store = store.for_db_index(1);
        let manager = TtlManager::new(
            store,
            TtlConfig {
                sweep_interval_ms: 10,
                batch_size: 1,
            },
        );
        let expired = now_ms().saturating_sub(1);
        let future = now_ms().saturating_add(60_000);

        for key in ["db0-a", "db0-b"] {
            db0_store.put_raw(&main_key(0, key), &regular_meta(expired, 1, TYPE_STRING));
            manager.add(expired, 0, key.to_string());
        }
        db0_store.put_raw(
            &main_key(0, "db0-future"),
            &regular_meta(future, 1, TYPE_STRING),
        );
        manager.add(future, 0, "db0-future".to_string());
        db1_store.put_raw(
            &main_key(1, "db1-expired"),
            &regular_meta(expired, 1, TYPE_STRING),
        );
        manager.add(expired, 1, "db1-expired".to_string());

        let first = manager.scan_expired_batch_async(expired, 1).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].db_index, 0);

        assert!(manager.sweep_once_async().await);
        assert_eq!(db1_store.get_raw(&main_key(1, "db1-expired")), None);
        assert_eq!(
            db0_store.get_raw(&main_key(0, "db0-future")),
            Some(regular_meta(future, 1, TYPE_STRING))
        );
    }

    #[tokio::test]
    async fn ttl_sweeper_drains_backlog_without_waiting_between_full_batches() {
        let store = test_store();
        let db0_store = store.for_db_index(0);
        let manager = TtlManager::new(
            store,
            TtlConfig {
                sweep_interval_ms: 100,
                batch_size: 1,
            },
        );
        let expired = now_ms().saturating_sub(1);
        for key in ["backlog-a", "backlog-b", "backlog-c"] {
            db0_store.put_raw(&main_key(0, key), &regular_meta(expired, 1, TYPE_STRING));
            manager.add(expired, 0, key.to_string());
        }

        let sweeper = manager.start_sweeper();
        tokio::time::sleep(Duration::from_millis(150)).await;
        manager.shutdown();
        sweeper.await.unwrap();

        assert_eq!(manager.index_size(), 0);
        for key in ["backlog-a", "backlog-b", "backlog-c"] {
            assert_eq!(db0_store.get_raw(&main_key(0, key)), None);
        }
    }

    #[tokio::test]
    async fn ttl_sweep_uses_the_persisted_legacy_key_layout() {
        let store = test_store();
        let db0_store = store.for_db_index(0);
        db0_store.put_raw(b"\x80m\x00layout", b"db-prefixed-v1");
        let versions = Arc::new(VersionCounter::new());
        let manager = TtlManager::new(
            store.clone(),
            TtlConfig {
                sweep_interval_ms: 10,
                batch_size: 10,
            },
        );
        let db = Db::new(0, store, versions, manager.clone());
        db.insert_string("legacy".to_string(), "value".to_string(), Some(1));
        tokio::time::sleep(Duration::from_millis(5)).await;

        assert!(!manager.sweep_once_async().await);
        let mut legacy_main_key = 0u16.to_be_bytes().to_vec();
        legacy_main_key.extend_from_slice(b"legacy");
        assert_eq!(db0_store.get_raw(&legacy_main_key), None);
        assert_eq!(manager.index_size(), 0);
    }

    #[tokio::test]
    async fn ttl_sweep_of_plain_key_preserves_a_distinct_nul_key() {
        let store = test_store();
        let versions = Arc::new(VersionCounter::new());
        let manager = TtlManager::new(
            store.clone(),
            TtlConfig {
                sweep_interval_ms: 10,
                batch_size: 10,
            },
        );
        let db = Db::new(0, store, versions.clone(), manager.clone());

        db.hash_set("a", "field", "source").unwrap();
        let nested_key = "a\0nested".to_string();
        db.hash_set(&nested_key, "field", "nested").unwrap();

        assert!(db.expire("a".to_string(), 1));
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(!manager.sweep_once_async().await);

        assert_eq!(db.hash_get("a", "field").unwrap(), None);
        assert_eq!(
            db.hash_get(&nested_key, "field").unwrap(),
            Some("nested".to_string())
        );
    }

    #[tokio::test]
    async fn ttl_sweep_deletes_valid_expired_keys_and_skips_not_found_stale_and_hook_rejected() {
        let store = test_store();
        let db0_store = store.for_db_index(0);
        let manager = TtlManager::new(
            store.clone(),
            TtlConfig {
                sweep_interval_ms: 10,
                batch_size: 10,
            },
        );
        let expired = now_ms().saturating_sub(1000);
        let future = now_ms().saturating_add(60_000);

        db0_store.put_raw(&main_key(0, "valid"), &regular_meta(expired, 1, TYPE_HASH));
        manager.add(expired, 0, "valid".to_string());

        manager.add(expired, 0, "missing".to_string());

        db0_store.put_raw(&main_key(0, "stale"), &regular_meta(future, 2, TYPE_SET));
        manager.add(expired, 0, "stale".to_string());

        db0_store.put_raw(&main_key(0, "bad-meta"), b"short");
        manager.add(expired, 0, "bad-meta".to_string());

        db0_store.put_raw(&main_key(0, "hooked"), &regular_meta(expired, 3, TYPE_LIST));
        manager.add(expired, 0, "hooked".to_string());
        manager.set_expire_hook(std::sync::Arc::new(|_, key, _, _| {
            if key == "hooked" {
                return false;
            }
            true
        }));

        assert!(!manager.sweep_once_async().await);
        assert_eq!(db0_store.get_raw(&main_key(0, "valid")), None);
        assert_eq!(db0_store.get_raw(&main_key(0, "missing")), None);
        assert_eq!(
            db0_store.get_raw(&main_key(0, "stale")),
            Some(regular_meta(future, 2, TYPE_SET))
        );
        assert_eq!(
            db0_store.get_raw(&main_key(0, "bad-meta")),
            Some(b"short".to_vec())
        );
        assert_eq!(
            db0_store.get_raw(&main_key(0, "hooked")),
            Some(regular_meta(expired, 3, TYPE_LIST))
        );
        assert_eq!(db0_store.get_raw(b"hook:called"), None);
        assert_eq!(manager.stats().keys_expired.load(Ordering::Relaxed), 1);
        assert_eq!(
            manager
                .stats()
                .stale_entries_skipped
                .load(Ordering::Relaxed),
            4
        );
        assert_eq!(manager.index_size(), 1);
    }

    #[tokio::test]
    async fn ttl_async_sweep_honors_batch_size_and_defers_json_cleanup_to_compaction() {
        let store = test_store();
        let db0_store = store.for_db_index(0);
        let manager = TtlManager::new(
            store.clone(),
            TtlConfig {
                sweep_interval_ms: 10,
                batch_size: 1,
            },
        );
        let expired = now_ms().saturating_sub(1000);
        for key in ["json-a", "json-b"] {
            db0_store.put_raw(&main_key(0, key), &regular_meta(expired, 8, TYPE_JSON));
            let mut node_key = json_node_prefix(0, key, 8);
            node_key.extend_from_slice(b":node");
            db0_store.put_raw(&node_key, b"node");
            manager.add(expired, 0, key.to_string());
        }

        assert!(manager.sweep_once_async().await);
        assert_eq!(manager.stats().keys_expired.load(Ordering::Relaxed), 1);
        assert_eq!(manager.index_size(), 1);

        assert!(manager.sweep_once_async().await);
        assert!(!manager.sweep_once_async().await);
        assert_eq!(manager.stats().keys_expired.load(Ordering::Relaxed), 2);
        assert_eq!(manager.index_size(), 0);
        assert!(
            !db0_store
                .scan_prefix_raw(&json_node_prefix(0, "json-a", 8))
                .is_empty()
        );
        assert!(
            !db0_store
                .scan_prefix_raw(&json_node_prefix(0, "json-b", 8))
                .is_empty()
        );

        store.mark_version_compaction_ready();
        store.manual_compaction().unwrap();
        assert!(
            db0_store
                .scan_prefix_raw(&json_node_prefix(0, "json-a", 8))
                .is_empty()
        );
        assert!(
            db0_store
                .scan_prefix_raw(&json_node_prefix(0, "json-b", 8))
                .is_empty()
        );
    }

    #[tokio::test]
    async fn ttl_async_sweep_does_not_delete_a_concurrently_replaced_value() {
        let store = test_store();
        let db0_store = store.for_db_index(0);
        let manager = TtlManager::new(
            store,
            TtlConfig {
                sweep_interval_ms: 10,
                batch_size: 10,
            },
        );
        let expired = now_ms().saturating_sub(1000);
        let replacement_expire = now_ms().saturating_add(60_000);
        db0_store.put_raw(
            &main_key(0, "raced"),
            &regular_meta(expired, 1, TYPE_HASH),
        );
        manager.add(expired, 0, "raced".to_string());

        let hook_store = db0_store.clone();
        manager.set_expire_hook(std::sync::Arc::new(move |db_index, key, _, _| {
            if key == "raced" {
                hook_store.put_raw(
                    &main_key(db_index, key),
                    &regular_meta(replacement_expire, 2, TYPE_HASH),
                );
            }
            true
        }));

        assert!(!manager.sweep_once_async().await);
        assert_eq!(
            db0_store.get_raw(&main_key(0, "raced")),
            Some(regular_meta(replacement_expire, 2, TYPE_HASH))
        );
        assert_eq!(manager.stats().keys_expired.load(Ordering::Relaxed), 0);
        assert_eq!(
            manager
                .stats()
                .stale_entries_skipped
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(manager.index_size(), 1);
    }
