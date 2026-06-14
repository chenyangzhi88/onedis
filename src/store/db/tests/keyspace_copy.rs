    #[test]
    fn repeated_expire_appends_ttl_index_entries() {
        let db = test_db();

        db.insert("hot-key".to_string(), Structure::String("v".to_string()));
        assert!(db.expire("hot-key".to_string(), 10_000));
        assert!(db.expire("hot-key".to_string(), 20_000));
        assert!(db.expire("hot-key".to_string(), 30_000));

        assert_eq!(db.ttl_manager.index_size(), 3);
        assert!(db.ttl_millis("hot-key") > 20_000);
    }

    #[test]
    fn persist_removes_ttl_index_entry() {
        let db = test_db();

        db.insert("session".to_string(), Structure::String("v".to_string()));
        assert!(db.expire("session".to_string(), 10_000));
        assert_eq!(db.ttl_manager.index_size(), 1);

        assert!(db.persist("session"));
        assert_eq!(db.ttl_manager.index_size(), 0);
        assert_eq!(db.ttl_millis("session"), -1);
    }

    #[test]
    fn ttl_rebuild_loads_persisted_ttl_namespace() {
        let db = test_db();

        db.insert(
            "persisted-ttl".to_string(),
            Structure::String("v".to_string()),
        );
        assert!(db.expire("persisted-ttl".to_string(), 10_000));

        let rebuilt = TtlManager::new(db.store.clone(), TtlConfig::default());
        let recovered_counter = VersionCounter::new();
        rebuilt.rebuild_from_store(1, &recovered_counter);

        assert_eq!(rebuilt.index_size(), 1);
        assert!(recovered_counter.current() >= db.version_counter.current());
    }

    #[test]
    fn transaction_ttl_index_is_published_after_commit() {
        let db = test_db();
        let txn_db = db.transactional_view().unwrap();

        txn_db.insert_string("txn-ttl".to_string(), "value".to_string(), Some(10_000));

        assert_eq!(db.ttl_manager.index_size(), 0);
        txn_db.commit_transaction().unwrap();
        assert_eq!(db.ttl_manager.index_size(), 1);
    }

    #[test]
    fn set_string_over_hash_removes_old_subkeys() {
        let db = test_db();

        db.hash_set("mixed", "field", "value").unwrap();
        let raw = db.store.get_raw(&db.mk("mixed")).unwrap();
        let header = super::decode_meta_header(&raw).unwrap();
        let field_key = hash_field_key(db.db_index, "mixed", header.version, "field");
        assert!(db.store.contains_key(&field_key));

        db.insert_string_ref("mixed", "plain");

        assert!(!db.store.contains_key(&field_key));
        assert!(matches!(
            db.get("mixed"),
            Some(Structure::String(value)) if value == "plain"
        ));
    }

    #[test]
    fn move_key_between_dbs_moves_full_structure() {
        let root = test_root("onedis-move-test");
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);

        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());

        let db0 = Db::new(
            0,
            store.clone(),
            version_counter.clone(),
            ttl_manager.clone(),
        );
        let db1 = Db::new(
            1,
            store.clone(),
            version_counter.clone(),
            ttl_manager.clone(),
        );
        db0.zset_add(
            "leaders",
            &[(1.0, "alice".to_string()), (2.0, "bob".to_string())],
        )
        .unwrap();

        assert!(
            Db::move_key_between_dbs(&store, 0, "leaders", 1, "leaders", &version_counter, None,)
                .unwrap()
        );

        assert!(db0.get("leaders").is_none());
        assert!(matches!(
            db1.get("leaders"),
            Some(Structure::SortedSet(value)) if value.get("alice") == Some(&1.0) && value.get("bob") == Some(&2.0)
        ));
    }

    #[tokio::test]
    async fn async_copy_key_to_db_copies_full_zset_structure() {
        let root = test_root("onedis-copy-async-test");
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        let db0 = Db::new(
            0,
            store.clone(),
            version_counter.clone(),
            ttl_manager.clone(),
        );
        let db1 = Db::new(1, store, version_counter, ttl_manager);

        db0.zset_add(
            "leaders",
            &[(1.0, "alice".to_string()), (2.0, "bob".to_string())],
        )
        .unwrap();

        assert!(
            db0.copy_key_to_db_async(1, "leaders", "leaders-copy", false)
                .await
                .unwrap()
        );
        assert_eq!(
            db1.zset_range_async("leaders-copy", 0, -1, false)
                .await
                .unwrap(),
            vec![("alice".to_string(), 1.0), ("bob".to_string(), 2.0)]
        );
    }

    #[tokio::test]
    async fn copy_move_rename_and_remove_cover_complex_structure_namespaces() {
        let root = test_root("onedis-complex-copy-test");
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        let db0 = Db::new(
            0,
            store.clone(),
            version_counter.clone(),
            ttl_manager.clone(),
        );
        let db1 = Db::new(1, store, version_counter, ttl_manager);

        db0.hash_multi_set(
            "hash",
            &HashMap::from([
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ]),
        )
        .unwrap();
        assert!(db0.expire("hash".to_string(), 10_000));
        db0.list_push_right("list", &["x".to_string(), "y".to_string()], false)
            .unwrap();
        db0.stream_add(
            "stream",
            Some(StreamId { ms: 1, seq: 0 }),
            &[("f".to_string(), "v".to_string())],
        )
        .unwrap();
        db0.stream_group_create("stream", "g", StreamId { ms: 0, seq: 0 }, false)
            .unwrap();
        assert_eq!(
            db0.stream_read_group(
                "g",
                "consumer",
                &[("stream".to_string(), StreamReadGroupStart::New)],
                Some(1),
                false,
            )
            .unwrap()[0]
                .1
                .len(),
            1
        );
        db0.json_set(
            "json",
            "$",
            r#"{"a":1,"nested":{"b":2}}"#,
            SetCondition::Always,
        )
        .unwrap();

        assert!(db0.copy_key_to_db(1, "hash", "hash-copy", false).unwrap());
        assert_eq!(
            db1.hash_get("hash-copy", "a").unwrap(),
            Some("1".to_string())
        );
        assert!(db1.ttl_millis_readonly("hash-copy") > 0);

        assert!(
            db0.copy_key_to_db_async(1, "list", "list-copy", false)
                .await
                .unwrap()
        );
        assert_eq!(
            db1.list_range("list-copy", 0, -1).unwrap(),
            vec!["x".to_string(), "y".to_string()]
        );

        assert!(
            db0.copy_key_to_db_async(1, "stream", "stream-copy", false)
                .await
                .unwrap()
        );
        assert_eq!(db1.stream_len("stream-copy").unwrap(), 1);
        assert_eq!(
            db1.stream_read_group(
                "g",
                "consumer",
                &[(
                    "stream-copy".to_string(),
                    StreamReadGroupStart::Id(StreamId { ms: 0, seq: 0 })
                )],
                Some(1),
                false,
            )
            .unwrap()[0]
                .1
                .len(),
            1
        );

        assert!(db0.copy_key_to_db(1, "json", "json-copy", false).unwrap());
        assert_eq!(
            db1.json_get("json-copy", "$.nested.b").unwrap(),
            Some("2".to_string())
        );

        assert!(db0.move_key_to_db(1, "hash").unwrap());
        assert!(db0.hash_get("hash", "a").unwrap().is_none());
        assert_eq!(db1.hash_get("hash", "b").unwrap(), Some("2".to_string()));
        assert!(!db1.move_key_to_db_async(1, "hash").await.unwrap());

        db0.list_push_right(
            "rename-list",
            &["left".to_string(), "right".to_string()],
            false,
        )
        .unwrap();
        db0.insert_string_ref("rename-target", "old");
        assert!(
            db0.rename_key_async("rename-list", "rename-target", true)
                .await
                .unwrap()
        );
        assert_eq!(
            db0.list_range("rename-target", 0, -1).unwrap(),
            vec!["left".to_string(), "right".to_string()]
        );
        assert!(
            db0.rename_key_async("rename-target", "rename-target", false)
                .await
                .unwrap()
        );
        db0.insert_string_ref("rename-src", "v");
        db0.insert_string_ref("rename-existing", "e");
        assert!(
            !db0.rename_key_async("rename-src", "rename-existing", false)
                .await
                .unwrap()
        );
        assert!(db0.rename_key_async("missing", "x", true).await.is_err());

        db1.hash_multi_set(
            "remove-hash",
            &HashMap::from([("a".to_string(), "1".to_string())]),
        )
        .unwrap();
        db1.list_push_right("remove-list", &["x".to_string(), "y".to_string()], false)
            .unwrap();
        db1.stream_add(
            "remove-stream",
            Some(StreamId { ms: 1, seq: 0 }),
            &[("f".to_string(), "v".to_string())],
        )
        .unwrap();
        db1.json_set(
            "remove-json",
            "$",
            r#"{"nested":{"b":2}}"#,
            SetCondition::Always,
        )
        .unwrap();
        assert!(db1.remove("remove-hash").is_none());
        assert!(matches!(
            db1.remove_async("remove-list").await,
            Some(Structure::List(values)) if values == vec!["x".to_string(), "y".to_string()]
        ));
        assert!(matches!(
            db1.remove("remove-stream"),
            Some(Structure::Stream(entries)) if entries.len() == 1
        ));
        assert!(matches!(
            db1.remove_async("remove-json").await,
            Some(Structure::Json(_))
        ));
        db1.set_add("remove-set", &["a".to_string(), "b".to_string()])
            .unwrap();
        db1.zset_add("remove-zset", &[(1.0, "a".to_string())])
            .unwrap();
        assert!(db1.delete_key("remove-set"));
        assert!(db1.delete_key_async("remove-zset").await);
    }

    #[tokio::test]
    async fn static_copy_move_and_remove_helpers_cover_native_namespaces() {
        let db0 = test_db();
        let db1 = Db::new(
            1,
            db0.store.clone(),
            db0.version_counter.clone(),
            db0.ttl_manager.clone(),
        );

        db0.hash_set_ex(
            "hash",
            &[("field".to_string(), "value".to_string())],
            Some(StringExpireUpdate::RelativeMs(20_000)),
            false,
            false,
            false,
        )
        .unwrap();
        db0.set_add("set", &["a".to_string(), "b".to_string()])
            .unwrap();
        db0.zset_add("zset", &[(1.0, "a".to_string()), (2.0, "b".to_string())])
            .unwrap();
        db0.list_push_right("list", &["a".to_string(), "b".to_string()], false)
            .unwrap();
        db0.stream_add(
            "stream",
            Some(StreamId { ms: 1, seq: 0 }),
            &[("f".to_string(), "v".to_string())],
        )
        .unwrap();
        db0.json_set("json", "$", r#"{"a":1,"b":[2]}"#, SetCondition::Always)
            .unwrap();
        db0.insert(
            "legacy-stream".to_string(),
            Structure::Stream(vec![
                StreamEntry {
                    id: "bad".to_string(),
                    fields: vec![("skip".to_string(), "bad".to_string())],
                },
                StreamEntry {
                    id: "10-0".to_string(),
                    fields: vec![("f".to_string(), "v".to_string())],
                },
            ]),
        );
        assert_eq!(db0.stream_len("legacy-stream").unwrap(), 1);

        for key in ["hash", "set", "zset", "list", "stream", "json"] {
            assert!(
                Db::copy_key_between_dbs(
                    &db0.store,
                    0,
                    key,
                    1,
                    &format!("{key}-copy"),
                    false,
                    &db0.version_counter,
                    Some(&db0.ttl_manager),
                )
                .unwrap(),
                "{key}"
            );
        }
        assert_eq!(
            db1.hash_get("hash-copy", "field").unwrap(),
            Some("value".to_string())
        );
        assert!(db1.set_contains("set-copy", "a").unwrap());
        assert_eq!(db1.zset_score("zset-copy", "b").unwrap(), Some(2.0));
        assert_eq!(
            db1.list_range("list-copy", 0, -1).unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(db1.stream_len("stream-copy").unwrap(), 1);
        assert_eq!(
            db1.json_get("json-copy", "$.b[0]").unwrap(),
            Some("2".to_string())
        );
        assert!(
            !Db::copy_key_between_dbs(
                &db0.store,
                0,
                "missing",
                1,
                "missing-copy",
                false,
                &db0.version_counter,
                Some(&db0.ttl_manager),
            )
            .unwrap()
        );
        assert!(
            !Db::copy_key_between_dbs(
                &db0.store,
                0,
                "hash",
                1,
                "hash-copy",
                false,
                &db0.version_counter,
                Some(&db0.ttl_manager),
            )
            .unwrap()
        );
        assert!(
            Db::copy_key_between_dbs(
                &db0.store,
                0,
                "hash",
                1,
                "hash-copy",
                true,
                &db0.version_counter,
                Some(&db0.ttl_manager),
            )
            .unwrap()
        );

        assert!(
            Db::move_key_between_dbs_async(
                &db0.store,
                0,
                "list",
                1,
                "list-moved",
                &db0.version_counter,
                Some(&db0.ttl_manager),
            )
            .await
            .unwrap()
        );
        assert!(!db0.exists("list"));
        assert_eq!(
            db1.list_range("list-moved", 0, -1).unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
        assert!(
            !Db::move_key_between_dbs_async(
                &db0.store,
                0,
                "missing",
                1,
                "missing-moved",
                &db0.version_counter,
                Some(&db0.ttl_manager),
            )
            .await
            .unwrap()
        );
        assert!(
            !Db::move_key_between_dbs_async(
                &db0.store,
                1,
                "list-moved",
                1,
                "list-moved",
                &db0.version_counter,
                Some(&db0.ttl_manager),
            )
            .await
            .unwrap()
        );

        assert!(matches!(
            db0.remove("legacy-stream"),
            Some(Structure::Stream(entries)) if entries.len() == 1
        ));
        assert!(matches!(
            db1.remove_async("stream-copy").await,
            Some(Structure::Stream(entries)) if entries.len() == 1
        ));
        assert!(db1.set_contains("set-copy", "a").unwrap());
        assert_eq!(db1.zset_score("zset-copy", "a").unwrap(), Some(1.0));
        assert_eq!(
            db1.hash_get("hash-copy", "field").unwrap(),
            Some("value".to_string())
        );
        assert!(db1.delete_key_async("set-copy").await);
        assert!(db1.delete_key_async("zset-copy").await);
        assert!(db1.delete_key_async("hash-copy").await);
    }

    #[tokio::test]
    async fn key_space_readonly_ttl_copy_move_scan_and_clear_cover_edges() {
        let root = test_root("onedis-keyspace-test");
        let db_path = root.join("db");
        let wal_dir = root.join("wal");
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::create_dir_all(&wal_dir).unwrap();
        let store = KvStore::new(db_path, wal_dir, 1);
        let version_counter = Arc::new(VersionCounter::new());
        let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
        let db0 = Db::new(
            0,
            store.clone(),
            version_counter.clone(),
            ttl_manager.clone(),
        );
        let db1 = Db::new(1, store, version_counter, ttl_manager);

        assert_eq!(db0.type_name_readonly("missing"), "none");
        assert_eq!(db0.ttl_millis_readonly("missing"), -2);
        assert_eq!(db0.expire_time_millis_readonly("missing"), -2);
        assert!(db0.random_key().is_none());
        assert!(db0.random_key_async().await.is_none());

        db0.insert_string_ref("prefix:a", "one");
        db0.insert_string_ref("prefix:b", "two");
        db0.hash_set("hash", "field", "value").unwrap();
        db0.list_push_right("list", &["x".to_string()], false)
            .unwrap();
        db0.set_add("set", &["m".to_string()]).unwrap();
        db0.zset_add("zset", &[(1.0, "m".to_string())]).unwrap();
        db0.stream_add(
            "stream",
            Some(StreamId { ms: 1, seq: 0 }),
            &[("f".to_string(), "v".to_string())],
        )
        .unwrap();
        db0.json_set("json", "$", r#"{"a":1}"#, SetCondition::Always)
            .unwrap();

        assert_eq!(db0.type_name_readonly("prefix:a"), "string");
        assert_eq!(db0.type_name_readonly("hash"), "hash");
        assert_eq!(db0.type_name_readonly("list"), "list");
        assert_eq!(db0.type_name_readonly("set"), "set");
        assert_eq!(db0.type_name_readonly("zset"), "zset");
        assert_eq!(db0.type_name_readonly("stream"), "stream");
        assert_eq!(db0.type_name_readonly("json"), "json");
        assert_eq!(db0.type_name_readonly_async("hash").await, "hash");
        assert!(db0.exists_readonly("prefix:a"));
        assert!(db0.exists_readonly_async("prefix:a").await);
        assert!(db0.len() >= 7);
        assert!(db0.len_async().await >= 7);
        assert!(db0.random_key().is_some());
        assert!(db0.random_key_async().await.is_some());

        let mut prefix_keys = db0.keys("prefix:*");
        prefix_keys.sort();
        assert_eq!(
            prefix_keys,
            vec!["prefix:a".to_string(), "prefix:b".to_string()]
        );
        let mut prefix_keys_async = db0.keys_async("prefix:*").await;
        prefix_keys_async.sort();
        assert_eq!(prefix_keys_async, prefix_keys);
        assert_eq!(db0.scan_string_prefix_async("prefix:", 1).await.len(), 1);

        assert!(db0.expire_with_condition("prefix:a".to_string(), 10_000, ExpireCondition::Nx));
        assert!(!db0.expire_with_condition("prefix:a".to_string(), 10_000, ExpireCondition::Nx));
        assert!(db0.expire_with_condition("prefix:a".to_string(), 20_000, ExpireCondition::Gt));
        assert!(!db0.expire_with_condition("prefix:a".to_string(), 10_000, ExpireCondition::Gt));
        assert!(db0.expire_time_millis_readonly("prefix:a") > now_ms() as i64);
        assert!(db0.ttl_millis_readonly("prefix:a") > 0);
        assert!(db0.persist_async("prefix:a").await);
        assert_eq!(db0.ttl_millis_readonly_async("prefix:a").await, -1);
        assert!(
            db0.expire_with_condition_async("prefix:a".to_string(), 1, ExpireCondition::Always)
                .await
        );
        sleep(Duration::from_millis(5));
        assert_eq!(db0.ttl_millis("prefix:a"), -2);
        assert!(!db0.exists_readonly("prefix:a"));

        assert!(db0.copy_key_to_db(1, "zset", "copied-zset", false).unwrap());
        assert!(!db0.copy_key_to_db(1, "zset", "copied-zset", false).unwrap());
        assert!(db0.copy_key_to_db(1, "zset", "copied-zset", true).unwrap());
        assert_eq!(db1.zset_score("copied-zset", "m").unwrap(), Some(1.0));
        assert!(db0.move_key_to_db_async(1, "set").await.unwrap());
        assert!(!db0.exists("set"));
        assert!(db1.set_contains("set", "m").unwrap());
        assert!(!db1.move_key_to_db_async(1, "set").await.unwrap());
        assert!(!db0.delete_key("missing"));
        assert!(db0.delete_key_async("prefix:b").await);

        db0.clear_async().await;
        assert_eq!(db0.len_async().await, 0);
        assert!(db1.exists("set"));
    }

