    #[test]
    fn hash_is_stored_and_loaded_via_kv_entries() {
        let db = test_db();
        let hash = HashMap::from([
            ("name".to_string(), "alice".to_string()),
            ("city".to_string(), "paris".to_string()),
        ]);

        db.insert("user:1".to_string(), Structure::Hash(hash.clone()));

        assert!(matches!(
            db.get("user:1"),
            Some(Structure::Hash(value)) if value == hash
        ));

        assert_eq!(db.len(), 1);
    }

    #[test]
    fn list_is_stored_and_loaded_via_kv_entries() {
        let db = test_db();
        let list = vec![
            "job-1".to_string(),
            "job-2".to_string(),
            "job-3".to_string(),
        ];

        db.insert("queue".to_string(), Structure::List(list.clone()));

        assert!(matches!(
            db.get("queue"),
            Some(Structure::List(value)) if value == list
        ));

        assert_eq!(db.len(), 1);
    }

    #[test]
    fn hash_native_ops_use_field_level_storage() {
        let db = test_db();

        assert!(db.hash_set("user:1", "name", "alice").unwrap());
        assert!(!db.hash_set("user:1", "name", "bob").unwrap());
        assert!(db.hash_set("user:1", "city", "paris").unwrap());

        assert_eq!(
            db.hash_get("user:1", "name").unwrap(),
            Some("bob".to_string())
        );
        assert_eq!(db.hash_get("user:1", "missing").unwrap(), None);
        assert!(db.hash_exists("user:1", "city").unwrap());
        assert_eq!(db.hash_len("user:1").unwrap(), 2);
    }

    #[test]
    fn hash_delete_removes_meta_when_last_field_is_deleted() {
        let db = test_db();

        db.hash_set("user:2", "name", "alice").unwrap();
        assert_eq!(
            db.hash_delete("user:2", &[String::from("name")]).unwrap(),
            1
        );
        assert_eq!(db.hash_len("user:2").unwrap(), 0);
        assert!(!db.exists("user:2"));
    }

    #[test]
    fn hash_native_ops_reject_wrong_type() {
        let db = test_db();
        db.insert("plain".to_string(), Structure::String("value".to_string()));

        assert!(db.hash_get("plain", "field").is_err());
        assert!(db.hash_set("plain", "field", "value").is_err());
        assert!(db.hash_delete("plain", &[String::from("field")]).is_err());
        assert!(db.hash_exists("plain", "field").is_err());
        assert!(db.hash_len("plain").is_err());
    }

    #[test]
    fn hash_native_read_apis_share_same_storage_model() {
        let db = test_db();
        db.hash_set("user:3", "name", "alice").unwrap();
        db.hash_set("user:3", "city", "paris").unwrap();

        let values = db
            .hash_multi_get("user:3", &[String::from("name"), String::from("missing")])
            .unwrap();
        assert_eq!(values, vec![Some("alice".to_string()), None]);

        let mut all = db.hash_get_all("user:3").unwrap();
        all.sort();
        assert_eq!(
            all,
            vec![
                ("city".to_string(), "paris".to_string()),
                ("name".to_string(), "alice".to_string())
            ]
        );

        let mut keys = db.hash_keys("user:3").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["city".to_string(), "name".to_string()]);

        let mut values = db.hash_values("user:3").unwrap();
        values.sort();
        assert_eq!(values, vec!["alice".to_string(), "paris".to_string()]);
    }

    #[test]
    fn hash_set_nx_only_writes_missing_field() {
        let db = test_db();

        assert!(db.hash_set_nx("user:4", "name", "alice").unwrap());
        assert!(!db.hash_set_nx("user:4", "name", "bob").unwrap());
        assert_eq!(
            db.hash_get("user:4", "name").unwrap(),
            Some("alice".to_string())
        );
    }

    #[test]
    fn hash_scan_paginates_and_filters_by_match() {
        let db = test_db();
        db.hash_set("user:5", "name", "alice").unwrap();
        db.hash_set("user:5", "nickname", "ally").unwrap();
        db.hash_set("user:5", "city", "paris").unwrap();

        let (next_cursor, first_page) = db.hash_scan("user:5", 0, "*", 2).unwrap();
        assert_eq!(next_cursor, 2);
        assert_eq!(first_page.len(), 2);

        let (done_cursor, matched) = db.hash_scan("user:5", 0, "*name*", 10).unwrap();
        assert_eq!(done_cursor, 0);
        assert_eq!(
            matched,
            vec![
                ("name".to_string(), "alice".to_string()),
                ("nickname".to_string(), "ally".to_string())
            ]
        );
    }

    #[test]
    fn hash_multi_set_shares_native_storage_model() {
        let db = test_db();
        let fields = HashMap::from([
            ("name".to_string(), "alice".to_string()),
            ("age".to_string(), "30".to_string()),
        ]);

        db.hash_multi_set("user:6", &fields).unwrap();

        assert_eq!(db.hash_len("user:6").unwrap(), 2);
        assert_eq!(
            db.hash_get("user:6", "name").unwrap(),
            Some("alice".to_string())
        );

        let mut all = db.hash_get_all("user:6").unwrap();
        all.sort();
        assert_eq!(
            all,
            vec![
                ("age".to_string(), "30".to_string()),
                ("name".to_string(), "alice".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn concurrent_hash_set_async_on_same_new_key_keeps_all_fields() {
        let db = Arc::new(test_db());
        let mut tasks = Vec::new();
        for idx in 0..128 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                let field = format!("f{idx}");
                let value = format!("v{idx}");
                db.hash_set_async("concurrent-hash", &field, &value)
                    .await
                    .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(db.hash_len_async("concurrent-hash").await.unwrap(), 128);
        for idx in 0..128 {
            let field = format!("f{idx}");
            let expected = format!("v{idx}");
            assert_eq!(
                db.hash_get_async("concurrent-hash", &field).await.unwrap(),
                Some(expected)
            );
        }
    }

    #[tokio::test]
    async fn concurrent_hash_set_async_same_field_reports_single_new_field() {
        let db = Arc::new(test_db());
        let mut tasks = Vec::new();
        for idx in 0..32 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                let value = format!("v{idx}");
                db.hash_set_async("same-field-hash", "field", &value)
                    .await
                    .unwrap()
            }));
        }

        let mut added = 0usize;
        for task in tasks {
            if task.await.unwrap() {
                added += 1;
            }
        }

        assert_eq!(added, 1);
        assert_eq!(db.hash_len_async("same-field-hash").await.unwrap(), 1);
        assert!(
            db.hash_get_async("same-field-hash", "field")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn concurrent_hash_increment_async_keeps_all_increments() {
        let db = Arc::new(test_db());
        let mut tasks = Vec::new();
        for _ in 0..64 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                db.hash_increment_by_async("counter-hash", "field", 1)
                    .await
                    .unwrap()
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(
            db.hash_get_async("counter-hash", "field").await.unwrap(),
            Some("64".to_string())
        );
    }

