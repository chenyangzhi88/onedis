    #[test]
    fn update_preserves_ttl_for_get() {
        let db = test_db();

        db.insert("ttl-key".to_string(), Structure::String("v1".to_string()));
        db.expire("ttl-key".to_string(), 20);
        db.update("ttl-key".to_string(), Structure::String("v2".to_string()));

        assert!(matches!(
            db.get("ttl-key"),
            Some(Structure::String(value)) if value == "v2"
        ));

        sleep(Duration::from_millis(30));
        assert!(db.get("ttl-key").is_none());
    }

    #[test]
    fn json_set_get_type_and_del_paths() {
        let db = test_db();

        assert!(
            db.json_set(
                "doc",
                "$",
                r#"{"name":"alice","items":[1,2],"profile":{"city":"Paris"}}"#,
                SetCondition::Always,
            )
            .unwrap()
        );

        assert_eq!(db.json_type("doc", "$").unwrap(), Some("object"));
        assert_eq!(db.json_type("doc", "$.items").unwrap(), Some("array"));
        assert_eq!(db.json_type("doc", "$.items[0]").unwrap(), Some("integer"));
        assert_eq!(
            db.json_get("doc", "$.profile.city").unwrap(),
            Some(r#""Paris""#.to_string())
        );

        assert!(
            db.json_set("doc", "$.profile.city", r#""Berlin""#, SetCondition::Xx)
                .unwrap()
        );
        assert_eq!(
            db.json_get("doc", "$.profile").unwrap(),
            Some(r#"{"city":"Berlin"}"#.to_string())
        );

        assert!(
            db.json_set("doc", "$.profile.zip", "10115", SetCondition::Nx)
                .unwrap()
        );
        assert!(
            !db.json_set("doc", "$.profile.zip", "75000", SetCondition::Nx)
                .unwrap()
        );
        assert_eq!(db.json_del("doc", "$.profile.zip").unwrap(), 1);
        assert_eq!(db.json_del("doc", "$.profile.zip").unwrap(), 0);
        assert_eq!(db.json_get("doc", "$.profile.zip").unwrap(), None);
    }

    #[test]
    fn json_partial_update_uses_indexed_subtree_storage() {
        let db = test_db();

        assert!(
            db.json_set(
                "doc",
                "$",
                r#"{"profile":{"name":"alice","city":"Paris"},"stats":{"count":1}}"#,
                SetCondition::Always,
            )
            .unwrap()
        );
        let raw = db.store.get_raw(&db.mk("doc")).unwrap();
        let (_, version, structure) = super::decode_entry(&raw).unwrap();
        assert!(matches!(
            structure,
            Structure::Json(marker) if marker == super::JSON_INDEXED_MARKER
        ));

        let untouched_path = super::parse_json_path("$.profile.name").unwrap();
        let untouched_key = super::json_node_key(db.db_index, "doc", version, &untouched_path);
        let untouched_before = db.store.get_raw(&untouched_key).unwrap();

        assert!(
            db.json_set("doc", "$.stats.count", "2", SetCondition::Xx)
                .unwrap()
        );

        assert_eq!(db.store.get_raw(&untouched_key).unwrap(), untouched_before);
        let root: serde_json::Value =
            serde_json::from_str(&db.json_get("doc", "$").unwrap().unwrap()).unwrap();
        assert_eq!(root["profile"]["name"], "alice");
        assert_eq!(root["profile"]["city"], "Paris");
        assert_eq!(root["stats"]["count"], 2);
        assert!(matches!(
            db.get("doc"),
            Some(Structure::Json(json)) if serde_json::from_str::<serde_json::Value>(&json).unwrap()["stats"]["count"] == 2
        ));
    }

    #[test]
    fn json_array_delete_rewrites_parent_subtree_and_root_delete_cleans_nodes() {
        let db = test_db();

        assert!(
            db.json_set(
                "doc",
                "$",
                r#"{"tags":["a","b","c"],"profile":{"name":"alice"}}"#,
                SetCondition::Always,
            )
            .unwrap()
        );
        assert_eq!(db.json_del("doc", "$.tags[1]").unwrap(), 1);
        assert_eq!(
            db.json_get("doc", "$.tags").unwrap(),
            Some(r#"["a","c"]"#.to_string())
        );
        assert_eq!(
            db.json_get("doc", "$.tags[1]").unwrap(),
            Some(r#""c""#.to_string())
        );

        let raw = db.store.get_raw(&db.mk("doc")).unwrap();
        let header = super::decode_meta_header(&raw).unwrap();
        let node_prefix = super::json_node_prefix(db.db_index, "doc", header.version);
        assert!(!db.store.scan_prefix_raw(&node_prefix).is_empty());
        assert_eq!(db.json_del("doc", "$").unwrap(), 1);
        assert!(db.store.scan_prefix_raw(&node_prefix).is_empty());
    }

    #[tokio::test]
    async fn json_sync_legacy_indexed_and_integer_string_edges_are_covered() {
        let db = test_db();

        db.insert(
            "legacy".to_string(),
            Structure::Json(r#"{"name":"alice","nested":{"x":1},"arr":[1,2]}"#.to_string()),
        );
        assert!(
            !db.json_set("legacy", "$", r#"{"blocked":true}"#, SetCondition::Nx)
                .unwrap()
        );
        assert!(
            db.json_set("legacy", "$.nested.y", "2", SetCondition::Nx)
                .unwrap()
        );
        assert!(
            !db.json_set("legacy", "$.nested.y", "3", SetCondition::Nx)
                .unwrap()
        );
        assert!(
            db.json_set("legacy", "$.arr[1]", "20", SetCondition::Xx)
                .unwrap()
        );
        assert!(
            !db.json_set("legacy", "$.arr[9]", "90", SetCondition::Always)
                .unwrap()
        );
        assert!(
            !db.json_set("legacy", "$.missing.child", "1", SetCondition::Always)
                .unwrap()
        );
        assert!(
            !db.json_set("legacy", "$.name.child", "1", SetCondition::Always)
                .unwrap()
        );
        assert!(
            db.json_set("legacy", "$", r#"{"replaced":true}"#, SetCondition::Always)
                .unwrap()
        );

        assert!(
            !db.json_set("missing-json", "$.a", "1", SetCondition::Always)
                .unwrap()
        );
        assert!(
            !db.json_set("missing-json", "$", "1", SetCondition::Xx)
                .unwrap()
        );
        assert!(
            db.json_set(
                "indexed",
                "$",
                r#"{"obj":{},"arr":[1]}"#,
                SetCondition::Always
            )
            .unwrap()
        );
        assert!(
            !db.json_set("indexed", "$", r#"{"blocked":true}"#, SetCondition::Nx)
                .unwrap()
        );
        assert!(
            db.json_set("indexed", "$.obj.name", r#""alice""#, SetCondition::Nx)
                .unwrap()
        );
        assert!(
            db.json_set("indexed", "$.arr[0]", "42", SetCondition::Xx)
                .unwrap()
        );
        assert!(
            !db.json_set("indexed", "$.arr[4]", "99", SetCondition::Always)
                .unwrap()
        );
        assert!(
            !db.json_set("indexed", "$.obj[0]", "99", SetCondition::Always)
                .unwrap()
        );
        assert!(
            db.json_set("indexed", "$", r#"{"root":true}"#, SetCondition::Always)
                .unwrap()
        );

        assert_eq!(
            db.update_integer_string("counter", |value| Some(value + 5))
                .unwrap(),
            5
        );
        assert_eq!(
            db.update_integer_string_async("counter", |value| Some(value + 1))
                .await
                .unwrap(),
            6
        );
        assert!(db.update_integer_string("counter", |_| None).is_err());
        assert_eq!(db.increment_integer_string("cached", 1).unwrap(), 1);
        assert_eq!(db.increment_integer_string("cached", 2).unwrap(), 3);
        assert_eq!(
            db.increment_integer_string_async("cached", 3)
                .await
                .unwrap(),
            6
        );
        db.insert_string("ttl-counter".to_string(), "10".to_string(), Some(20_000));
        assert_eq!(
            db.update_integer_string_async("ttl-counter", |value| Some(value + 1))
                .await
                .unwrap(),
            11
        );
        assert!(db.ttl_millis("ttl-counter") > 0);

        db.insert_string_ref("ex", "value");
        assert_eq!(
            db.getex_string_bytes("ex", None).unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(
            db.getex_string_bytes("ex", Some(StringExpireUpdate::RelativeMs(20_000)))
                .unwrap(),
            Some(b"value".to_vec())
        );
        assert!(db.ttl_millis("ex") > 0);
        assert_eq!(
            db.getex_string_bytes_async("ex", Some(StringExpireUpdate::Persist))
                .await
                .unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(db.ttl_millis("ex"), -1);
        assert_eq!(
            db.getex_string_bytes_async(
                "ex",
                Some(StringExpireUpdate::AbsoluteMs(
                    now_ms().saturating_add(20_000),
                )),
            )
            .await
            .unwrap(),
            Some(b"value".to_vec())
        );
        assert!(db.ttl_millis("ex") > 0);
        assert_eq!(
            db.getex_string_bytes("ex", Some(StringExpireUpdate::AbsoluteMs(1)))
                .unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(db.get_string("ex").unwrap(), None);
        assert_eq!(db.getex_string_bytes("missing", None).unwrap(), None);

        assert!(db.get_string_entry_raw_bytes(b"missing").unwrap().is_none());
        db.hash_set("not-string", "f", "v").unwrap();
        assert!(db.get_string_entry_raw_bytes(b"not-string").is_err());
        assert!(db.get_string_bytes("not-string").is_err());
        assert!(db.get_string_bytes_async("not-string").await.is_err());
        assert_eq!(db.type_name_readonly("not-string"), "hash");
        assert_eq!(db.type_name_readonly_async("not-string").await, "hash");
        assert!(db.exists_readonly_async("not-string").await);
    }

    #[test]
    fn json_wrong_type_is_rejected() {
        let db = test_db();

        db.insert_string_ref("doc", "plain");

        assert_eq!(
            db.json_get("doc", "$").unwrap_err().to_string(),
            WRONG_TYPE_ERROR
        );
        assert_eq!(
            db.json_set("doc", "$", "{}", SetCondition::Always)
                .unwrap_err()
                .to_string(),
            WRONG_TYPE_ERROR
        );
    }

    #[tokio::test]
    async fn json_command_async_path_uses_json_store() {
        let db = test_db();

        let frame = Command::JsonSet(JsonSet {
            key: "cart".to_string(),
            path: "$".to_string(),
            value: r#"{"total":10}"#.to_string(),
            condition: SetCondition::Always,
        });

        assert!(matches!(
            db.handle_command_async(frame).await.unwrap(),
            crate::frame::Frame::Ok
        ));
        assert_eq!(
            db.json_get_async("cart", "$.total").await.unwrap(),
            Some("10".to_string())
        );
    }

    #[tokio::test]
    async fn json_indexed_async_updates_deletes_and_conditions_cover_edges() {
        let db = test_db();

        assert!(
            db.json_set_async(
                "doc",
                "$",
                r#"{"profile":{"name":"alice","city":"Paris"},"tags":["a","b","c"],"stats":{"count":1}}"#,
                SetCondition::Always,
            )
            .await
            .unwrap()
        );
        let raw = db.store.get_raw(&db.mk("doc")).unwrap();
        let (_, _, structure) = super::decode_entry(&raw).unwrap();
        assert!(matches!(
            structure,
            Structure::Json(marker) if marker == super::JSON_INDEXED_MARKER
        ));

        assert_eq!(
            db.json_type_async("doc", "$.profile").await.unwrap(),
            Some("object")
        );
        assert_eq!(
            db.json_type_async("doc", "$.tags").await.unwrap(),
            Some("array")
        );
        assert!(
            !db.json_set_async("doc", "$.stats.count", "2", SetCondition::Nx)
                .await
                .unwrap()
        );
        assert!(
            db.json_set_async("doc", "$.stats.count", "2", SetCondition::Xx)
                .await
                .unwrap()
        );
        assert!(
            db.json_set_async("doc", "$.profile.zip", "10115", SetCondition::Nx)
                .await
                .unwrap()
        );
        assert_eq!(
            db.json_get_async("doc", "$.profile.zip").await.unwrap(),
            Some("10115".to_string())
        );

        assert_eq!(db.json_del_async("doc", "$.tags[1]").await.unwrap(), 1);
        assert_eq!(
            db.json_get_async("doc", "$.tags").await.unwrap(),
            Some(r#"["a","c"]"#.to_string())
        );
        assert_eq!(db.json_del_async("doc", "$.profile.zip").await.unwrap(), 1);
        assert_eq!(db.json_del_async("doc", "$.profile.zip").await.unwrap(), 0);
        assert_eq!(db.json_del_async("doc", "$.missing").await.unwrap(), 0);
        assert_eq!(db.json_del_async("missing", "$").await.unwrap(), 0);
        assert_eq!(db.json_del_async("doc", "$").await.unwrap(), 1);
        assert_eq!(db.json_get_async("doc", "$").await.unwrap(), None);
    }

    #[tokio::test]
    async fn concurrent_json_set_async_keeps_all_object_fields() {
        let db = Arc::new(test_db());
        db.json_set_async("doc", "$", r#"{"fields":{}}"#, SetCondition::Always)
            .await
            .unwrap();

        let mut tasks = Vec::new();
        for idx in 0..64 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                let path = format!("$.fields.f{idx}");
                db.json_set_async("doc", &path, &idx.to_string(), SetCondition::Nx)
                    .await
                    .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        let fields: serde_json::Value =
            serde_json::from_str(&db.json_get_async("doc", "$.fields").await.unwrap().unwrap())
                .unwrap();
        assert_eq!(fields.as_object().unwrap().len(), 64);
        for idx in 0..64 {
            assert_eq!(fields[format!("f{idx}")], idx);
        }
    }

