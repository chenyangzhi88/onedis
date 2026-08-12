use super::*;
use std::sync::atomic::Ordering;

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
fn stale_hash_field_expiry_cleanup_cannot_delete_new_value() {
    let db = test_db();
    db.hash_set("hash", "field", "old").unwrap();
    let (_, version) = db.hash_expire_ms("hash").unwrap().unwrap();
    let expire_key = hash_field_expire_key(db.db_index, "hash", version, "field");
    let field_key = hash_field_key(db.db_index, "hash", version, "field");
    db.store
        .put_raw(&expire_key, &now_ms().saturating_sub(1).to_be_bytes());

    let observed_expire = db.store.get_raw_observed(&expire_key);
    let observed_field = db.store.get_raw_observed(&field_key);
    db.hash_set("hash", "field", "new").unwrap();

    let mut stale_cleanup = WriteBatch::new();
    stale_cleanup.delete(&field_key);
    stale_cleanup.delete(&expire_key);
    assert!(
        !db.compare_and_write_batch_if_not_empty(
            &[
                CompareCondition::from_observed(&observed_expire),
                CompareCondition::from_observed(&observed_field),
            ],
            &stale_cleanup,
        )
        .unwrap()
    );
    assert_eq!(
        db.hash_get("hash", "field").unwrap(),
        Some("new".to_string())
    );
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
fn hash_delete_counts_duplicate_fields_once() {
    let db = test_db();

    db.hash_set("user:duplicate", "name", "alice").unwrap();
    let field = String::from("name");
    assert_eq!(
        db.hash_delete("user:duplicate", &[field.clone(), field])
            .unwrap(),
        1
    );
    assert!(!db.exists("user:duplicate"));
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
async fn ordered_hash_set_batch_reports_each_command_and_keeps_last_value() {
    let db = test_db();

    let first = [
        ("name", b"alice".as_slice()),
        ("name", b"bob".as_slice()),
        ("city", b"paris".as_slice()),
    ];
    assert_eq!(
        db.hash_set_ordered_bytes_async("user:batch", &first)
            .await
            .unwrap(),
        vec![true, false, true]
    );
    assert_eq!(
        db.hash_get_async("user:batch", "name").await.unwrap(),
        Some("bob".to_string())
    );

    let second = [
        ("city", b"london".as_slice()),
        ("age", b"30".as_slice()),
        ("age", b"31".as_slice()),
    ];
    assert_eq!(
        db.hash_set_ordered_bytes_async("user:batch", &second)
            .await
            .unwrap(),
        vec![false, true, false]
    );
    assert_eq!(
        db.hash_get_async("user:batch", "city").await.unwrap(),
        Some("london".to_string())
    );
    assert_eq!(
        db.hash_get_async("user:batch", "age").await.unwrap(),
        Some("31".to_string())
    );
}

#[tokio::test]
async fn hash_set_same_persistent_value_is_a_storage_noop() {
    let db = test_db();

    assert!(
        db.hash_set_bytes_async("user:noop", "name", b"alice")
            .await
            .unwrap()
    );
    let changes_after_insert = db.changes.load(Ordering::Relaxed);

    assert!(
        !db.hash_set_bytes_async("user:noop", "name", b"alice")
            .await
            .unwrap()
    );
    assert_eq!(db.changes.load(Ordering::Relaxed), changes_after_insert);
    assert_eq!(
        db.hash_get_bytes_async("user:noop", "name").await.unwrap(),
        Some(b"alice".to_vec())
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
async fn concurrent_hash_set_async_existing_field_is_last_write_wins() {
    let db = Arc::new(test_db());
    assert!(
        db.hash_set_async("existing-field-hash", "field", "seed")
            .await
            .unwrap()
    );

    let mut tasks = Vec::new();
    for idx in 0..64 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            let value = format!("v{idx}");
            let added = db
                .hash_set_async("existing-field-hash", "field", &value)
                .await
                .unwrap();
            (added, value)
        }));
    }

    let mut written_values = HashSet::new();
    for task in tasks {
        let (added, value) = task.await.unwrap();
        assert!(!added);
        written_values.insert(value);
    }

    assert_eq!(db.hash_len_async("existing-field-hash").await.unwrap(), 1);
    assert!(
        written_values.contains(
            &db.hash_get_async("existing-field-hash", "field")
                .await
                .unwrap()
                .unwrap()
        )
    );
}

#[tokio::test]
async fn concurrent_hash_set_many_async_only_counts_each_new_field_once() {
    let db = Arc::new(test_db());
    assert!(
        db.hash_set_async("mixed-field-hash", "base", "seed")
            .await
            .unwrap()
    );

    let mut tasks = Vec::new();
    for idx in 0..64 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            db.hash_set_many_async(
                "mixed-field-hash",
                &[
                    ("shared".to_string(), format!("shared-{idx}")),
                    (format!("unique-{idx}"), format!("value-{idx}")),
                ],
            )
            .await
            .unwrap()
        }));
    }

    let mut added = 0usize;
    for task in tasks {
        added += task.await.unwrap();
    }

    assert_eq!(added, 65);
    assert_eq!(db.hash_len_async("mixed-field-hash").await.unwrap(), 66);
    for idx in 0..64 {
        assert_eq!(
            db.hash_get_async("mixed-field-hash", &format!("unique-{idx}"))
                .await
                .unwrap(),
            Some(format!("value-{idx}"))
        );
    }
}

#[tokio::test]
async fn concurrent_hash_set_async_expired_field_reports_single_new_field() {
    let db = Arc::new(test_db());
    assert!(
        db.hash_set_async("expired-field-hash", "field", "old")
            .await
            .unwrap()
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "expired-field-hash",
            now_ms().saturating_add(25),
            &["field".to_string()],
            ExpireCondition::Always,
        )
        .await
        .unwrap(),
        vec![1]
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut tasks = Vec::new();
    for idx in 0..32 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            db.hash_set_async("expired-field-hash", "field", &format!("v{idx}"))
                .await
                .unwrap()
        }));
    }

    let mut added = 0usize;
    for task in tasks {
        added += usize::from(task.await.unwrap());
    }

    assert_eq!(added, 1);
    assert_eq!(db.hash_len_async("expired-field-hash").await.unwrap(), 1);
    assert!(
        db.hash_get_async("expired-field-hash", "field")
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

#[tokio::test]
async fn cached_hash_increment_is_cut_off_by_hset_and_delete() {
    let db = test_db();
    db.hash_set_async("cached-hash", "field", "10")
        .await
        .unwrap();
    assert_eq!(
        db.hash_increment_by_async("cached-hash", "field", 1)
            .await
            .unwrap(),
        11
    );
    db.hash_set_async("cached-hash", "field", "100")
        .await
        .unwrap();
    assert_eq!(
        db.hash_increment_by_async("cached-hash", "field", 1)
            .await
            .unwrap(),
        101
    );
    assert_eq!(
        db.hash_delete_async("cached-hash", &["field".to_string()])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.hash_increment_by_async("cached-hash", "field", 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.hash_get_async("cached-hash", "field").await.unwrap(),
        Some("1".to_string())
    );
}

#[tokio::test]
async fn cached_hash_length_tracks_sets_and_deletes() {
    let db = test_db();
    db.hash_set_async("length-cache", "a", "1").await.unwrap();
    assert_eq!(db.hash_len_async("length-cache").await.unwrap(), 1);

    db.hash_set_async("length-cache", "b", "2").await.unwrap();
    assert_eq!(db.hash_len_async("length-cache").await.unwrap(), 2);
    assert_eq!(
        db.hash_delete_async("length-cache", &["a".to_string()])
            .await
            .unwrap(),
        1
    );
    assert_eq!(db.hash_len_async("length-cache").await.unwrap(), 1);
    assert_eq!(
        db.hash_delete_async("length-cache", &["b".to_string()])
            .await
            .unwrap(),
        1
    );
    assert_eq!(db.hash_len_async("length-cache").await.unwrap(), 0);
    assert!(!db.exists_readonly_async("length-cache").await);
}

#[tokio::test]
async fn concurrent_hash_set_nx_is_field_local_and_has_one_winner_per_field() {
    let db = Arc::new(test_db());
    let mut distinct = Vec::new();
    for index in 0..64 {
        let db = Arc::clone(&db);
        distinct.push(tokio::spawn(async move {
            db.hash_set_nx_async("nx-fields", &format!("field-{index}"), "value")
                .await
                .unwrap()
        }));
    }
    for task in distinct {
        assert!(task.await.unwrap());
    }
    assert_eq!(db.hash_len_async("nx-fields").await.unwrap(), 64);

    let mut same = Vec::new();
    for index in 0..64 {
        let db = Arc::clone(&db);
        same.push(tokio::spawn(async move {
            db.hash_set_nx_async("nx-same", "field", &format!("value-{index}"))
                .await
                .unwrap()
        }));
    }
    let mut winners = 0usize;
    for task in same {
        winners += usize::from(task.await.unwrap());
    }
    assert_eq!(winners, 1);
    assert_eq!(db.hash_len_async("nx-same").await.unwrap(), 1);
}

#[tokio::test]
async fn hash_numeric_updates_treat_expired_fields_as_missing() {
    let db = test_db();
    db.hash_set_async("expired-counter", "integer", "99")
        .await
        .unwrap();
    db.hash_set_async("expired-counter", "float", "99.5")
        .await
        .unwrap();
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "expired-counter",
            now_ms().saturating_add(25),
            &["integer".to_string(), "float".to_string()],
            ExpireCondition::Always,
        )
        .await
        .unwrap(),
        vec![1, 1]
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        db.hash_increment_by_async("expired-counter", "integer", 2)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        db.hash_increment_by_float_async("expired-counter", "float", 0.5)
            .await
            .unwrap(),
        "0.5"
    );
    assert_eq!(
        db.hash_field_ttls_async(
            "expired-counter",
            &["integer".to_string(), "float".to_string()],
            true,
            false,
        )
        .await
        .unwrap(),
        vec![-1, -1]
    );
}
