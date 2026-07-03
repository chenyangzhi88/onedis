use super::*;

#[test]
fn list_native_queue_ops_use_head_tail_metadata() {
    let db = test_db();

    assert_eq!(
        db.list_push_right("queue", &["a".to_string(), "b".to_string()], false)
            .unwrap(),
        2
    );
    assert_eq!(
        db.list_push_left("queue", &["x".to_string(), "y".to_string()], false)
            .unwrap(),
        4
    );
    assert_eq!(db.list_len("queue").unwrap(), 4);
    assert!(matches!(
        db.get("queue"),
        Some(Structure::List(items))
            if items == vec![
                "y".to_string(),
                "x".to_string(),
                "a".to_string(),
                "b".to_string()
            ]
    ));
}

#[test]
fn list_native_pop_updates_meta_and_removes_empty_key() {
    let db = test_db();
    db.list_push_right("queue", &["a".to_string(), "b".to_string()], false)
        .unwrap();

    assert_eq!(db.list_pop_left("queue").unwrap(), Some("a".to_string()));
    assert_eq!(db.list_pop_right("queue").unwrap(), Some("b".to_string()));
    assert_eq!(db.list_pop_left("queue").unwrap(), None);
    assert_eq!(db.list_len("queue").unwrap(), 0);
    assert!(!db.exists("queue"));
}

#[tokio::test]
async fn concurrent_list_push_async_on_same_key_keeps_all_items() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for idx in 0..128 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            let value = format!("v{idx}");
            db.list_push_right_async("concurrent-list", &[value], false)
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(db.list_len_async("concurrent-list").await.unwrap(), 128);
    assert_eq!(
        db.list_range_async("concurrent-list", 0, -1)
            .await
            .unwrap()
            .len(),
        128
    );
}

#[test]
fn list_native_ops_reject_wrong_type() {
    let db = test_db();
    db.insert("plain".to_string(), Structure::String("value".to_string()));

    assert!(
        db.list_push_left("plain", &["x".to_string()], false)
            .is_err()
    );
    assert!(
        db.list_push_right("plain", &["x".to_string()], false)
            .is_err()
    );
    assert!(db.list_pop_left("plain").is_err());
    assert!(db.list_pop_right("plain").is_err());
    assert!(db.list_len("plain").is_err());
}

#[test]
fn list_native_pushx_only_updates_existing_list() {
    let db = test_db();

    assert_eq!(
        db.list_push_left("missing", &["a".to_string()], true)
            .unwrap(),
        0
    );
    assert_eq!(
        db.list_push_right("missing", &["a".to_string()], true)
            .unwrap(),
        0
    );

    db.list_push_right("queue", &["a".to_string()], false)
        .unwrap();
    assert_eq!(
        db.list_push_left("queue", &["b".to_string()], true)
            .unwrap(),
        2
    );
    assert_eq!(
        db.list_push_right("queue", &["c".to_string()], true)
            .unwrap(),
        3
    );
    assert!(matches!(
        db.get("queue"),
        Some(Structure::List(items))
            if items == vec!["b".to_string(), "a".to_string(), "c".to_string()]
    ));
}

#[test]
fn list_native_index_and_range_support_negative_offsets() {
    let db = test_db();
    db.list_push_right(
        "queue",
        &[
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ],
        false,
    )
    .unwrap();

    assert_eq!(db.list_index("queue", 0).unwrap(), Some("a".to_string()));
    assert_eq!(db.list_index("queue", -1).unwrap(), Some("d".to_string()));
    assert_eq!(db.list_index("queue", 10).unwrap(), None);

    assert_eq!(
        db.list_range("queue", 1, -2).unwrap(),
        vec!["b".to_string(), "c".to_string()]
    );
    assert_eq!(
        db.list_range("queue", 10, 20).unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn list_positions_support_rank_count_and_maxlen() {
    let db = test_db();
    db.list_push_right(
        "queue",
        &[
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
            "a".to_string(),
        ],
        false,
    )
    .unwrap();

    assert_eq!(
        db.list_positions("queue", "a", 1, None, None).unwrap(),
        vec![0]
    );
    assert_eq!(
        db.list_positions("queue", "a", 2, None, None).unwrap(),
        vec![2]
    );
    assert_eq!(
        db.list_positions("queue", "a", 1, Some(0), None).unwrap(),
        vec![0, 2, 4]
    );
    assert_eq!(
        db.list_positions("queue", "a", 1, Some(2), None).unwrap(),
        vec![0, 2]
    );
    assert_eq!(
        db.list_positions("queue", "a", -1, Some(2), None).unwrap(),
        vec![4, 2]
    );
    assert_eq!(
        db.list_positions("queue", "a", 1, Some(3), Some(2))
            .unwrap(),
        vec![0]
    );
}

#[test]
fn list_move_supports_lmove_and_rpoplpush_shapes() {
    let db = test_db();
    db.list_push_right(
        "source",
        &["a".to_string(), "b".to_string(), "c".to_string()],
        false,
    )
    .unwrap();
    db.list_push_right("dest", &["x".to_string()], false)
        .unwrap();

    assert_eq!(
        db.list_move("source", "dest", false, true).unwrap(),
        Some("c".to_string())
    );
    assert_eq!(
        db.list_range("source", 0, -1).unwrap(),
        vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(
        db.list_range("dest", 0, -1).unwrap(),
        vec!["c".to_string(), "x".to_string()]
    );

    assert_eq!(
        db.list_move("source", "source", true, false).unwrap(),
        Some("a".to_string())
    );
    assert_eq!(
        db.list_range("source", 0, -1).unwrap(),
        vec!["b".to_string(), "a".to_string()]
    );
    assert_eq!(db.list_move("missing", "dest", false, true).unwrap(), None);
}

#[test]
fn list_insert_supports_before_after_and_missing_pivot() {
    let db = test_db();
    db.list_push_right(
        "queue",
        &["a".to_string(), "c".to_string(), "d".to_string()],
        false,
    )
    .unwrap();

    assert_eq!(db.list_insert("missing", true, "a", "x").unwrap(), 0);
    assert_eq!(db.list_insert("queue", true, "missing", "x").unwrap(), -1);
    assert_eq!(db.list_insert("queue", true, "c", "b").unwrap(), 4);
    assert_eq!(db.list_insert("queue", false, "d", "e").unwrap(), 5);
    assert_eq!(
        db.list_range("queue", 0, -1).unwrap(),
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string()
        ]
    );
}

#[test]
fn list_multi_pop_returns_first_non_empty_key() {
    let db = test_db();
    db.list_push_right(
        "right",
        &["a".to_string(), "b".to_string(), "c".to_string()],
        false,
    )
    .unwrap();

    assert_eq!(
        db.list_multi_pop(&["missing".to_string(), "right".to_string()], true, 2)
            .unwrap(),
        Some(("right".to_string(), vec!["a".to_string(), "b".to_string()]))
    );
    assert_eq!(
        db.list_range("right", 0, -1).unwrap(),
        vec!["c".to_string()]
    );
    assert_eq!(
        db.list_multi_pop(&["right".to_string()], false, 5).unwrap(),
        Some(("right".to_string(), vec!["c".to_string()]))
    );
    assert_eq!(
        db.list_multi_pop(&["right".to_string()], false, 1).unwrap(),
        None
    );
}

#[test]
fn list_range_scans_lpush_negative_indexes_in_order() {
    let db = test_db();
    db.list_push_left(
        "queue",
        &[
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ],
        false,
    )
    .unwrap();

    assert_eq!(
        db.list_range("queue", 0, 2).unwrap(),
        vec!["d".to_string(), "c".to_string(), "b".to_string()]
    );
    assert_eq!(
        db.list_range("queue", 2, 3).unwrap(),
        vec!["b".to_string(), "a".to_string()]
    );
}

#[test]
fn list_range_scans_mixed_negative_and_positive_indexes_in_order() {
    let db = test_db();
    db.list_push_left("queue", &["b".to_string(), "a".to_string()], false)
        .unwrap();
    db.list_push_right("queue", &["c".to_string(), "d".to_string()], false)
        .unwrap();

    assert_eq!(
        db.list_range("queue", 0, -1).unwrap(),
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string()
        ]
    );
    assert_eq!(
        db.list_range("queue", 1, 2).unwrap(),
        vec!["b".to_string(), "c".to_string()]
    );
}

#[tokio::test]
async fn list_range_async_matches_sync_scan_order() {
    let db = test_db();
    db.list_push_left("queue", &["b".to_string(), "a".to_string()], false)
        .unwrap();
    db.list_push_right("queue", &["c".to_string(), "d".to_string()], false)
        .unwrap();

    assert_eq!(
        db.list_range_async("queue", 0, -1).await.unwrap(),
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string()
        ]
    );
}

#[tokio::test]
async fn list_async_bytes_positions_move_trim_remove_and_errors_cover_edges() {
    let db = test_db();

    assert_eq!(
        db.list_push_left_async("missing", &["x".to_string()], true)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        db.list_push_right_async("missing", &["x".to_string()], true)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        db.list_push_left_async("queue", &["b".to_string(), "a".to_string()], false)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        db.list_push_right_async(
            "queue",
            &[
                "c".to_string(),
                "a".to_string(),
                "d".to_string(),
                "a".to_string(),
            ],
            false,
        )
        .await
        .unwrap(),
        6
    );
    assert_eq!(db.list_len_async("queue").await.unwrap(), 6);
    assert_eq!(
        db.list_index_async("queue", -1).await.unwrap(),
        Some("a".to_string())
    );
    assert_eq!(db.list_index_async("queue", 99).await.unwrap(), None);
    assert_eq!(
        db.list_positions_async("queue", "a", 1, Some(3), Some(4))
            .await
            .unwrap(),
        vec![0, 3]
    );
    assert_eq!(
        db.list_positions_async("queue", "a", -1, Some(2), None)
            .await
            .unwrap(),
        vec![5, 3]
    );
    assert!(
        db.list_positions_async("queue", "a", 1, Some(1), Some(0))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.list_insert_async("queue", true, "c", "before-c")
            .await
            .unwrap(),
        7
    );
    assert_eq!(
        db.list_insert_async("queue", false, "missing", "x")
            .await
            .unwrap(),
        -1
    );
    assert_eq!(
        db.list_insert_async("missing-list", true, "x", "y")
            .await
            .unwrap(),
        0
    );
    db.list_set_async("queue", -1, "tail").await.unwrap();
    assert!(db.list_set_async("queue", 99, "bad").await.is_err());
    assert!(db.list_set_async("missing-list", 0, "bad").await.is_err());

    assert_eq!(
        db.list_move_async("queue", "dest", false, true)
            .await
            .unwrap(),
        Some("tail".to_string())
    );
    assert_eq!(
        db.list_move_async("queue", "queue", true, false)
            .await
            .unwrap(),
        Some("a".to_string())
    );
    assert_eq!(
        db.list_multi_pop_async(&["missing".to_string(), "dest".to_string()], true, 5)
            .await
            .unwrap(),
        Some(("dest".to_string(), vec!["tail".to_string()]))
    );
    assert_eq!(
        db.list_multi_pop_async(&["dest".to_string()], true, 1)
            .await
            .unwrap(),
        None
    );
    assert_eq!(db.list_remove_async("queue", -1, "a").await.unwrap(), 1);
    assert_eq!(db.list_remove_async("queue", 0, "a").await.unwrap(), 1);
    assert_eq!(
        db.list_remove_async("queue", 1, "missing").await.unwrap(),
        0
    );
    db.list_trim_async("queue", 1, -2).await.unwrap();
    assert!(db.list_len_async("queue").await.unwrap() > 0);
    db.list_trim_async("queue", 99, 100).await.unwrap();
    assert_eq!(db.list_len_async("queue").await.unwrap(), 0);
    db.list_trim_async("missing-list", 0, -1).await.unwrap();

    db.list_push_right_bytes_async("raw", &[b"ok".as_slice(), b"\xff"], false)
        .await
        .unwrap();
    assert_eq!(
        db.list_range_bytes_async("raw", 0, -1).await.unwrap(),
        vec![b"ok".to_vec(), b"\xff".to_vec()]
    );
    let mut visited = 0usize;
    assert_eq!(
        db.list_range_visit_bytes_async("raw", 0, -1, |value| {
            visited += 1;
            value != b"ok"
        })
        .await
        .unwrap(),
        1
    );
    assert_eq!(visited, 1);
    assert!(db.list_range_async("raw", 0, -1).await.is_err());

    db.insert_string_ref("plain", "value");
    assert!(
        db.list_push_left_async("plain", &["x".to_string()], false)
            .await
            .is_err()
    );
    assert!(db.list_pop_left_async("plain").await.is_err());
    assert!(db.list_len_async("plain").await.is_err());
    assert!(db.list_index_async("plain", 0).await.is_err());
    assert!(db.list_range_async("plain", 0, -1).await.is_err());
    assert!(
        db.list_positions_async("plain", "x", 1, None, None)
            .await
            .is_err()
    );
    assert!(db.list_remove_async("plain", 0, "x").await.is_err());
}

#[test]
fn list_native_set_and_trim_update_storage_in_place() {
    let db = test_db();
    db.list_push_right(
        "queue",
        &[
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ],
        false,
    )
    .unwrap();

    db.list_set("queue", -2, "x").unwrap();
    assert_eq!(
        db.list_range("queue", 0, -1).unwrap(),
        vec![
            "a".to_string(),
            "b".to_string(),
            "x".to_string(),
            "d".to_string()
        ]
    );

    db.list_trim("queue", 1, 2).unwrap();
    assert_eq!(
        db.list_range("queue", 0, -1).unwrap(),
        vec!["b".to_string(), "x".to_string()]
    );
    assert_eq!(db.list_len("queue").unwrap(), 2);

    db.list_trim("queue", 10, 20).unwrap();
    assert_eq!(db.list_len("queue").unwrap(), 0);
    assert!(!db.exists("queue"));
}
