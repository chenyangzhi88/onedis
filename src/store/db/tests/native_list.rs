use super::*;
use crate::store::db::{SMALL_LIST_MAX_ITEMS, decode_list_meta, decode_packed_list};

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
    assert!(decode_packed_list(&db.store.get_raw(&db.mk("queue")).unwrap().unwrap()).is_some());
    assert!(matches!(
        db.get("queue").unwrap(),
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
fn list_promotes_once_at_the_inline_item_limit() {
    let db = test_db();
    let values = (0..=SMALL_LIST_MAX_ITEMS)
        .map(|index| format!("item-{index}"))
        .collect::<Vec<_>>();
    db.list_push_right("queue", &values, false).unwrap();
    let raw = db.store.get_raw(&db.mk("queue")).unwrap().unwrap();
    let meta = decode_list_meta(&raw).unwrap();
    assert_ne!(meta.version, 0);
    assert!(decode_packed_list(&raw).is_none());

    db.list_pop_right("queue").unwrap();
    assert!(decode_packed_list(&db.store.get_raw(&db.mk("queue")).unwrap().unwrap()).is_none());
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
    assert!(!db.exists("queue").unwrap());
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

#[tokio::test]
async fn concurrent_count_pops_return_every_item_once_and_remove_idle_queue() {
    let db = Arc::new(test_db());
    let values = (0..128)
        .map(|index| format!("v{index:03}"))
        .collect::<Vec<_>>();
    db.list_push_right_async("concurrent-pop", &values, false)
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            db.list_pop_merged_async("concurrent-pop", true, 8)
                .await
                .unwrap()
        }));
    }
    let mut popped = Vec::new();
    for task in tasks {
        popped.extend(
            task.await
                .unwrap()
                .into_iter()
                .map(|value| String::from_utf8(value).unwrap()),
        );
    }
    popped.sort();
    assert_eq!(popped, values);
    assert!(!db.exists_readonly("concurrent-pop").unwrap());
    for _ in 0..8 {
        if db.counter_cache.list_pop_queues.is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(db.counter_cache.list_pop_queues.is_empty());
}

#[tokio::test]
async fn insert_and_remove_preserve_nonzero_list_storage_boundaries() {
    let db = test_db();
    db.list_push_right_async(
        "shifted",
        &["a".to_string(), "b".to_string(), "c".to_string()],
        false,
    )
    .await
    .unwrap();
    db.promote_packed_list_async("shifted").await.unwrap();
    db.list_push_left_async("shifted", &["front".to_string()], false)
        .await
        .unwrap();
    let before = db.list_meta_async("shifted").await.unwrap().unwrap();
    assert!(before.head < 0);

    assert_eq!(
        db.list_insert_async("shifted", true, "front", "new-front")
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        db.list_insert_async("shifted", false, "c", "new-back")
            .await
            .unwrap(),
        6
    );
    assert_eq!(
        db.list_remove_async("shifted", 1, "new-front")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.list_remove_async("shifted", -1, "new-back")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.list_remove_async("shifted", 1, "front").await.unwrap(),
        1
    );
    assert_eq!(
        db.list_range_async("shifted", 0, -1).await.unwrap(),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    let after = db.list_meta_async("shifted").await.unwrap().unwrap();
    assert_eq!((after.tail - after.head) as usize, 3);
}

#[tokio::test]
async fn streaming_list_remove_preserves_direction_counts_and_cross_zero_layout() {
    let db = test_db();
    db.list_push_right_async(
        "stream-remove",
        &[
            "a".to_string(),
            "x".to_string(),
            "b".to_string(),
            "x".to_string(),
            "c".to_string(),
            "x".to_string(),
            "d".to_string(),
        ],
        false,
    )
    .await
    .unwrap();
    db.promote_packed_list_async("stream-remove").await.unwrap();
    db.list_push_left_async(
        "stream-remove",
        &["left".to_string(), "x".to_string()],
        false,
    )
    .await
    .unwrap();
    let before = db.list_meta_async("stream-remove").await.unwrap().unwrap();
    assert!(before.head < 0 && before.tail > 0);

    assert_eq!(
        db.list_remove_async("stream-remove", 2, "x").await.unwrap(),
        2
    );
    assert_eq!(
        db.list_range_async("stream-remove", 0, -1).await.unwrap(),
        vec!["left", "a", "b", "x", "c", "x", "d"]
    );
    assert_eq!(
        db.list_remove_async("stream-remove", -1, "x")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.list_range_async("stream-remove", 0, -1).await.unwrap(),
        vec!["left", "a", "b", "x", "c", "d"]
    );
    assert_eq!(
        db.list_remove_async("stream-remove", 0, "x").await.unwrap(),
        1
    );
    assert_eq!(
        db.list_range_async("stream-remove", 0, -1).await.unwrap(),
        vec!["left", "a", "b", "c", "d"]
    );
}

#[tokio::test]
async fn mixed_direction_pop_batch_moves_values_without_duplicate_copies() {
    let db = test_db();
    db.list_push_right_async(
        "mixed-pop",
        &(0..12).map(|index| format!("v{index}")).collect::<Vec<_>>(),
        false,
    )
    .await
    .unwrap();

    let replies = db
        .list_pop_many_batch_async(&[
            ("mixed-pop", true, 2),
            ("mixed-pop", false, 3),
            ("mixed-pop", true, 1),
            ("mixed-pop", false, 2),
        ])
        .await
        .into_iter()
        .map(Result::unwrap)
        .map(|values| {
            values
                .into_iter()
                .map(|value| String::from_utf8(value).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        replies,
        vec![
            vec!["v0", "v1"],
            vec!["v11", "v10", "v9"],
            vec!["v2"],
            vec!["v8", "v7"],
        ]
    );
    assert_eq!(
        db.list_range_async("mixed-pop", 0, -1).await.unwrap(),
        vec!["v3", "v4", "v5", "v6"]
    );
}

#[tokio::test]
async fn list_remove_rewrites_the_shorter_side_for_matches_near_each_end() {
    let db = test_db();
    let values = (0..20).map(|index| format!("v{index}")).collect::<Vec<_>>();
    db.list_push_right_async("remove-near-head", &values, false)
        .await
        .unwrap();
    assert_eq!(
        db.list_remove_async("remove-near-head", 1, "v1")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.list_range_async("remove-near-head", 0, -1)
            .await
            .unwrap(),
        values
            .iter()
            .filter(|value| value.as_str() != "v1")
            .cloned()
            .collect::<Vec<_>>()
    );

    db.list_push_right_async("remove-near-tail", &values, false)
        .await
        .unwrap();
    assert_eq!(
        db.list_remove_async("remove-near-tail", -1, "v18")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.list_range_async("remove-near-tail", 0, -1)
            .await
            .unwrap(),
        values
            .iter()
            .filter(|value| value.as_str() != "v18")
            .cloned()
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn stale_expired_list_cache_cannot_delete_recreated_list() {
    let db = test_db();
    db.list_push_right_async("queue", &["new".to_string()], false)
        .await
        .unwrap();
    let current = db.list_meta_async("queue").await.unwrap().unwrap();

    db.list_meta_cache.insert(
        db.mk("queue"),
        ListMeta {
            expire_ms: now_ms().saturating_sub(1),
            version: current.version,
            head: current.head,
            tail: current.tail,
        },
    );

    assert_eq!(db.list_len_async("queue").await.unwrap(), 1);
    assert_eq!(
        db.list_index_async("queue", 0).await.unwrap(),
        Some("new".to_string())
    );
    assert!(db.exists("queue").unwrap());
}

#[test]
fn list_native_ops_reject_wrong_type() {
    let db = test_db();
    db.insert("plain".to_string(), Structure::String("value".to_string()))
        .unwrap();

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
        db.get("queue").unwrap(),
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

    db.insert_string_ref("plain-destination", "value").unwrap();
    assert!(
        db.list_move("source", "plain-destination", false, true)
            .is_err()
    );
    assert_eq!(
        db.list_range("source", 0, -1).unwrap(),
        vec!["b".to_string(), "a".to_string()]
    );
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

    db.insert_string_ref("plain", "value").unwrap();
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
    assert!(!db.exists("queue").unwrap());
}
