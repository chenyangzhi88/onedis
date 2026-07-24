use super::*;

#[tokio::test]
async fn async_set_and_zset_scan_helpers_match_sync_results() {
    let db = test_db();
    db.set_add(
        "letters",
        &["a".to_string(), "b".to_string(), "c".to_string()],
    )
    .unwrap();
    db.zset_add("ranked", &[(1.0, "a".to_string()), (2.0, "b".to_string())])
        .unwrap();

    assert_eq!(db.set_members_async("letters").await.unwrap().len(), 3);
    assert_eq!(db.set_scan_async("letters", 0, "*", 10).await.unwrap().0, 0);
    assert_eq!(
        db.zset_scan_async("ranked", 0, "*", 10).await.unwrap(),
        (0, vec![("a".to_string(), 1.0), ("b".to_string(), 2.0)])
    );
}

#[tokio::test]
async fn set_async_store_random_move_and_error_paths_cover_edges() {
    let db = test_db();

    assert_eq!(
        db.set_add_async(
            "left",
            &[
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "c".to_string(),
            ],
        )
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        db.set_add_async("right", &["b".to_string(), "d".to_string()])
            .await
            .unwrap(),
        2
    );
    assert!(db.set_contains_async("left", "a").await.unwrap());
    assert!(!db.set_contains_async("left", "missing").await.unwrap());
    assert!(db.set_move_async("left", "right", "a").await.unwrap());
    assert!(!db.set_move_async("left", "right", "missing").await.unwrap());
    assert!(db.set_contains("right", "a").unwrap());
    db.insert_string("wrong-destination".to_string(), "value".to_string(), None);
    assert!(
        db.set_move_async("left", "wrong-destination", "b")
            .await
            .is_err()
    );
    assert!(db.set_contains("left", "b").unwrap());

    assert_eq!(
        db.set_intersection_card(&["right".to_string(), "left".to_string()], 1)
            .unwrap(),
        1
    );
    assert_eq!(
        db.set_diff_async(&["right".to_string(), "left".to_string()])
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.set_intersection_async(&["right".to_string(), "left".to_string()])
            .await
            .unwrap(),
        HashSet::from(["b".to_string()])
    );
    assert_eq!(
        db.set_union_async(&["right".to_string(), "left".to_string()])
            .await
            .unwrap()
            .len(),
        4
    );

    assert_eq!(
        db.set_diff_store_async("diff-dst", &["right".to_string(), "left".to_string()])
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        db.set_intersection_store_async("inter-dst", &["right".to_string(), "left".to_string()],)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.set_union_store_async("union-dst", &["right".to_string(), "left".to_string()])
            .await
            .unwrap(),
        4
    );
    assert!(db.set_random_members("right", None).unwrap().is_some());
    assert_eq!(
        db.set_random_members("right", Some(-5))
            .unwrap()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(
        db.set_random_members_async("right", Some(2))
            .await
            .unwrap()
            .unwrap()
            .len(),
        2
    );
    assert!(db.set_random_members("missing", None).unwrap().is_none());

    let popped = db.set_pop_async("union-dst", 2).await.unwrap();
    assert_eq!(popped.len(), 2);
    assert_eq!(
        db.set_pop_async("union-dst", 0).await.unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        db.set_remove_async("missing", &["x".to_string()])
            .await
            .unwrap(),
        0
    );

    db.insert_string_ref("not-set", "value");
    assert_eq!(
        db.set_diff(&["not-set".to_string()])
            .unwrap_err()
            .to_string(),
        WRONG_TYPE_ERROR
    );
    assert!(db.set_diff_async(&[]).await.is_err());
    assert!(db.set_intersection_async(&[]).await.is_err());
}

#[tokio::test]
async fn set_pop_async_repairs_missing_slot_index_entries() {
    let db = test_db();

    assert_eq!(
        db.set_add("repair", &["a".to_string(), "b".to_string()])
            .unwrap(),
        2
    );
    let meta = db.set_meta("repair").unwrap().unwrap();
    assert!(
        db.store
            .delete_key(&set_slot_key(db.db_index, "repair", meta.version, 0))
    );

    let popped = db.set_pop_async("repair", 1).await.unwrap();
    assert_eq!(popped.len(), 1);
    assert!(matches!(popped[0].as_str(), "a" | "b"));

    let remaining = db.set_members("repair").unwrap();
    assert_eq!(remaining.len(), 1);
    let repaired = db.set_meta("repair").unwrap().unwrap();
    assert!(
        db.store
            .contains_key(&set_slot_key(db.db_index, "repair", repaired.version, 0))
    );
    assert_eq!(
        db.set_pop_async("missing", 1).await.unwrap(),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn set_and_list_async_mutations_cover_rebuild_delete_and_concurrency_paths() {
    let db = test_db();

    assert_eq!(
        db.set_add_async(
            "s",
            &[
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "d".to_string(),
            ],
        )
        .await
        .unwrap(),
        4
    );
    assert_eq!(
        db.set_add_async("s", &["b".to_string(), "e".to_string()])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.set_remove_async("s", &["a".to_string()]).await.unwrap(),
        1
    );
    assert_eq!(
        db.set_remove_async(
            "s",
            &[
                "missing".to_string(),
                "b".to_string(),
                "c".to_string(),
                "c".to_string(),
            ],
        )
        .await
        .unwrap(),
        2
    );
    assert_eq!(db.set_len("s").unwrap(), 2);
    let popped = db.set_pop_async("s", 99).await.unwrap();
    assert_eq!(popped.len(), 2);
    assert_eq!(db.set_len("s").unwrap(), 0);

    let seek_members = (0..10).map(|idx| format!("m{idx}")).collect::<Vec<_>>();
    assert_eq!(
        db.set_add_async("seek", &seek_members).await.unwrap(),
        seek_members.len()
    );
    let popped = db.set_pop_async("seek", 3).await.unwrap();
    assert_eq!(popped.len(), 3);
    assert_eq!(db.set_len("seek").unwrap(), 7);
    assert_eq!(db.set_scan_async("seek", 99, "*", 10).await.unwrap().0, 0);

    db.insert_string_ref("plain-set", "value");
    assert!(
        db.set_add_async("plain-set", &["x".to_string()])
            .await
            .is_err()
    );
    assert!(
        db.set_remove_async("plain-set", &["x".to_string()])
            .await
            .is_err()
    );

    db.list_push_right_async(
        "list",
        &[
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
            "a".to_string(),
        ],
        false,
    )
    .await
    .unwrap();
    assert_eq!(db.list_remove_async("list", 1, "a").await.unwrap(), 1);
    assert_eq!(
        db.list_range_async("list", 0, -1).await.unwrap(),
        vec![
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
            "a".to_string()
        ]
    );
    assert_eq!(db.list_remove_async("list", -1, "a").await.unwrap(), 1);
    assert_eq!(
        db.list_range_async("list", 0, -1).await.unwrap(),
        vec!["b".to_string(), "a".to_string(), "c".to_string()]
    );
    assert_eq!(db.list_remove_async("list", 0, "a").await.unwrap(), 1);
    assert_eq!(
        db.list_range_async("list", 0, -1).await.unwrap(),
        vec!["b".to_string(), "c".to_string()]
    );
    assert_eq!(db.list_remove_async("list", 0, "missing").await.unwrap(), 0);

    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for worker in 0..8 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            let members = (0..20)
                .map(|offset| format!("m{}", worker * 10 + offset))
                .collect::<Vec<_>>();
            db.set_add_async("shared", &members).await.unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(db.set_len("shared").unwrap(), 90);
}
