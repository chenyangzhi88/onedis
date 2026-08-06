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
    let right_members = HashSet::from(["a".to_string(), "b".to_string(), "d".to_string()]);
    let one_random = db.set_random_members("right", None).unwrap().unwrap();
    assert_eq!(one_random.len(), 1);
    assert!(right_members.contains(&one_random[0]));

    let repeated = db.set_random_members("right", Some(-5)).unwrap().unwrap();
    assert_eq!(repeated.len(), 5);
    assert!(repeated.iter().all(|member| right_members.contains(member)));

    let unique = db
        .set_random_members_async("right", Some(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unique.len(), 2);
    assert_eq!(unique.iter().collect::<HashSet<_>>().len(), 2);
    assert!(unique.iter().all(|member| right_members.contains(member)));
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
async fn set_pop_async_uses_only_member_entries_for_set_data() {
    let db = test_db();

    assert_eq!(
        db.set_add("repair", &["a".to_string(), "b".to_string()])
            .unwrap(),
        2
    );
    let meta = db.set_meta("repair").unwrap().unwrap();
    let member_prefix = set_member_prefix(db.db_index, "repair", meta.version);
    let owner_prefix = version_owner_prefix(db.db_index);
    assert_eq!(db.store.scan_prefix_raw(&member_prefix).len(), 2);
    assert_eq!(db.store.scan_prefix_raw(&owner_prefix).len(), 1);

    let popped = db.set_pop_async("repair", 1).await.unwrap();
    assert_eq!(popped.len(), 1);
    assert!(matches!(popped[0].as_str(), "a" | "b"));

    let remaining = db.set_members("repair").unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(db.store.scan_prefix_raw(&member_prefix).len(), 1);
    assert_eq!(
        db.set_pop_async("missing", 1).await.unwrap(),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn set_pop_async_does_not_mutate_an_inconsistent_set() {
    let db = test_db();
    assert_eq!(
        db.set_add("inconsistent-pop", &["a".to_string(), "b".to_string()])
            .unwrap(),
        2
    );

    let meta = db.set_meta("inconsistent-pop").unwrap().unwrap();
    db.store.put_raw(
        &db.mk("inconsistent-pop"),
        &encode_set_meta(meta.expire_ms, meta.version, 3),
    );

    let error = db.set_pop_async("inconsistent-pop", 3).await.unwrap_err();
    assert!(error.to_string().contains("metadata length"));
    let member_prefix = set_member_prefix(db.db_index, "inconsistent-pop", meta.version);
    assert_eq!(db.store.scan_prefix_raw(&member_prefix).len(), 2);
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

    let scan_members = (0..10).map(|idx| format!("m{idx}")).collect::<Vec<_>>();
    assert_eq!(
        db.set_add_async("scan-pop", &scan_members).await.unwrap(),
        scan_members.len()
    );
    let popped = db.set_pop_async("scan-pop", 3).await.unwrap();
    assert_eq!(popped.len(), 3);
    assert_eq!(popped.iter().collect::<HashSet<_>>().len(), 3);
    let remaining = db.set_members_async("scan-pop").await.unwrap();
    assert_eq!(remaining.len(), 7);
    assert!(
        popped
            .iter()
            .all(|member| !remaining.iter().any(|other| other == member))
    );
    let all_after_pop = popped
        .iter()
        .chain(&remaining)
        .cloned()
        .collect::<HashSet<_>>();
    assert_eq!(all_after_pop, scan_members.into_iter().collect());
    assert_eq!(db.set_len("scan-pop").unwrap(), 7);
    assert_eq!(
        db.set_scan_async("scan-pop", 99, "*", 10).await.unwrap().0,
        0
    );

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

#[tokio::test]
async fn concurrent_set_add_preserves_len_and_existing_member_noops() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for worker in 0..16 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            let members = (0..16)
                .map(|offset| format!("member-{}", worker * 16 + offset))
                .collect::<Vec<_>>();
            db.set_add_async("cas-set", &members).await.unwrap()
        }));
    }
    let mut added = 0usize;
    for task in tasks {
        added += task.await.unwrap();
    }
    assert_eq!(added, 256);
    assert_eq!(db.set_len_async("cas-set").await.unwrap(), 256);
    assert_eq!(
        db.set_random_members_async("cas-set", Some(256))
            .await
            .unwrap()
            .unwrap()
            .len(),
        256
    );

    let mut noops = Vec::new();
    for index in 0..64 {
        let db = Arc::clone(&db);
        noops.push(tokio::spawn(async move {
            db.set_add_async("cas-set", &[format!("member-{index}")])
                .await
                .unwrap()
        }));
    }
    for task in noops {
        assert_eq!(task.await.unwrap(), 0);
    }
    assert_eq!(db.set_len_async("cas-set").await.unwrap(), 256);
}

#[tokio::test]
async fn concurrent_set_pop_returns_each_member_once_and_preserves_len() {
    let db = Arc::new(test_db());
    let members = (0..100)
        .map(|index| format!("member-{index}"))
        .collect::<Vec<_>>();
    assert_eq!(
        db.set_add_async("concurrent-pop", &members).await.unwrap(),
        members.len()
    );

    let mut tasks = Vec::new();
    for _ in 0..128 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            db.set_pop_async("concurrent-pop", 1).await.unwrap()
        }));
    }

    let mut popped = HashSet::new();
    for task in tasks {
        for member in task.await.unwrap() {
            assert!(popped.insert(member));
        }
    }
    assert_eq!(popped, members.into_iter().collect());
    assert_eq!(db.set_len_async("concurrent-pop").await.unwrap(), 0);
    assert!(
        db.set_members_async("concurrent-pop")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn set_add_async_replaces_expired_structure_in_one_write_path() {
    let db = test_db();
    db.insert_string_ref("expired-then-set", "old-value");
    assert!(db.expire_async("expired-then-set".to_string(), 1).await);
    tokio::time::sleep(Duration::from_millis(5)).await;

    assert_eq!(
        db.set_add_async(
            "expired-then-set",
            &["a".to_string(), "b".to_string(), "a".to_string()],
        )
        .await
        .unwrap(),
        2
    );
    assert_eq!(db.set_len("expired-then-set").unwrap(), 2);
    assert!(db.set_contains("expired-then-set", "a").unwrap());
    assert!(db.set_contains("expired-then-set", "b").unwrap());
    assert_eq!(db.ttl_millis("expired-then-set"), -1);
}
