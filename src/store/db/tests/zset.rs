use super::*;

#[tokio::test]
async fn ordered_zset_increment_pipeline_batch_preserves_command_results() {
    let db = test_db();
    db.zset_add_async(
        "increment-batch",
        &[(10.0, "same".to_string()), (3.0, "other".to_string())],
    )
    .await
    .unwrap();

    let replies = db
        .zset_increment_batch_async(&[
            ("increment-batch", 1.5, "same"),
            ("increment-batch", 2.0, "same"),
            ("increment-batch", 5.0, "same"),
            ("increment-batch", -5.0, "same"),
            ("increment-batch", -1.0, "other"),
            ("created-batch", 4.0, "new"),
        ])
        .await;
    assert!(matches!(replies[0], Ok(score) if score == 11.5));
    assert!(matches!(replies[1], Ok(score) if score == 13.5));
    assert!(matches!(replies[2], Ok(score) if score == 18.5));
    assert!(matches!(replies[3], Ok(score) if score == 13.5));
    assert!(matches!(replies[4], Ok(score) if score == 2.0));
    assert!(matches!(replies[5], Ok(score) if score == 4.0));
    assert_eq!(
        db.zset_score_async("increment-batch", "same")
            .await
            .unwrap(),
        Some(13.5)
    );
    assert_eq!(
        db.zset_score_async("increment-batch", "other")
            .await
            .unwrap(),
        Some(2.0)
    );
    assert_eq!(
        db.zset_score_async("created-batch", "new").await.unwrap(),
        Some(4.0)
    );
}

#[tokio::test]
async fn ordered_zset_pop_pipeline_batch_preserves_both_ends_and_empty_replies() {
    let db = test_db();
    db.zset_add(
        "batch-pop",
        &[
            (1.0, "a".to_string()),
            (2.0, "b".to_string()),
            (3.0, "c".to_string()),
            (4.0, "d".to_string()),
        ],
    )
    .unwrap();
    let replies = db
        .zset_pop_batch_async(&[
            ("batch-pop", true, 1),
            ("batch-pop", false, 2),
            ("batch-pop", true, 2),
            ("batch-pop", false, 1),
            ("missing-pop", true, 1),
        ])
        .await;
    assert!(matches!(&replies[0], Ok(entries) if entries == &vec![("a".to_string(), 1.0)]));
    assert!(
        matches!(&replies[1], Ok(entries) if entries == &vec![("d".to_string(), 4.0), ("c".to_string(), 3.0)])
    );
    assert!(matches!(&replies[2], Ok(entries) if entries == &vec![("b".to_string(), 2.0)]));
    assert!(matches!(&replies[3], Ok(entries) if entries.is_empty()));
    assert!(matches!(&replies[4], Ok(entries) if entries.is_empty()));
    assert!(!db.exists_readonly("batch-pop"));
}

#[tokio::test]
async fn zset_card_cache_invalidates_on_member_mutations_and_score_ranges_are_bounded() {
    let db = test_db();
    db.zset_add(
        "bounded",
        &[
            (-0.0, "negative-zero".to_string()),
            (0.0, "positive-zero".to_string()),
            (1.0, "one".to_string()),
            (2.0, "two".to_string()),
            (3.0, "three".to_string()),
        ],
    )
    .unwrap();

    assert_eq!(db.zset_card_async("bounded").await.unwrap(), 5);
    assert_eq!(db.zset_card_async("bounded").await.unwrap(), 5);
    assert_eq!(
        db.zset_count_bounded("bounded", 0.0, true, 2.0, false)
            .unwrap(),
        3
    );
    assert_eq!(
        db.zset_count_bounded_async("bounded", 0.0, false, 2.0, true)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        db.zset_range_by_score_async("bounded", 1.0, 2.0)
            .await
            .unwrap(),
        vec![("one".to_string(), 1.0), ("two".to_string(), 2.0)]
    );
    assert_eq!(
        db.zset_range_by_score_window_async("bounded", 0.0, true, 3.0, false, true, Some((1, 2)))
            .await
            .unwrap(),
        vec![("one".to_string(), 1.0), ("positive-zero".to_string(), 0.0)]
    );

    db.zset_add_async("bounded", &[(4.0, "four".to_string())])
        .await
        .unwrap();
    assert_eq!(db.zset_card_async("bounded").await.unwrap(), 6);
    db.zset_remove_async("bounded", &["one".to_string()])
        .await
        .unwrap();
    assert_eq!(db.zset_card_async("bounded").await.unwrap(), 5);

    db.zset_add(
        "remove-last",
        &[(1.0, "a".to_string()), (2.0, "b".to_string())],
    )
    .unwrap();
    assert_eq!(
        db.zset_remove_async("remove-last", &["a".to_string()])
            .await
            .unwrap(),
        1
    );
    assert!(db.exists_readonly("remove-last"));
    assert_eq!(
        db.zset_remove_async("remove-last", &["b".to_string()])
            .await
            .unwrap(),
        1
    );
    assert!(!db.exists_readonly("remove-last"));
}

#[tokio::test]
async fn zset_lex_ranges_push_open_and_closed_bounds_into_member_index() {
    use crate::cmds::sorted_set::zrange::LexBound;

    let db = test_db();
    db.zset_add(
        "lex-bounded",
        &[
            (0.0, "a".to_string()),
            (0.0, "aa".to_string()),
            (0.0, "ab".to_string()),
            (0.0, "b".to_string()),
            (0.0, "ba".to_string()),
        ],
    )
    .unwrap();
    let exclusive_a = LexBound::Value {
        value: "a".to_string(),
        inclusive: false,
    };
    let inclusive_b = LexBound::Value {
        value: "b".to_string(),
        inclusive: true,
    };
    assert_eq!(
        db.zset_range_by_lex_async("lex-bounded", &exclusive_a, &inclusive_b)
            .await
            .unwrap(),
        vec![
            ("aa".to_string(), 0.0),
            ("ab".to_string(), 0.0),
            ("b".to_string(), 0.0),
        ]
    );
    assert_eq!(
        db.zset_lex_count_async("lex-bounded", &exclusive_a, &inclusive_b)
            .await
            .unwrap(),
        3
    );

    let inclusive_ab = LexBound::Value {
        value: "ab".to_string(),
        inclusive: true,
    };
    let exclusive_ba = LexBound::Value {
        value: "ba".to_string(),
        inclusive: false,
    };
    assert_eq!(
        db.zset_remove_range_by_lex_async("lex-bounded", &inclusive_ab, &exclusive_ba)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        db.zset_range_by_lex_async(
            "lex-bounded",
            &LexBound::NegInfinity,
            &LexBound::PosInfinity,
        )
        .await
        .unwrap(),
        vec![
            ("a".to_string(), 0.0),
            ("aa".to_string(), 0.0),
            ("ba".to_string(), 0.0)
        ]
    );
    assert_eq!(
        db.zset_lex_count_async(
            "lex-bounded",
            &LexBound::PosInfinity,
            &LexBound::PosInfinity,
        )
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn zset_random_and_rank_paths_preserve_count_and_order_semantics() {
    let db = test_db();
    let members = (0..20)
        .map(|index| (index as f64, format!("m{index:02}")))
        .collect::<Vec<_>>();
    db.zset_add("random-rank", &members).unwrap();

    let one = db
        .zset_random_members_async("random-rank", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(one.len(), 1);
    let distinct = db
        .zset_random_members_async("random-rank", Some(10))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(distinct.len(), 10);
    assert_eq!(
        distinct
            .iter()
            .map(|(member, _)| member)
            .collect::<HashSet<_>>()
            .len(),
        10
    );
    assert_eq!(
        db.zset_random_members_async("random-rank", Some(-25))
            .await
            .unwrap()
            .unwrap()
            .len(),
        25
    );
    assert_eq!(
        db.zset_rank_async("random-rank", "m07").await.unwrap(),
        Some(7)
    );
    assert_eq!(
        db.zset_rev_rank_async("random-rank", "m07").await.unwrap(),
        Some(12)
    );
    let (next, entries) = db.zset_scan_async("random-rank", 7, "*", 3).await.unwrap();
    assert_eq!(next, 10);
    assert_eq!(
        entries,
        vec![
            ("m07".to_string(), 7.0),
            ("m08".to_string(), 8.0),
            ("m09".to_string(), 9.0)
        ]
    );
}

#[tokio::test]
async fn zset_composite_random_pop_and_async_paths_cover_edges() {
    let db = test_db();
    db.zset_add(
        "z1",
        &[
            (1.0, "a".to_string()),
            (2.0, "b".to_string()),
            (3.0, "c".to_string()),
        ],
    )
    .unwrap();
    db.zset_add(
        "z2",
        &[
            (10.0, "b".to_string()),
            (20.0, "c".to_string()),
            (30.0, "d".to_string()),
        ],
    )
    .unwrap();

    assert!(db.zset_random_members("missing", None).unwrap().is_none());
    assert_eq!(
        db.zset_random_members("z1", Some(-5))
            .unwrap()
            .unwrap()
            .len(),
        5
    );
    assert_eq!(
        db.zset_random_members_async("z1", Some(2))
            .await
            .unwrap()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        db.zset_rev_range_by_score("z1", 3.0, 1.0).unwrap()[0],
        ("c".to_string(), 3.0)
    );
    assert_eq!(
        db.zset_rev_range_by_score_async("z1", 3.0, 1.0)
            .await
            .unwrap()[0],
        ("c".to_string(), 3.0)
    );
    assert_eq!(
        db.zset_diff(&["z1".to_string(), "z2".to_string()]).unwrap(),
        vec![("a".to_string(), 1.0)]
    );
    assert_eq!(
        db.zset_diff_async(&["z1".to_string(), "z2".to_string()])
            .await
            .unwrap(),
        vec![("a".to_string(), 1.0)]
    );
    assert_eq!(db.zset_diff(&[]).unwrap(), Vec::<(String, f64)>::new());

    assert_eq!(
        db.zset_union_or_inter(
            &["z1".to_string(), "z2".to_string()],
            &[2.0, 1.0],
            ZsetAggregate::Max,
            false,
        )
        .unwrap()
        .last()
        .unwrap(),
        &("d".to_string(), 30.0)
    );
    assert_eq!(
        db.zset_union_or_inter_async(
            &["z1".to_string(), "z2".to_string()],
            &[1.0, 1.0],
            ZsetAggregate::Min,
            true,
        )
        .await
        .unwrap(),
        vec![("b".to_string(), 2.0), ("c".to_string(), 3.0)]
    );
    assert_eq!(
        db.zset_union_or_inter(
            &["z1".to_string(), "z2".to_string()],
            &[1.0, 1.0],
            ZsetAggregate::Sum,
            true,
        )
        .unwrap(),
        vec![("b".to_string(), 12.0), ("c".to_string(), 23.0)]
    );

    assert_eq!(
        db.zset_pop("z1", true, 1).unwrap(),
        vec![("a".to_string(), 1.0)]
    );
    assert_eq!(
        db.zset_pop_async("z1", false, 1).await.unwrap(),
        vec![("c".to_string(), 3.0)]
    );
    assert_eq!(
        db.zset_multi_pop(&["missing".to_string(), "z1".to_string()], true, 2)
            .unwrap()
            .unwrap()
            .0,
        "z1"
    );
    assert!(
        db.zset_multi_pop_async(&["missing".to_string()], true, 1)
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn zset_limited_filter_stops_after_enough_matches() {
    let db = test_db();
    let members = (0..10)
        .map(|index| (index as f64, format!("m{index:02}")))
        .collect::<Vec<_>>();
    db.zset_add("limited", &members).unwrap();

    let mut visited = 0usize;
    let entries = db
        .zset_filter_entries_limited("limited", 3, |_, score| {
            visited += 1;
            score >= 3.0
        })
        .unwrap();

    assert_eq!(
        entries,
        vec![
            ("m03".to_string(), 3.0),
            ("m04".to_string(), 4.0),
            ("m05".to_string(), 5.0),
        ]
    );
    assert_eq!(visited, 6);
}

#[tokio::test]
async fn zset_async_rank_range_store_remove_and_error_paths_cover_edges() {
    let db = test_db();

    assert_eq!(
        db.zset_add_async(
            "leaders",
            &[
                (2.0, "bob".to_string()),
                (1.0, "alice".to_string()),
                (1.0, "carol".to_string()),
                (4.0, "dave".to_string()),
                (5.0, "dave".to_string()),
            ],
        )
        .await
        .unwrap(),
        4
    );
    assert_eq!(db.zset_card_async("leaders").await.unwrap(), 4);
    assert_eq!(
        db.zset_score_async("leaders", "dave").await.unwrap(),
        Some(5.0)
    );
    assert_eq!(
        db.zset_increment_by_async("leaders", "bob", 0.5)
            .await
            .unwrap(),
        2.5
    );
    assert!(
        db.zset_increment_by_async("leaders", "bob", f64::NAN)
            .await
            .is_err()
    );
    assert_eq!(
        db.zset_rank_async("leaders", "alice").await.unwrap(),
        Some(0)
    );
    assert_eq!(
        db.zset_rev_rank_async("leaders", "alice").await.unwrap(),
        Some(3)
    );
    assert_eq!(
        db.zset_rank_async("leaders", "missing").await.unwrap(),
        None
    );
    assert_eq!(db.zset_count_async("leaders", 1.0, 2.5).await.unwrap(), 3);
    assert_eq!(
        db.zset_range_async("leaders", -2, -1, false).await.unwrap(),
        vec![("bob".to_string(), 2.5), ("dave".to_string(), 5.0)]
    );
    assert_eq!(
        db.zset_range_async("leaders", 10, 20, false).await.unwrap(),
        Vec::<(String, f64)>::new()
    );
    assert_eq!(
        db.zset_range_by_score_async("leaders", 1.0, 2.5)
            .await
            .unwrap(),
        vec![
            ("alice".to_string(), 1.0),
            ("carol".to_string(), 1.0),
            ("bob".to_string(), 2.5)
        ]
    );
    assert_eq!(
        db.zset_all_entries_async("missing").await.unwrap(),
        Vec::<(String, f64)>::new()
    );

    assert_eq!(
        db.zset_store_entries("stored-empty", Vec::new()).unwrap(),
        0
    );
    assert!(!db.exists("stored-empty"));
    assert_eq!(
        db.zset_store_entries(
            "stored",
            vec![("x".to_string(), 9.0), ("y".to_string(), 8.0)],
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.zset_store_entries_async("stored", Vec::new())
            .await
            .unwrap(),
        0
    );
    assert!(!db.exists("stored"));
    assert_eq!(
        db.zset_store_entries_async(
            "stored",
            vec![("x".to_string(), 9.0), ("y".to_string(), 8.0)],
        )
        .await
        .unwrap(),
        2
    );
    assert_eq!(db.zset_card_async("stored").await.unwrap(), 2);

    assert_eq!(
        db.zset_remove_range_by_rank_async("leaders", 0, 1)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        db.zset_remove_range_by_score_async("leaders", 2.0, 10.0)
            .await
            .unwrap(),
        2
    );
    assert!(!db.exists("leaders"));
    assert_eq!(
        db.zset_remove_async("leaders", &["missing".to_string()])
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        db.zset_pop_async("missing", true, 10).await.unwrap(),
        Vec::<(String, f64)>::new()
    );

    db.insert_string_ref("plain", "value");
    assert!(
        db.zset_add_async("plain", &[(1.0, "x".to_string())])
            .await
            .is_err()
    );
    assert!(
        db.zset_remove_async("plain", &["x".to_string()])
            .await
            .is_err()
    );
    assert!(db.zset_score_async("plain", "x").await.is_err());
    assert!(db.zset_card_async("plain").await.is_err());
    assert!(db.zset_rank_async("plain", "x").await.is_err());
    assert!(db.zset_count_async("plain", 0.0, 1.0).await.is_err());
    assert!(db.zset_range_async("plain", 0, -1, false).await.is_err());
    assert!(
        db.zset_range_by_score_async("plain", 0.0, 1.0)
            .await
            .is_err()
    );
    assert!(db.zset_scan_async("plain", 0, "*", 10).await.is_err());
}

#[tokio::test]
async fn concurrent_zset_writes_preserve_members_and_increments() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for index in 0..16 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            db.zset_add_async(
                "concurrent-members",
                &[(index as f64, format!("member-{index}"))],
            )
            .await
            .unwrap();
            db.zset_increment_by_async("concurrent-score", "member", 1.0)
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(db.zset_card_async("concurrent-members").await.unwrap(), 16);
    assert_eq!(
        db.zset_score_async("concurrent-score", "member")
            .await
            .unwrap(),
        Some(16.0)
    );
}

#[tokio::test]
async fn concurrent_same_member_zadd_keeps_exactly_one_rank_entry() {
    let db = Arc::new(test_db());
    db.zset_add_async("same-member", &[(0.0, "member".to_string())])
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for score in 1..=64 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            db.zset_add_async("same-member", &[(score as f64, "member".to_string())])
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), 0);
    }

    let (_, version) = db
        .zset_expire_ms_async("same-member")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        db.zset_members_raw_async("same-member", version)
            .await
            .len(),
        1
    );
    assert_eq!(
        db.zset_rank_entries_raw_async("same-member", version)
            .await
            .len(),
        1
    );
    assert_eq!(db.zset_card_async("same-member").await.unwrap(), 1);
    assert_eq!(
        db.zset_rank_async("same-member", "member").await.unwrap(),
        Some(0)
    );
}
