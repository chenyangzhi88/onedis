use super::*;

#[test]
fn set_is_stored_and_loaded_via_kv_entries() {
    let db = test_db();
    let set = HashSet::from(["a".to_string(), "b".to_string()]);

    db.insert("tags".to_string(), Structure::Set(set.clone()));

    assert!(matches!(
        db.get("tags"),
        Some(Structure::Set(value)) if value == set
    ));

    assert_eq!(db.len(), 1);
}

#[test]
fn set_native_ops_use_member_level_storage() {
    let db = test_db();

    assert_eq!(
        db.set_add(
            "tags",
            &["rust".to_string(), "db".to_string(), "rust".to_string()]
        )
        .unwrap(),
        2
    );
    assert_eq!(db.set_len("tags").unwrap(), 2);
    assert!(db.set_contains("tags", "rust").unwrap());
    assert!(!db.set_contains("tags", "redis").unwrap());

    let mut members = db.set_members("tags").unwrap();
    members.sort();
    assert_eq!(members, vec!["db".to_string(), "rust".to_string()]);
}

#[test]
fn set_remove_and_pop_cleanup_meta_when_empty() {
    let db = test_db();
    db.set_add("tags", &["a".to_string(), "b".to_string(), "c".to_string()])
        .unwrap();

    assert_eq!(
        db.set_remove("tags", &["missing".to_string(), "a".to_string()])
            .unwrap(),
        1
    );
    let popped = db.set_pop("tags", 10).unwrap();
    assert_eq!(popped.len(), 2);
    assert_eq!(db.set_len("tags").unwrap(), 0);
    assert!(!db.exists("tags"));
}

#[test]
fn set_remove_deduplicates_requested_members() {
    let db = test_db();
    db.set_add("tags", &["a".to_string(), "b".to_string()])
        .unwrap();

    assert_eq!(
        db.set_remove("tags", &["a".to_string(), "a".to_string()])
            .unwrap(),
        1
    );
    assert_eq!(db.set_len("tags").unwrap(), 1);
    assert!(!db.set_contains("tags", "a").unwrap());
    assert!(db.set_contains("tags", "b").unwrap());
}

#[test]
fn set_pop_single_member_keeps_remaining_set() {
    let db = test_db();
    db.set_add("tags", &["a".to_string(), "b".to_string(), "c".to_string()])
        .unwrap();

    let popped = db.set_pop("tags", 1).unwrap();

    assert_eq!(popped.len(), 1);
    assert!(db.exists("tags"));
    assert_eq!(db.set_len("tags").unwrap(), 2);
    assert!(!db.set_contains("tags", &popped[0]).unwrap());
}

#[test]
fn set_pop_zero_is_noop() {
    let db = test_db();
    db.set_add("tags", &["a".to_string(), "b".to_string()])
        .unwrap();

    let popped = db.set_pop("tags", 0).unwrap();

    assert!(popped.is_empty());
    assert_eq!(db.set_len("tags").unwrap(), 2);
}

#[test]
fn set_scan_paginates_and_filters_by_match() {
    let db = test_db();
    db.set_add(
        "tags",
        &[
            "name".to_string(),
            "nickname".to_string(),
            "city".to_string(),
        ],
    )
    .unwrap();

    let (next_cursor, first_page) = db.set_scan("tags", 0, "*", 2).unwrap();
    assert_eq!(next_cursor, 2);
    assert_eq!(first_page.len(), 2);

    let (done_cursor, matched) = db.set_scan("tags", 0, "*name*", 10).unwrap();
    assert_eq!(done_cursor, 0);
    assert_eq!(matched, vec!["name".to_string(), "nickname".to_string()]);
}

#[test]
fn set_native_ops_reject_wrong_type() {
    let db = test_db();
    db.insert("plain".to_string(), Structure::String("value".to_string()));

    assert!(db.set_add("plain", &["x".to_string()]).is_err());
    assert!(db.set_remove("plain", &["x".to_string()]).is_err());
    assert!(db.set_contains("plain", "x").is_err());
    assert!(db.set_len("plain").is_err());
    assert!(db.set_members("plain").is_err());
    assert!(db.set_pop("plain", 1).is_err());
    assert!(db.set_scan("plain", 0, "*", 10).is_err());
}

#[test]
fn zset_is_stored_and_loaded_via_kv_entries() {
    let db = test_db();
    let set = BTreeMap::from([("alice".to_string(), 1.5), ("bob".to_string(), 2.0)]);

    db.insert("leaders".to_string(), Structure::SortedSet(set.clone()));

    assert!(matches!(
        db.get("leaders"),
        Some(Structure::SortedSet(value)) if value == set
    ));
}

#[test]
fn zset_native_ops_use_dual_index_storage() {
    let db = test_db();

    assert_eq!(
        db.zset_add(
            "leaders",
            &[
                (2.0, "bob".to_string()),
                (1.0, "alice".to_string()),
                (2.0, "bob".to_string()),
            ],
        )
        .unwrap(),
        2
    );
    assert_eq!(db.zset_card("leaders").unwrap(), 2);
    assert_eq!(db.zset_score("leaders", "alice").unwrap(), Some(1.0));

    assert_eq!(
        db.zset_add("leaders", &[(3.0, "alice".to_string())])
            .unwrap(),
        0
    );
    assert_eq!(db.zset_score("leaders", "alice").unwrap(), Some(3.0));
}

#[test]
fn zset_rank_and_count_follow_score_then_member_order() {
    let db = test_db();
    db.zset_add(
        "leaders",
        &[
            (2.0, "bob".to_string()),
            (1.0, "carol".to_string()),
            (1.0, "alice".to_string()),
            (3.0, "dave".to_string()),
        ],
    )
    .unwrap();

    assert_eq!(db.zset_rank("leaders", "alice").unwrap(), Some(0));
    assert_eq!(db.zset_rank("leaders", "carol").unwrap(), Some(1));
    assert_eq!(db.zset_rank("leaders", "bob").unwrap(), Some(2));
    assert_eq!(db.zset_count("leaders", 1.0, 2.0).unwrap(), 3);
}

#[test]
fn zset_remove_cleans_up_meta_when_empty() {
    let db = test_db();
    db.zset_add(
        "leaders",
        &[(1.0, "alice".to_string()), (2.0, "bob".to_string())],
    )
    .unwrap();

    assert_eq!(
        db.zset_remove("leaders", &["alice".to_string(), "missing".to_string()])
            .unwrap(),
        1
    );
    assert_eq!(db.zset_remove("leaders", &["bob".to_string()]).unwrap(), 1);
    assert_eq!(db.zset_card("leaders").unwrap(), 0);
    assert!(!db.exists("leaders"));
}

#[test]
fn zset_range_apis_follow_rank_index() {
    let db = test_db();
    db.zset_add(
        "leaders",
        &[
            (2.0, "bob".to_string()),
            (1.0, "alice".to_string()),
            (3.0, "dave".to_string()),
            (1.0, "carol".to_string()),
        ],
    )
    .unwrap();

    assert_eq!(
        db.zset_range("leaders", 0, 2, false).unwrap(),
        vec![
            ("alice".to_string(), 1.0),
            ("carol".to_string(), 1.0),
            ("bob".to_string(), 2.0)
        ]
    );
    assert_eq!(
        db.zset_range("leaders", 0, 1, true).unwrap(),
        vec![("dave".to_string(), 3.0), ("bob".to_string(), 2.0)]
    );
    assert_eq!(
        db.zset_range("leaders", -2, -1, false).unwrap(),
        vec![("bob".to_string(), 2.0), ("dave".to_string(), 3.0)]
    );
}

#[test]
fn zset_range_by_score_and_scan_share_rank_storage() {
    let db = test_db();
    db.zset_add(
        "leaders",
        &[
            (1.0, "alice".to_string()),
            (2.0, "bob".to_string()),
            (3.0, "carol".to_string()),
            (4.0, "dave".to_string()),
        ],
    )
    .unwrap();

    assert_eq!(
        db.zset_range_by_score("leaders", 2.0, 3.0).unwrap(),
        vec![("bob".to_string(), 2.0), ("carol".to_string(), 3.0)]
    );

    let (next_cursor, first_page) = db.zset_scan("leaders", 0, "*", 2).unwrap();
    assert_eq!(next_cursor, 2);
    assert_eq!(first_page.len(), 2);

    let (done_cursor, matched) = db.zset_scan("leaders", 0, "*a*", 10).unwrap();
    assert_eq!(done_cursor, 0);
    assert_eq!(
        matched,
        vec![
            ("alice".to_string(), 1.0),
            ("carol".to_string(), 3.0),
            ("dave".to_string(), 4.0)
        ]
    );
}

#[test]
fn zset_native_ops_reject_wrong_type() {
    let db = test_db();
    db.insert("plain".to_string(), Structure::String("value".to_string()));

    assert!(db.zset_add("plain", &[(1.0, "x".to_string())]).is_err());
    assert!(db.zset_remove("plain", &["x".to_string()]).is_err());
    assert!(db.zset_score("plain", "x").is_err());
    assert!(db.zset_card("plain").is_err());
    assert!(db.zset_rank("plain", "x").is_err());
    assert!(db.zset_count("plain", 0.0, 1.0).is_err());
    assert!(db.zset_range("plain", 0, -1, false).is_err());
    assert!(db.zset_range_by_score("plain", 0.0, 1.0).is_err());
    assert!(db.zset_scan("plain", 0, "*", 10).is_err());
}
