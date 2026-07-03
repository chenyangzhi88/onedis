use super::*;

#[tokio::test]
async fn hash_field_ttl_getex_setex_and_async_conditions_cover_edges() {
    let db = test_db();
    let fields = vec![
        ("a".to_string(), "1".to_string()),
        ("b".to_string(), "2".to_string()),
    ];

    assert!(
        db.hash_set_ex(
            "h",
            &fields,
            Some(StringExpireUpdate::RelativeMs(10_000)),
            false,
            false,
            false,
        )
        .unwrap()
    );
    let names = vec!["a".to_string(), "b".to_string(), "missing".to_string()];
    let ttls = db.hash_field_ttls("h", &names, true, false).unwrap();
    assert!(ttls[0] > 0);
    assert!(ttls[1] > 0);
    assert_eq!(ttls[2], -2);
    assert!(
        db.hash_field_ttls("missing-hash", &names, true, false)
            .unwrap()
            .iter()
            .all(|v| *v == -2)
    );

    assert!(
        !db.hash_set_ex(
            "h",
            &[("a".to_string(), "new".to_string())],
            None,
            true,
            true,
            false,
        )
        .unwrap()
    );
    assert!(
        !db.hash_set_ex(
            "h",
            &[("missing".to_string(), "new".to_string())],
            None,
            false,
            false,
            true,
        )
        .unwrap()
    );
    assert!(
        db.hash_set_ex(
            "h",
            &[("a".to_string(), "updated".to_string())],
            Some(StringExpireUpdate::Persist),
            false,
            false,
            true,
        )
        .unwrap()
    );
    assert_eq!(
        db.hash_field_ttls("h", &["a".to_string()], true, false)
            .unwrap(),
        vec![-1]
    );

    let future = now_ms().saturating_add(20_000);
    assert_eq!(
        db.hash_expire_fields_at_ms(
            "h",
            future,
            &["a".to_string(), "missing".to_string()],
            ExpireCondition::Nx,
        )
        .unwrap(),
        vec![1, -2]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms("h", future - 1, &["a".to_string()], ExpireCondition::Gt)
            .unwrap(),
        vec![0]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms("h", future + 1, &["a".to_string()], ExpireCondition::Gt)
            .unwrap(),
        vec![1]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms("h", future, &["a".to_string()], ExpireCondition::Lt)
            .unwrap(),
        vec![1]
    );
    assert!(
        db.hash_field_ttls("h", &["a".to_string()], false, true)
            .unwrap()[0]
            > 0
    );

    assert_eq!(
        db.hash_get_ex(
            "h",
            &["a".to_string(), "b".to_string()],
            Some(StringExpireUpdate::Persist),
        )
        .unwrap(),
        vec![Some("updated".to_string()), Some("2".to_string())]
    );
    assert_eq!(
        db.hash_get_del("h", &["b".to_string(), "missing".to_string()])
            .unwrap(),
        vec![Some("2".to_string()), None]
    );

    assert!(
        db.hash_set_ex_async(
            "async-h",
            &[("x".to_string(), "1".to_string())],
            Some(StringExpireUpdate::AbsoluteMs(
                now_ms().saturating_add(10_000)
            )),
            false,
            false,
            false,
        )
        .await
        .unwrap()
    );
    assert!(
        db.hash_field_ttls_async("async-h", &["x".to_string()], true, false)
            .await
            .unwrap()[0]
            > 0
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "async-h",
            now_ms().saturating_sub(1),
            &["x".to_string()],
            ExpireCondition::Always,
        )
        .await
        .unwrap(),
        vec![2]
    );
    assert_eq!(
        db.hash_get_ex_async(
            "async-h",
            &["x".to_string()],
            Some(StringExpireUpdate::RelativeMs(1_000)),
        )
        .await
        .unwrap(),
        vec![None]
    );
    assert_eq!(
        db.hash_persist_fields_async("missing-hash", &["x".to_string()])
            .await
            .unwrap(),
        vec![-2]
    );
}

#[tokio::test]
async fn hash_random_float_expire_and_async_read_paths_cover_edges() {
    let db = test_db();

    assert!(
        db.hash_random_fields("missing", None, false)
            .unwrap()
            .is_none()
    );
    db.hash_multi_set(
        "h",
        &HashMap::from([
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
            ("c".to_string(), "3".to_string()),
        ]),
    )
    .unwrap();
    assert_eq!(
        db.hash_random_fields("h", None, false)
            .unwrap()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        db.hash_random_fields("h", Some(-8), true)
            .unwrap()
            .unwrap()
            .len(),
        8
    );
    assert_eq!(
        db.hash_random_fields_async("h", Some(2), false)
            .await
            .unwrap()
            .unwrap()
            .len(),
        2
    );

    assert_eq!(
        db.hash_multi_get_async("h", &["a".to_string(), "missing".to_string()])
            .await
            .unwrap(),
        vec![Some("1".to_string()), None]
    );
    let mut async_all = db.hash_get_all_async("h").await.unwrap();
    async_all.sort();
    assert_eq!(async_all.len(), 3);
    let mut async_keys = db.hash_keys_async("h").await.unwrap();
    async_keys.sort();
    assert_eq!(
        async_keys,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    let mut async_values = db.hash_values_async("h").await.unwrap();
    async_values.sort();
    assert_eq!(
        async_values,
        vec!["1".to_string(), "2".to_string(), "3".to_string()]
    );
    assert_eq!(
        db.hash_scan_async("h", 99, "*", 10).await.unwrap(),
        (0, Vec::new())
    );

    assert_eq!(
        db.hash_increment_by_float("h", "float", 1.25).unwrap(),
        "1.25"
    );
    assert_eq!(
        db.hash_increment_by_float_async("h", "float", 0.75)
            .await
            .unwrap(),
        "2"
    );
    db.hash_set("h", "bad-float", "not-a-number").unwrap();
    assert!(db.hash_increment_by_float("h", "bad-float", 1.0).is_err());
    db.hash_set("h", "inf", "inf").unwrap();
    assert!(db.hash_increment_by_float("h", "inf", 1.0).is_err());
    assert!(
        db.hash_increment_by_float_async("h", "float", f64::INFINITY)
            .await
            .is_err()
    );
    db.hash_set("h", "int", &i64::MAX.to_string()).unwrap();
    assert!(db.hash_increment_by("h", "int", 1).is_err());
    assert!(db.hash_increment_by_async("h", "int", 1).await.is_err());

    assert_eq!(
        db.hash_persist_fields("h", &["a".to_string(), "missing".to_string()])
            .unwrap(),
        vec![-1, -2]
    );
    let future = now_ms() + 10_000;
    assert_eq!(
        db.hash_expire_fields_at_ms("h", future, &["a".to_string()], ExpireCondition::Xx)
            .unwrap(),
        vec![0]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms("h", future, &["a".to_string()], ExpireCondition::Lt)
            .unwrap(),
        vec![1]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms("h", future + 1_000, &["a".to_string()], ExpireCondition::Lt,)
            .unwrap(),
        vec![0]
    );
    assert!(
        db.hash_field_ttls("h", &["a".to_string()], false, false)
            .unwrap()[0]
            > 0
    );
    assert_eq!(
        db.hash_persist_fields("h", &["a".to_string()]).unwrap(),
        vec![1]
    );
    assert_eq!(
        db.hash_persist_fields("h", &["a".to_string()]).unwrap(),
        vec![-1]
    );

    db.insert_string_ref("plain", "value");
    assert!(db.hash_random_fields("plain", None, false).is_err());
    assert!(db.hash_increment_by_float("plain", "f", 1.0).is_err());
    assert!(db.hash_set_nx_async("plain", "f", "v").await.is_err());
    assert!(
        db.hash_multi_set_async("plain", &HashMap::new())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn hash_persist_fields_async_returns_all_redis_states() {
    let db = test_db();
    let expire_at = now_ms() + 10_000;

    assert!(
        db.hash_set_ex_async(
            "h",
            &[
                ("volatile".to_string(), "1".to_string()),
                ("persistent".to_string(), "2".to_string()),
            ],
            None,
            false,
            false,
            false,
        )
        .await
        .unwrap()
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "h",
            expire_at,
            &["volatile".to_string()],
            ExpireCondition::Always,
        )
        .await
        .unwrap(),
        vec![1]
    );

    assert_eq!(
        db.hash_persist_fields_async(
            "h",
            &[
                "volatile".to_string(),
                "persistent".to_string(),
                "missing".to_string(),
            ],
        )
        .await
        .unwrap(),
        vec![1, -1, -2]
    );
    assert_eq!(
        db.hash_persist_fields_async("h", &["volatile".to_string()])
            .await
            .unwrap(),
        vec![-1]
    );
    assert_eq!(
        db.hash_persist_fields_async("missing", &["field".to_string()])
            .await
            .unwrap(),
        vec![-2]
    );
}

#[tokio::test]
async fn hash_async_conditionals_ttls_and_concurrent_increments_cover_edges() {
    let db = test_db();

    assert_eq!(
        db.hash_set_many_async(
            "h",
            &[
                ("a".to_string(), "1".to_string()),
                ("a".to_string(), "2".to_string()),
                ("b".to_string(), "3".to_string()),
            ],
        )
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        db.hash_multi_get_async(
            "h",
            &["a".to_string(), "b".to_string(), "missing".to_string()],
        )
        .await
        .unwrap(),
        vec![Some("2".to_string()), Some("3".to_string()), None]
    );
    assert!(
        !db.hash_set_ex_async(
            "h",
            &[("a".to_string(), "blocked".to_string())],
            None,
            false,
            true,
            false,
        )
        .await
        .unwrap()
    );
    assert!(
        !db.hash_set_ex_async(
            "h",
            &[("missing".to_string(), "blocked".to_string())],
            None,
            false,
            false,
            true,
        )
        .await
        .unwrap()
    );
    assert!(
        db.hash_set_ex_async(
            "h",
            &[
                ("a".to_string(), "10".to_string()),
                ("c".to_string(), "30".to_string()),
            ],
            Some(StringExpireUpdate::RelativeMs(30_000)),
            false,
            false,
            false,
        )
        .await
        .unwrap()
    );
    assert!(
        db.hash_field_ttls_async("h", &["a".to_string(), "c".to_string()], false, true)
            .await
            .unwrap()
            .into_iter()
            .all(|ttl| ttl > 0)
    );
    assert!(
        db.hash_set_ex_async(
            "h",
            &[("a".to_string(), "11".to_string())],
            None,
            true,
            false,
            true,
        )
        .await
        .unwrap()
    );
    assert!(
        db.hash_field_ttls_async("h", &["a".to_string()], false, true)
            .await
            .unwrap()[0]
            > 0
    );
    assert!(
        db.hash_set_ex_async(
            "h",
            &[("a".to_string(), "12".to_string())],
            Some(StringExpireUpdate::Persist),
            false,
            false,
            true,
        )
        .await
        .unwrap()
    );
    assert_eq!(
        db.hash_field_ttls_async("h", &["a".to_string()], false, true)
            .await
            .unwrap(),
        vec![-1]
    );

    let future = now_ms().saturating_add(30_000);
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "h",
            future,
            &["a".to_string(), "missing".to_string()],
            ExpireCondition::Nx,
        )
        .await
        .unwrap(),
        vec![1, -2]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "h",
            future + 100,
            &["a".to_string()],
            ExpireCondition::Nx,
        )
        .await
        .unwrap(),
        vec![0]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "h",
            future + 100,
            &["a".to_string()],
            ExpireCondition::Xx,
        )
        .await
        .unwrap(),
        vec![1]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async("h", future, &["a".to_string()], ExpireCondition::Gt,)
            .await
            .unwrap(),
        vec![0]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "h",
            future + 200,
            &["a".to_string()],
            ExpireCondition::Gt,
        )
        .await
        .unwrap(),
        vec![1]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "h",
            future + 300,
            &["a".to_string()],
            ExpireCondition::Lt,
        )
        .await
        .unwrap(),
        vec![0]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "h",
            future + 50,
            &["a".to_string()],
            ExpireCondition::Lt,
        )
        .await
        .unwrap(),
        vec![1]
    );
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "h",
            now_ms().saturating_sub(1),
            &["b".to_string()],
            ExpireCondition::Always,
        )
        .await
        .unwrap(),
        vec![2]
    );
    assert_eq!(db.hash_get("h", "b").unwrap(), None);
    assert_eq!(
        db.hash_expire_fields_at_ms_async(
            "missing-hash",
            future,
            &["a".to_string(), "b".to_string()],
            ExpireCondition::Always,
        )
        .await
        .unwrap(),
        vec![-2, -2]
    );

    assert_eq!(
        db.hash_increment_by_async("numbers", "int", 5)
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        db.hash_increment_by_async("numbers", "int", -2)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        db.hash_increment_by_float_async("numbers", "float", 1.5)
            .await
            .unwrap(),
        "1.5"
    );
    db.hash_set("numbers", "bad-float", "nan").unwrap();
    assert!(
        db.hash_increment_by_float_async("numbers", "bad-float", 1.0)
            .await
            .is_err()
    );
    db.insert_string_ref("plain", "value");
    assert!(
        db.hash_set_many_async("plain", &[("f".to_string(), "v".to_string())])
            .await
            .is_err()
    );
    assert!(
        db.hash_expire_fields_at_ms_async(
            "plain",
            future,
            &["f".to_string()],
            ExpireCondition::Always,
        )
        .await
        .is_err()
    );

    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let db = Arc::clone(&db);
        tasks.push(tokio::spawn(async move {
            db.hash_increment_by_async("concurrent", "counter", 1)
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        db.hash_get("concurrent", "counter").unwrap(),
        Some("16".to_string())
    );
}
