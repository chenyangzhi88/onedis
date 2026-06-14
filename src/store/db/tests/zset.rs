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
            Some(4.0)
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
            vec![("bob".to_string(), 2.5), ("dave".to_string(), 4.0)]
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

