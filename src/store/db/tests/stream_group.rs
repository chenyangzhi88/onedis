    #[tokio::test]
    async fn stream_group_pending_claim_autoclaim_and_async_paths_cover_edges() {
        let db = test_db();

        assert!(
            db.stream_group_create("missing", "g", StreamId { ms: 0, seq: 0 }, false)
                .is_err()
        );
        db.stream_group_create_async("mk", "g", StreamId { ms: 0, seq: 0 }, true)
            .await
            .unwrap();
        assert_eq!(db.stream_group_destroy_async("mk", "g").await.unwrap(), 1);

        let id1 = db
            .stream_add(
                "s",
                Some(StreamId { ms: 1, seq: 0 }),
                &[("f".to_string(), "v1".to_string())],
            )
            .unwrap();
        let id2 = db
            .stream_add(
                "s",
                Some(StreamId { ms: 2, seq: 0 }),
                &[("f".to_string(), "v2".to_string())],
            )
            .unwrap();
        assert!(
            db.stream_add(
                "s",
                Some(StreamId { ms: 0, seq: 0 }),
                &[("f".to_string(), "bad".to_string())]
            )
            .is_err()
        );
        assert!(
            db.stream_add("s", Some(id1), &[("f".to_string(), "dup".to_string())])
                .is_err()
        );

        db.stream_group_create("s", "g", StreamId { ms: 0, seq: 0 }, false)
            .unwrap();
        assert!(
            db.stream_group_create("s", "g", StreamId { ms: 0, seq: 0 }, false)
                .is_err()
        );
        assert_eq!(db.stream_group_create_consumer("s", "g", "c1").unwrap(), 1);
        assert_eq!(db.stream_group_create_consumer("s", "g", "c1").unwrap(), 0);

        let read = db
            .stream_read_group(
                "g",
                "c1",
                &[("s".to_string(), StreamReadGroupStart::New)],
                Some(1),
                false,
            )
            .unwrap();
        assert_eq!(read[0].1.len(), 1);
        let summary = db.stream_pending_summary("s", "g").unwrap();
        assert_eq!(summary.total, 1);
        assert_eq!(summary.smallest_id, Some(id1.to_redis_id()));
        assert_eq!(summary.consumers, vec![("c1".to_string(), 1)]);

        let pending = db
            .stream_pending_range(
                "s",
                "g",
                StreamId { ms: 0, seq: 0 },
                StreamId {
                    ms: u64::MAX,
                    seq: u64::MAX,
                },
                10,
                Some("c1"),
            )
            .unwrap();
        assert_eq!(pending.len(), 1);

        let claimed = db.stream_claim("s", "g", "c2", 0, &[id1]).unwrap();
        assert_eq!(claimed.len(), 1);
        let auto = db
            .stream_auto_claim("s", "g", "c3", 0, StreamId { ms: 0, seq: 0 }, 10)
            .unwrap();
        assert_eq!(auto.entries.len(), 1);
        assert_eq!(db.stream_ack("s", "g", &[id1, id2]).unwrap(), 1);

        let read_pending = db
            .stream_read_group(
                "g",
                "c3",
                &[(
                    "s".to_string(),
                    StreamReadGroupStart::Id(StreamId { ms: 0, seq: 0 }),
                )],
                Some(10),
                false,
            )
            .unwrap();
        assert!(read_pending.is_empty());

        let read_noack = db
            .stream_read_group_async(
                "g",
                "c4",
                &[("s".to_string(), StreamReadGroupStart::New)],
                Some(10),
                true,
            )
            .await
            .unwrap();
        assert_eq!(read_noack[0].1.len(), 1);
        assert_eq!(
            db.stream_pending_summary_async("s", "g")
                .await
                .unwrap()
                .total,
            0
        );
        assert_eq!(
            db.stream_pending_range_async(
                "s",
                "g",
                StreamId { ms: 0, seq: 0 },
                StreamId {
                    ms: u64::MAX,
                    seq: u64::MAX,
                },
                10,
                None,
            )
            .await
            .unwrap(),
            Vec::new()
        );

        let groups = db.stream_groups("s").unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, "g");
        assert!(db.stream_groups_async("s").await.unwrap()[0].consumers >= 1);
        assert_eq!(db.stream_group_delete_consumer("s", "g", "c1").unwrap(), 0);
        assert_eq!(
            db.stream_group_delete_consumer_async("s", "g", "c4")
                .await
                .unwrap(),
            0
        );
        assert_eq!(db.stream_group_destroy("s", "g").unwrap(), 1);
        assert_eq!(db.stream_group_destroy("s", "g").unwrap(), 0);
        assert!(db.stream_pending_summary("s", "g").is_err());
        assert!(db.stream_ack_async("s", "g", &[id1]).await.is_err());
        assert!(
            db.stream_claim_async("s", "g", "c", 0, &[id1])
                .await
                .is_err()
        );
        assert!(
            db.stream_auto_claim_async("s", "g", "c", 0, StreamId { ms: 0, seq: 0 }, 1)
                .await
                .is_err()
        );
    }

