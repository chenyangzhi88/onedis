    #[test]
    fn raw_key_namespace_helpers_cover_prefix_bounds_and_delete_batches() {
        assert!(db_prefix(7).is_empty());
        assert!(db_prefix_exclusive_upper_bound(7).is_none());
        assert!(db_prefix_exclusive_upper_bound(u16::MAX).is_none());
        assert_eq!(prefix_exclusive_upper_bound(b"ab").unwrap(), b"ac".to_vec());
        assert!(prefix_exclusive_upper_bound(&[0xff, 0xff]).is_none());
        assert_eq!(main_key(3, "key"), [b'k', b'e', b'y']);
        assert_eq!(main_key_bytes(3, b"key"), [b'k', b'e', b'y']);
        assert_eq!(decode_db_prefix(&[0x12, 0x34, b'k']), Some(0x1234));
        assert_eq!(decode_db_prefix(&[0x12]), None);

        let start = sub_key_range_start_bytes(1, &HASH_FIELD_NAMESPACE, b"k", 9);
        let mut expected_start = internal_prefix(1);
        expected_start.extend_from_slice(&HASH_FIELD_NAMESPACE);
        expected_start.extend_from_slice(b"k\0");
        expected_start.extend_from_slice(&9u64.to_be_bytes());
        assert_eq!(start, expected_start);
        let end = sub_key_range_end_bytes(1, &HASH_FIELD_NAMESPACE, b"k", 9);
        assert!(start < end);

        for namespace in [
            HASH_FIELD_NAMESPACE,
            LIST_ITEM_NAMESPACE,
            SET_MEMBER_NAMESPACE,
            SET_SLOT_NAMESPACE,
            SET_MEMBER_SLOT_NAMESPACE,
            ZSET_MEMBER_NAMESPACE,
            ZSET_RANK_NAMESPACE,
            STREAM_ENTRY_NAMESPACE,
            STREAM_GROUP_NAMESPACE,
            STREAM_PEL_NAMESPACE,
            STREAM_CONSUMER_NAMESPACE,
            JSON_NODE_NAMESPACE,
            VECTOR_META_NAMESPACE,
            VECTOR_DOC_NAMESPACE,
            VECTOR_TAG_NAMESPACE,
            VECTOR_NUMERIC_NAMESPACE,
            VECTOR_SEGMENT_NAMESPACE,
            VECTOR_GRAPH_NAMESPACE,
        ] {
            let mut rest = namespace.to_vec();
            rest.extend_from_slice(b"k");
            assert!(is_known_subkey_namespace(&rest));
        }
        assert!(!is_known_subkey_namespace(b"unknown"));

        let delete_counts = [
            (TYPE_HASH, 2),
            (TYPE_SET, 3),
            (TYPE_SORTED_SET, 2),
            (TYPE_LIST, 1),
            (TYPE_STREAM, 4),
            (TYPE_JSON, 2),
            (TYPE_VECTOR, 6),
        ];
        for (type_tag, min_count) in delete_counts {
            let mut batch = WriteBatch::new();
            delete_sub_keys_to_batch_bytes(&mut batch, 2, b"k", 11, type_tag);
            assert!(batch.count() >= min_count, "type tag {type_tag}");
        }

        let mut unknown = WriteBatch::new();
        delete_sub_keys_to_batch_bytes(&mut unknown, 2, b"k", 11, 0xff);
        assert_eq!(unknown.count(), 0);
    }

    #[test]
    fn get_returns_inserted_string_value() {
        let db = test_db();

        db.insert("test".to_string(), Structure::String("value".to_string()));

        assert!(matches!(
            db.get("test"),
            Some(Structure::String(value)) if value == "value"
        ));
    }

    #[test]
    fn integer_increment_uses_merge_and_returns_cached_value() {
        let db = test_db();

        assert_eq!(db.increment_integer_string("counter", 1).unwrap(), 1);
        assert_eq!(db.increment_integer_string("counter", 5).unwrap(), 6);
        assert_eq!(db.get_string("counter").unwrap(), Some("6".to_string()));
    }

    #[test]
    fn integer_increment_cache_is_invalidated_by_string_set() {
        let db = test_db();

        assert_eq!(db.increment_integer_string("counter", 1).unwrap(), 1);
        db.insert_string("counter".to_string(), "100".to_string(), None);

        assert_eq!(db.increment_integer_string("counter", 1).unwrap(), 101);
        assert_eq!(db.get_string("counter").unwrap(), Some("101".to_string()));
    }

    #[test]
    fn integer_increment_preserves_existing_ttl() {
        let db = test_db();

        db.insert_string("counter".to_string(), "1".to_string(), Some(10_000));

        assert_eq!(db.increment_integer_string("counter", 1).unwrap(), 2);
        assert!(db.ttl_millis_readonly("counter") > 0);
        assert_eq!(db.get_string("counter").unwrap(), Some("2".to_string()));
    }

    #[test]
    fn integer_increment_rejects_complex_type_after_overwrite() {
        let db = test_db();

        assert_eq!(db.increment_integer_string("counter", 1).unwrap(), 1);
        db.insert(
            "counter".to_string(),
            Structure::Set(HashSet::from(["member".to_string()])),
        );

        let err = db.increment_integer_string("counter", 1).unwrap_err();
        assert_eq!(err.to_string(), WRONG_TYPE_ERROR);
    }

    #[tokio::test]
    async fn string_raw_async_bitmap_and_bitfield_paths_cover_edges() {
        let db = test_db();

        db.insert_string_bytes_refs_async(&[]).await;
        db.insert_string_bytes_refs_async(&[("a", b"\x0f"), ("b", b"\xf0")])
            .await;
        db.insert_string_bytes_refs_without_watch_publish_async(&[("c", b"plain")])
            .await;
        db.insert_string_byte_keys_async(&[(b"raw-key".as_slice(), b"raw-value".as_slice())])
            .await;
        db.insert_string_byte_keys_without_watch_publish_async(&[(b"raw-key-2".as_slice(), b"v2")])
            .await;
        db.insert_string_bytes_many_async(vec![("d".to_string(), b"value".to_vec())])
            .await;
        assert_eq!(db.get_string("c").unwrap(), Some("plain".to_string()));
        assert_eq!(
            db.get_string_bytes_async("raw-key").await.unwrap(),
            Some(b"raw-value".to_vec())
        );
        assert_eq!(
            db.get_string_bytes_async("raw-key-2").await.unwrap(),
            Some(b"v2".to_vec())
        );

        assert!(db.insert_string_bytes_many_nx(Vec::new()) == false);
        assert!(db.insert_string_bytes_many_nx(vec![("nx-a".to_string(), b"1".to_vec())]));
        assert!(!db.insert_string_bytes_many_nx(vec![("nx-a".to_string(), b"2".to_vec())]));
        assert!(!db.insert_string_bytes_many_nx_async(Vec::new()).await);
        assert!(
            db.insert_string_bytes_many_nx_async(vec![("nx-b".to_string(), b"1".to_vec())])
                .await
        );
        assert!(
            !db.insert_string_bytes_many_nx_async(vec![("nx-b".to_string(), b"2".to_vec())])
                .await
        );

        assert_eq!(
            db.set_string_bytes(
                "set-old".to_string(),
                b"first".to_vec(),
                SetExpiration::Clear,
                SetCondition::Always,
                true,
            )
            .unwrap(),
            SetOutcome::Set { old_value: None }
        );
        assert_eq!(
            db.set_string_bytes(
                "set-old".to_string(),
                b"second".to_vec(),
                SetExpiration::KeepTtl,
                SetCondition::Xx,
                true,
            )
            .unwrap(),
            SetOutcome::Set {
                old_value: Some(b"first".to_vec())
            }
        );
        assert_eq!(
            db.set_string_bytes(
                "set-old".to_string(),
                b"third".to_vec(),
                SetExpiration::Clear,
                SetCondition::Nx,
                false,
            )
            .unwrap(),
            SetOutcome::NotSet
        );
        assert!(matches!(
            db.set_string_bytes_async(
                "set-expired".to_string(),
                b"gone".to_vec(),
                SetExpiration::At(now_ms().saturating_sub(1)),
                SetCondition::Always,
                false,
            )
            .await
            .unwrap(),
            SetOutcome::Set { .. }
        ));
        assert!(!db.exists_readonly("set-expired"));

        assert_eq!(db.string_get_bit("bits", 100).unwrap(), 0);
        assert_eq!(db.string_set_bit_async("bits", 3, 1).await.unwrap(), 0);
        assert_eq!(db.string_set_bit("bits", 3, 0).unwrap(), 1);
        assert!(db.string_set_bit("bits", 0, 2).is_err());
        db.string_write_bits("bits", 0, 8, 0b1010_0101).unwrap();
        assert_eq!(
            db.string_read_bits("bits", 0, 8, false).unwrap(),
            0b1010_0101
        );
        assert_eq!(db.string_read_bits("bits", 0, 4, true).unwrap(), -6);
        assert!(db.string_write_bits("bits", 0, 0, 0).is_err());
        assert!(
            db.string_read_bits_async("bits", 0, 64, false)
                .await
                .is_err()
        );
        db.string_write_bits_async("bits-async", 4, 4, 0b1111)
            .await
            .unwrap();
        assert_eq!(
            db.string_read_bits_async("bits-async", 4, 4, false)
                .await
                .unwrap(),
            15
        );

        assert_eq!(db.string_bitcount("bits", None, None).unwrap(), 4);
        assert_eq!(
            db.string_bitcount_async("bits", Some(0), Some(0))
                .await
                .unwrap(),
            4
        );
        assert_eq!(db.string_bitpos("bits", 1, None, None).unwrap(), 0);
        assert!(db.string_bitpos("bits", 2, None, None).is_err());
        assert_eq!(db.string_bitpos("bits", 1, Some(99), None).unwrap(), -1);
        assert_eq!(
            db.string_bitpos_async("bits", 0, Some(99), None)
                .await
                .unwrap(),
            8
        );

        assert_eq!(
            db.string_bitop("AND", "and-out", &["a".to_string(), "b".to_string()])
                .unwrap(),
            1
        );
        assert_eq!(db.get_string_bytes("and-out").unwrap(), Some(vec![0]));
        assert_eq!(
            db.string_bitop_async("OR", "or-out", &["a".to_string(), "b".to_string()])
                .await
                .unwrap(),
            1
        );
        assert_eq!(db.get_string_bytes("or-out").unwrap(), Some(vec![0xff]));
        assert_eq!(
            db.string_bitop("XOR", "xor-out", &["a".to_string(), "b".to_string()])
                .unwrap(),
            1
        );
        assert_eq!(db.get_string_bytes("xor-out").unwrap(), Some(vec![0xff]));
        assert_eq!(
            db.string_bitop_async("NOT", "not-out", &["a".to_string()])
                .await
                .unwrap(),
            1
        );
        assert!(
            db.string_bitop("NOT", "bad", &["a".to_string(), "b".to_string()])
                .is_err()
        );
        assert!(db.string_bitop("BAD", "bad", &["a".to_string()]).is_err());
    }

