use super::*;

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
        (TYPE_SET, 1),
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

    db.insert("test".to_string(), Structure::String("value".to_string()))
        .unwrap();

    assert!(matches!(
        db.get("test").unwrap(),
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
    db.insert_string("counter".to_string(), "100".to_string(), None)
        .unwrap();

    assert_eq!(db.increment_integer_string("counter", 1).unwrap(), 101);
    assert_eq!(db.get_string("counter").unwrap(), Some("101".to_string()));
}

#[test]
fn integer_increment_preserves_existing_ttl() {
    let db = test_db();

    db.insert_string("counter".to_string(), "1".to_string(), Some(10_000))
        .unwrap();

    assert_eq!(db.increment_integer_string("counter", 1).unwrap(), 2);
    assert!(db.ttl_millis_readonly("counter").unwrap() > 0);
    assert_eq!(db.get_string("counter").unwrap(), Some("2".to_string()));
}

#[test]
fn integer_increment_rejects_complex_type_after_overwrite() {
    let db = test_db();

    assert_eq!(db.increment_integer_string("counter", 1).unwrap(), 1);
    db.insert(
        "counter".to_string(),
        Structure::Set(HashSet::from(["member".to_string()])),
    )
    .unwrap();

    let err = db.increment_integer_string("counter", 1).unwrap_err();
    assert_eq!(err.to_string(), WRONG_TYPE_ERROR);
}

#[tokio::test]
async fn concurrent_set_nx_has_exactly_one_winner() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for index in 0..16 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            db.set_string_bytes_async(
                "set-nx-race".to_string(),
                format!("value-{index}").into_bytes(),
                SetExpiration::Clear,
                SetCondition::Nx,
                false,
            )
            .await
            .unwrap()
        }));
    }

    let mut winners = 0;
    for task in tasks {
        if matches!(task.await.unwrap(), SetOutcome::Set { .. }) {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
}

#[tokio::test]
async fn concurrent_msetnx_has_exactly_one_winner() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for index in 0..16 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            db.insert_string_bytes_many_nx_async(vec![
                (
                    "msetnx-shared".to_string(),
                    format!("value-{index}").into_bytes(),
                ),
                (format!("msetnx-side-{index}"), b"side".to_vec()),
            ])
            .await
        }));
    }

    let mut winners = 0;
    for task in tasks {
        winners += usize::from(task.await.unwrap().unwrap());
    }
    assert_eq!(winners, 1);
    assert_eq!(
        (0..16)
            .filter(|index| db.exists(&format!("msetnx-side-{index}")).unwrap())
            .count(),
        1
    );
}

#[tokio::test]
async fn concurrent_async_integer_updates_do_not_lose_writes() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            db.update_integer_string_async("integer-rmw-race", |current| current.checked_add(1))
                .await
                .unwrap()
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        db.get_string_async("integer-rmw-race").await.unwrap(),
        Some("32".to_string())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_async_bit_updates_do_not_lose_writes() {
    let db = Arc::new(test_db());
    let mut tasks = Vec::new();
    for offset in 0..32 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            db.string_set_bit_async("bitmap-race", offset, 1)
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), 0);
    }
    assert_eq!(
        db.get_string_bytes_async("bitmap-race").await.unwrap(),
        Some(vec![0xff; 4])
    );
}

#[tokio::test]
async fn ordered_string_pipeline_batches_preserve_per_command_replies_and_final_state() {
    let db = test_db();
    db.insert_string("batch".to_string(), "a".to_string(), Some(30_000))
        .unwrap();
    db.insert_string("delete-batch".to_string(), "gone".to_string(), Some(30_000))
        .unwrap();

    let replies = db
        .apply_string_batch_mutations_async(&[
            StringBatchMutation::Append {
                key: "batch",
                value: b"b",
            },
            StringBatchMutation::GetSet {
                key: "batch",
                value: b"c",
            },
            StringBatchMutation::SetNx {
                key: "batch",
                value: b"ignored",
            },
            StringBatchMutation::SetBit {
                key: "bitmap-batch",
                offset: 9,
                bit: 1,
            },
            StringBatchMutation::SetRange {
                key: "batch",
                offset: 1,
                value: b"z",
            },
            StringBatchMutation::Psetex {
                key: "batch",
                ttl_ms: 30_000,
                value: b"q",
            },
            StringBatchMutation::Append {
                key: "batch",
                value: b"r",
            },
            StringBatchMutation::GetDel {
                key: "delete-batch",
            },
            StringBatchMutation::GetDel {
                key: "delete-missing",
            },
            StringBatchMutation::SetRange {
                key: "missing-empty",
                offset: 100,
                value: b"",
            },
        ])
        .await;

    assert_eq!(replies.len(), 10);
    assert!(matches!(replies[0], Ok(StringBatchReply::Integer(2))));
    assert!(matches!(
        &replies[1],
        Ok(StringBatchReply::Bulk(Some(value))) if value == b"ab"
    ));
    assert!(matches!(replies[2], Ok(StringBatchReply::Integer(0))));
    assert!(matches!(replies[3], Ok(StringBatchReply::Integer(0))));
    assert!(matches!(replies[4], Ok(StringBatchReply::Integer(2))));
    assert!(matches!(replies[5], Ok(StringBatchReply::Ok)));
    assert!(matches!(replies[6], Ok(StringBatchReply::Integer(2))));
    assert!(matches!(
        &replies[7],
        Ok(StringBatchReply::Bulk(Some(value))) if value == b"gone"
    ));
    assert!(matches!(replies[8], Ok(StringBatchReply::Bulk(None))));
    assert!(matches!(replies[9], Ok(StringBatchReply::Integer(0))));
    assert_eq!(db.get_string("batch").unwrap().as_deref(), Some("qr"));
    assert!(db.ttl_millis_readonly("batch").unwrap() > 0);
    assert!(!db.exists_readonly("missing-empty").unwrap());
    assert!(!db.exists_readonly("delete-batch").unwrap());
    assert_eq!(db.string_get_bit("bitmap-batch", 9).unwrap(), 1);

    db.insert(
        "wrong-type".to_string(),
        Structure::Set(HashSet::from(["member".to_string()])),
    )
    .unwrap();
    let replies = db
        .apply_string_batch_mutations_async(&[
            StringBatchMutation::Append {
                key: "wrong-type",
                value: b"x",
            },
            StringBatchMutation::Psetex {
                key: "wrong-type",
                ttl_ms: 30_000,
                value: b"replaced",
            },
        ])
        .await;
    assert!(replies[0].is_err());
    assert!(matches!(replies[1], Ok(StringBatchReply::Ok)));
    assert_eq!(
        db.get_string("wrong-type").unwrap().as_deref(),
        Some("replaced")
    );

    let oversized_offset = crate::frame::MAX_BULK_STRING_BYTES;
    let replies = db
        .apply_string_batch_mutations_async(&[
            StringBatchMutation::SetRange {
                key: "rollback-after-error",
                offset: oversized_offset,
                value: b"x",
            },
            StringBatchMutation::SetNx {
                key: "rollback-after-error",
                value: b"winner",
            },
        ])
        .await;
    assert!(replies[0].is_err());
    assert!(matches!(replies[1], Ok(StringBatchReply::Integer(1))));
    assert_eq!(
        db.get_string("rollback-after-error").unwrap().as_deref(),
        Some("winner")
    );
}

#[tokio::test]
async fn ordered_key_expiration_batches_preserve_per_command_replies_and_ttl_index_state() {
    let db = test_db();
    db.insert_string("ttl-batch".to_string(), "value".to_string(), None)
        .unwrap();

    let replies = db
        .apply_key_expiration_batch_async(&[
            KeyExpirationBatchMutation::Persist { key: "ttl-batch" },
            KeyExpirationBatchMutation::Expire {
                key: "ttl-batch",
                ttl_ms: 30_000,
            },
            KeyExpirationBatchMutation::Persist { key: "ttl-batch" },
            KeyExpirationBatchMutation::Persist { key: "ttl-batch" },
            KeyExpirationBatchMutation::Expire {
                key: "missing-ttl-batch",
                ttl_ms: 30_000,
            },
        ])
        .await;

    assert_eq!(replies.len(), 5);
    assert!(matches!(replies[0], Ok(0)));
    assert!(matches!(replies[1], Ok(1)));
    assert!(matches!(replies[2], Ok(1)));
    assert!(matches!(replies[3], Ok(0)));
    assert!(matches!(replies[4], Ok(0)));
    assert_eq!(db.ttl_millis_readonly("ttl-batch").unwrap(), -1);

    let replies = db
        .apply_key_expiration_batch_async(&[
            KeyExpirationBatchMutation::Expire {
                key: "ttl-batch",
                ttl_ms: 30_000,
            },
            KeyExpirationBatchMutation::Expire {
                key: "ttl-batch",
                ttl_ms: 60_000,
            },
        ])
        .await;
    assert!(replies.into_iter().all(|reply| matches!(reply, Ok(1))));
    assert!(db.ttl_millis_readonly("ttl-batch").unwrap() > 50_000);
}

#[tokio::test]
async fn ordered_hll_pipeline_batch_preserves_replies_ttl_and_errors() {
    let db = test_db();
    db.insert_string("hll-batch".to_string(), "not-an-hll".to_string(), None)
        .unwrap();
    let invalid = db
        .hll_add_batch_async(&[("hll-batch", vec![b"a".as_slice()])])
        .await;
    assert!(invalid[0].is_err());

    db.delete_key("hll-batch").unwrap();
    let replies = db
        .hll_add_batch_async(&[
            ("hll-batch", vec![b"a".as_slice()]),
            ("hll-batch", vec![b"a".as_slice()]),
            ("hll-batch", vec![b"b".as_slice()]),
            ("other-hll", vec![b"x".as_slice(), b"y".as_slice()]),
        ])
        .await;
    assert!(matches!(replies[0], Ok(true)));
    assert!(matches!(replies[1], Ok(false)));
    assert!(matches!(replies[2], Ok(true)));
    assert!(matches!(replies[3], Ok(true)));
    assert_eq!(
        db.hll_count_async(&["hll-batch".to_string()])
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        db.hll_count_async(&["other-hll".to_string()])
            .await
            .unwrap(),
        2
    );

    db.expire("hll-batch".to_string(), 30_000).unwrap();
    let replies = db
        .hll_add_batch_async(&[("hll-batch", vec![b"c".as_slice()])])
        .await;
    assert!(replies[0].is_ok());
    assert!(db.ttl_millis_readonly("hll-batch").unwrap() > 0);
}

#[tokio::test]
async fn multi_key_delete_is_atomic_deduplicated_and_cleans_native_subkeys() {
    let db = test_db();
    db.insert_string("delete-string".to_string(), "value".to_string(), None)
        .unwrap();
    db.set_add_async("delete-set", &["a".to_string(), "b".to_string()])
        .await
        .unwrap();
    db.list_push_right_async("delete-list", &["a".to_string(), "b".to_string()], false)
        .await
        .unwrap();
    assert_eq!(
        db.delete_keys_async(&[
            "delete-string".to_string(),
            "delete-set".to_string(),
            "delete-set".to_string(),
            "missing".to_string(),
            "delete-list".to_string(),
        ])
        .await
        .unwrap(),
        3
    );
    assert!(!db.exists_readonly("delete-string").unwrap());
    assert!(!db.exists_readonly("delete-set").unwrap());
    assert!(!db.exists_readonly("delete-list").unwrap());
}

#[tokio::test]
async fn string_raw_async_bitmap_and_bitfield_paths_cover_edges() {
    let db = test_db();

    db.insert_string_bytes_refs_async(&[]).await.unwrap();
    db.insert_string_bytes_refs_async(&[("a", b"\x0f"), ("b", b"\xf0")])
        .await
        .unwrap();
    db.insert_string_bytes_refs_without_watch_publish_async(&[("c", b"plain")])
        .await
        .unwrap();
    db.insert_string_byte_keys_async(&[(b"raw-key".as_slice(), b"raw-value".as_slice())])
        .await
        .unwrap();
    db.insert_string_byte_keys_without_watch_publish_async(&[(b"raw-key-2".as_slice(), b"v2")])
        .await
        .unwrap();
    db.insert_string_bytes_many_async(vec![("d".to_string(), b"value".to_vec())])
        .await
        .unwrap();
    assert_eq!(db.get_string("c").unwrap(), Some("plain".to_string()));
    assert_eq!(
        db.get_string_bytes_async("raw-key").await.unwrap(),
        Some(b"raw-value".to_vec())
    );
    assert_eq!(
        db.get_string_bytes_async("raw-key-2").await.unwrap(),
        Some(b"v2".to_vec())
    );

    assert!(!db.insert_string_bytes_many_nx(Vec::new()).unwrap());
    assert!(
        db.insert_string_bytes_many_nx(vec![("nx-a".to_string(), b"1".to_vec())])
            .unwrap()
    );
    assert!(
        !db.insert_string_bytes_many_nx(vec![("nx-a".to_string(), b"2".to_vec())])
            .unwrap()
    );
    assert!(
        !db.insert_string_bytes_many_nx_async(Vec::new())
            .await
            .unwrap()
    );
    assert!(
        db.insert_string_bytes_many_nx_async(vec![("nx-b".to_string(), b"1".to_vec())])
            .await
            .unwrap()
    );
    assert!(
        !db.insert_string_bytes_many_nx_async(vec![("nx-b".to_string(), b"2".to_vec())])
            .await
            .unwrap()
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
    assert!(!db.exists_readonly("set-expired").unwrap());

    assert_eq!(db.string_get_bit("bits", 100).unwrap(), 0);
    assert_eq!(db.string_set_bit_async("bits", 3, 1).await.unwrap(), 0);
    assert_eq!(db.string_set_bit("bits", 3, 0).unwrap(), 1);
    assert!(db.string_set_bit("bits", 0, 2).is_err());
    db.insert_string("ttl-bits".to_string(), "x".to_string(), Some(30_000))
        .unwrap();
    db.string_set_bit("ttl-bits", 0, 1).unwrap();
    assert!(db.ttl_millis_readonly("ttl-bits").unwrap() > 0);
    db.string_write_bits_async("ttl-bits", 1, 3, 0b101)
        .await
        .unwrap();
    assert!(db.ttl_millis_readonly("ttl-bits").unwrap() > 0);
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
        -1
    );
    db.insert_string_bytes("range-bits".to_string(), vec![0xff, 0x01], None)
        .unwrap();
    assert_eq!(
        db.string_bitcount("range-bits", Some(0), Some(-99))
            .unwrap(),
        8
    );
    assert_eq!(
        db.string_bitcount_with_unit("range-bits", Some(4), Some(8), true)
            .unwrap(),
        4
    );
    assert_eq!(
        db.string_bitpos_with_unit("range-bits", 0, Some(4), Some(12), true)
            .unwrap(),
        8
    );
    assert_eq!(db.string_bitpos("missing-bits", 0, None, None).unwrap(), 0);
    db.insert_string_bytes("empty-bits".to_string(), Vec::new(), None)
        .unwrap();
    assert_eq!(db.string_bitpos("empty-bits", 0, None, None).unwrap(), -1);

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
    db.insert_string_bytes("empty-out".to_string(), b"stale".to_vec(), None)
        .unwrap();
    assert_eq!(
        db.string_bitop("OR", "empty-out", &["missing-a".to_string()])
            .unwrap(),
        0
    );
    assert!(!db.exists_readonly("empty-out").unwrap());
    db.insert_string_bytes("empty-out-async".to_string(), b"stale".to_vec(), None)
        .unwrap();
    assert_eq!(
        db.string_bitop_async("NOT", "empty-out-async", &["missing-b".to_string()])
            .await
            .unwrap(),
        0
    );
    assert!(!db.exists_readonly("empty-out-async").unwrap());

    db.insert_string_bytes("batch-a".to_string(), vec![0b1010_0000], None)
        .unwrap();
    db.insert_string_bytes("batch-b".to_string(), vec![0b1100_0000], None)
        .unwrap();
    let replies = db
        .string_bitop_batch_async(&[
            ("OR", "batch-dst", vec!["batch-a", "batch-b"]),
            ("XOR", "batch-next", vec!["batch-dst", "batch-a"]),
            ("NOT", "batch-dst", vec!["batch-next"]),
        ])
        .await;
    assert!(matches!(replies.as_slice(), [Ok(1), Ok(1), Ok(1)]));
    assert_eq!(
        db.get_string_bytes("batch-next").unwrap(),
        Some(vec![0b0100_0000])
    );
    assert_eq!(
        db.get_string_bytes("batch-dst").unwrap(),
        Some(vec![0b1011_1111])
    );

    let db = Arc::new(db);
    db.insert_string_bytes("hot-bitop".to_string(), vec![0], None)
        .unwrap();
    let mut bitop_tasks = Vec::new();
    for _ in 0..64 {
        let db = Arc::clone(&db);
        bitop_tasks.push(tokio::spawn(async move {
            db.string_bitop_async("NOT", "hot-bitop", &["hot-bitop".to_string()])
                .await
                .unwrap()
        }));
    }
    for task in bitop_tasks {
        assert_eq!(task.await.unwrap(), 1);
    }
    assert_eq!(db.get_string_bytes("hot-bitop").unwrap(), Some(vec![0]));
}
