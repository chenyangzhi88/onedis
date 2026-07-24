use super::*;

#[tokio::test]
async fn async_batch_string_overwrite_removes_old_collection_subkeys() {
    let db = test_db();
    db.hash_set_async("batch-overwrite", "field", "value")
        .await
        .unwrap();
    let raw = db.store.get_raw(&db.mk("batch-overwrite")).unwrap();
    let version = decode_meta_header(&raw).unwrap().version;
    let prefix = hash_field_prefix(db.db_index, "batch-overwrite", version);
    assert_eq!(db.store.scan_prefix_raw_async(&prefix).await.len(), 1);

    db.insert_string_bytes_many_async(vec![("batch-overwrite".to_string(), b"plain".to_vec())])
        .await;

    assert_eq!(
        db.get_string_bytes_async("batch-overwrite").await.unwrap(),
        Some(b"plain".to_vec())
    );
    assert!(db.store.scan_prefix_raw_async(&prefix).await.is_empty());
}

#[tokio::test]
async fn conditional_string_batch_is_atomic_and_preserves_requested_ttl() {
    let db = test_db();
    db.insert_string_bytes("existing".to_string(), b"old".to_vec(), Some(60_000));
    let old_ttl = db.ttl_millis("existing");

    assert!(
        !db.set_string_bytes_many_async(
            vec![
                ("missing".to_string(), b"new".to_vec()),
                ("existing".to_string(), b"changed".to_vec()),
            ],
            SetExpiration::Clear,
            SetCondition::Nx,
        )
        .await
        .unwrap()
    );
    assert_eq!(db.get_string("missing").unwrap(), None);
    assert_eq!(db.get_string("existing").unwrap().as_deref(), Some("old"));

    assert!(
        db.set_string_bytes_many_async(
            vec![("existing".to_string(), b"changed".to_vec())],
            SetExpiration::KeepTtl,
            SetCondition::Xx,
        )
        .await
        .unwrap()
    );
    assert_eq!(
        db.get_string("existing").unwrap().as_deref(),
        Some("changed")
    );
    assert!(db.ttl_millis("existing") > 0);
    assert!(db.ttl_millis("existing") <= old_ttl);
}

#[test]
fn stream_add_len_and_range_use_ordered_ids() {
    let db = test_db();
    let first = db
        .stream_add(
            "events",
            Some(StreamId { ms: 1, seq: 0 }),
            &[("type".to_string(), "created".to_string())],
        )
        .unwrap();
    let second = db
        .stream_add(
            "events",
            Some(StreamId { ms: 2, seq: 0 }),
            &[
                ("type".to_string(), "updated".to_string()),
                ("user".to_string(), "alice".to_string()),
            ],
        )
        .unwrap();

    assert_eq!(first.to_redis_id(), "1-0");
    assert_eq!(second.to_redis_id(), "2-0");
    assert_eq!(db.stream_len("events").unwrap(), 2);

    let entries = db
        .stream_range(
            "events",
            Some(StreamId { ms: 1, seq: 0 }),
            Some(StreamId { ms: 2, seq: 0 }),
            None,
            false,
        )
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].id, "1-0");
    assert_eq!(entries[1].id, "2-0");
    assert_eq!(
        entries[1].fields,
        vec![
            ("type".to_string(), "updated".to_string()),
            ("user".to_string(), "alice".to_string())
        ]
    );
}

#[test]
fn stream_reverse_range_and_xread_semantics() {
    let db = test_db();
    for seq in 0..3 {
        db.stream_add(
            "events",
            Some(StreamId { ms: 10, seq }),
            &[("v".to_string(), seq.to_string())],
        )
        .unwrap();
    }

    let reversed = db
        .stream_range(
            "events",
            Some(StreamId { ms: 10, seq: 0 }),
            Some(StreamId { ms: 10, seq: 2 }),
            Some(2),
            true,
        )
        .unwrap();
    assert_eq!(
        reversed
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        vec!["10-2", "10-1"]
    );

    let read = db
        .stream_read(
            &[(
                "events".to_string(),
                StreamReadStart::Id(StreamId { ms: 10, seq: 0 }),
            )],
            Some(10),
        )
        .unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(
        read[0]
            .1
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        vec!["10-1", "10-2"]
    );

    let latest = db
        .stream_read(&[("events".to_string(), StreamReadStart::Latest)], Some(10))
        .unwrap();
    assert!(latest.is_empty());
}

#[test]
fn stream_rejects_wrong_type_and_duplicate_ids() {
    let db = test_db();
    db.insert("plain".to_string(), Structure::String("value".to_string()));
    assert!(db.stream_len("plain").is_err());
    assert!(
        db.stream_add(
            "plain",
            Some(StreamId { ms: 1, seq: 0 }),
            &[("f".to_string(), "v".to_string())],
        )
        .is_err()
    );

    db.stream_add(
        "events",
        Some(StreamId { ms: 1, seq: 0 }),
        &[("f".to_string(), "v".to_string())],
    )
    .unwrap();
    assert!(
        db.stream_add(
            "events",
            Some(StreamId { ms: 1, seq: 0 }),
            &[("f".to_string(), "v2".to_string())],
        )
        .is_err()
    );
}

#[test]
fn stream_delete_removes_entry_namespace() {
    let db = test_db();
    db.stream_add(
        "events",
        Some(StreamId { ms: 1, seq: 0 }),
        &[("f".to_string(), "v".to_string())],
    )
    .unwrap();
    assert!(matches!(db.get("events"), Some(Structure::Stream(_))));

    db.remove("events");
    assert_eq!(db.stream_len("events").unwrap(), 0);
    assert!(
        db.stream_range("events", None, None, None, false)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn stream_delete_counts_duplicate_ids_once() {
    let db = test_db();
    let id = StreamId { ms: 1, seq: 0 };
    db.stream_add("events", Some(id), &[("f".to_string(), "v".to_string())])
        .unwrap();

    assert_eq!(db.stream_delete("events", &[id, id]).unwrap(), 1);
    assert_eq!(db.stream_len("events").unwrap(), 0);
}

#[tokio::test]
async fn string_batch_async_helpers_cover_empty_versioned_and_byte_key_paths() {
    let db = test_db();

    db.insert_string_bytes_refs_async(&[]).await;
    db.insert_string_bytes_refs_without_watch_publish_async(&[])
        .await;
    db.insert_string_byte_keys_async(&[]).await;
    db.insert_string_byte_keys_without_watch_publish_async(&[])
        .await;

    db.insert_string_bytes_refs_async(&[("a", b"1"), ("b", b"2")])
        .await;
    assert_eq!(db.get_string("a").unwrap(), Some("1".to_string()));
    assert_eq!(db.get_string("b").unwrap(), Some("2".to_string()));

    db.insert_string_bytes_refs_without_watch_publish_async(&[("a", b"updated")])
        .await;
    assert_eq!(db.get_string("a").unwrap(), Some("updated".to_string()));

    db.insert_string_byte_keys_async(&[(b"raw:a".as_slice(), b"ra".as_slice())])
        .await;
    assert_eq!(db.get_string("raw:a").unwrap(), Some("ra".to_string()));

    db.insert_string_byte_keys_without_watch_publish_async(&[(
        b"raw:a".as_slice(),
        b"rb".as_slice(),
    )])
    .await;
    assert_eq!(db.get_string("raw:a").unwrap(), Some("rb".to_string()));

    let _ = db.next_persisted_version();
    db.insert_string_bytes_refs_async(&[("a", b"v1"), ("c", b"3")])
        .await;
    assert_eq!(db.get_string("a").unwrap(), Some("v1".to_string()));
    assert_eq!(db.get_string("c").unwrap(), Some("3".to_string()));

    db.insert_string_bytes_refs_without_watch_publish_async(&[("c", b"4")])
        .await;
    assert_eq!(db.get_string("c").unwrap(), Some("4".to_string()));

    db.insert_string_byte_keys_async(&[(b"raw:b".as_slice(), b"1".as_slice())])
        .await;
    db.insert_string_byte_keys_without_watch_publish_async(&[(
        b"raw:b".as_slice(),
        b"2".as_slice(),
    )])
    .await;
    assert_eq!(db.get_string("raw:b").unwrap(), Some("2".to_string()));
}

#[tokio::test]
async fn borrowed_string_batch_publishes_watch_mutations() {
    let db = test_db();
    let (key_version, db_version) = db.watch_version_snapshot("watched-fast-set");

    db.insert_string_bytes_refs_async(&[("watched-fast-set", b"value")])
        .await;

    assert!(db.watch_version_changed("watched-fast-set", key_version, db_version));
}

#[tokio::test]
async fn stream_async_missing_mkstream_trim_delete_and_group_edges() {
    let db = test_db();

    assert_eq!(
        db.stream_delete_async("missing-stream", &[StreamId { ms: 1, seq: 0 }])
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        db.stream_trim_maxlen_async("missing-stream", 1)
            .await
            .unwrap(),
        0
    );
    assert!(
        db.stream_set_id_async("missing-stream", StreamId { ms: 1, seq: 0 })
            .await
            .is_err()
    );
    assert!(
        db.stream_group_create_async("missing-stream", "g", StreamId { ms: 0, seq: 0 }, false,)
            .await
            .is_err()
    );

    db.stream_group_create_async(
        "created-stream",
        "g",
        StreamId {
            ms: u64::MAX,
            seq: u64::MAX,
        },
        true,
    )
    .await
    .unwrap();
    assert!(
        db.stream_group_create_async("created-stream", "g", StreamId { ms: 0, seq: 0 }, false,)
            .await
            .is_err()
    );
    db.stream_group_set_id_async("created-stream", "g", StreamId { ms: 2, seq: 0 })
        .await
        .unwrap();
    assert!(
        db.stream_group_set_id_async("created-stream", "missing", StreamId { ms: 2, seq: 0 })
            .await
            .is_err()
    );

    db.stream_add_async(
        "events",
        Some(StreamId { ms: 1, seq: 0 }),
        &[("f".to_string(), "v1".to_string())],
    )
    .await
    .unwrap();
    db.stream_add_async(
        "events",
        Some(StreamId { ms: 2, seq: 0 }),
        &[("f".to_string(), "v2".to_string())],
    )
    .await
    .unwrap();
    assert_eq!(db.stream_trim_maxlen_async("events", 5).await.unwrap(), 0);
    assert_eq!(db.stream_trim_maxlen_async("events", 1).await.unwrap(), 1);
    assert_eq!(db.stream_len_async("events").await.unwrap(), 1);
    assert_eq!(
        db.stream_delete_async("events", &[StreamId { ms: 2, seq: 0 }])
            .await
            .unwrap(),
        1
    );
    assert_eq!(db.stream_len_async("events").await.unwrap(), 0);
}
