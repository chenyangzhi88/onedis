use super::super::*;

#[test]
fn storage_key_helpers_round_trip_and_reject_bad_suffixes() {
    assert_eq!(
        fulltext_alias_from_key(3, &fulltext_alias_key(3, "alias")).unwrap(),
        "alias"
    );
    assert_eq!(
        fulltext_alias_from_key(4, &fulltext_alias_key(3, "alias")).unwrap(),
        "alias"
    );
    assert_eq!(
        fulltext_dict_term_from_key(1, "dict", &fulltext_dict_term_key(1, "dict", "term")).unwrap(),
        "term"
    );
    assert_eq!(
        fulltext_any_dict_term_from_key(1, &fulltext_dict_term_key(1, "dict", "term")).unwrap(),
        "term"
    );
    assert_eq!(
        fulltext_suggest_string_from_key(2, "sug", &fulltext_suggest_key(2, "sug", "value"))
            .unwrap(),
        "value"
    );
    assert_eq!(
        fulltext_syn_group_from_key(2, "idx", &fulltext_syn_key(2, "idx", "group")).unwrap(),
        "group"
    );
    assert_eq!(
        fulltext_index_from_meta_key(5, &fulltext_meta_key(5, "idx")).unwrap(),
        "idx"
    );
    let mut bad_meta_key = fulltext_meta_key(5, "idx");
    bad_meta_key.pop();
    assert!(fulltext_index_from_meta_key(5, &bad_meta_key).is_none());
    assert_eq!(
        fulltext_outbox_seq_from_key(7, "idx", &fulltext_outbox_key(7, "idx", 42)).unwrap(),
        42
    );
    assert_eq!(
        fulltext_index_and_seq_from_outbox_key(7, &fulltext_outbox_key(7, "idx", 42)),
        Some(("idx".to_string(), 42))
    );
    assert!(
        fulltext_outbox_latest_key(7, "idx").starts_with(&fulltext_meta_prefix(7)),
        "the durable latest-outbox watermark belongs to the index metadata namespace"
    );
    assert!(
        fulltext_index_from_meta_key(7, &fulltext_outbox_latest_key(7, "idx")).is_none(),
        "the watermark must not be mistaken for an index definition"
    );
    assert_eq!(
        fulltext_index_from_outbox_latest_key(7, &fulltext_outbox_latest_key(7, "idx")).as_deref(),
        Some("idx")
    );
    let mut bad_outbox = fulltext_outbox_key(7, "idx", 42);
    bad_outbox.push(0);
    assert!(fulltext_outbox_seq_from_key(7, "idx", &bad_outbox).is_none());
    assert!(fulltext_file_prefix(1, "idx").starts_with(&internal_prefix(1)));
    assert!(fulltext_repair_marker_key(1, "idx").starts_with(&fulltext_meta_prefix(1)));
    assert!(fulltext_config_key(1, "DEFAULT_DIALECT").starts_with(&fulltext_meta_prefix(1)));

    assert!(current_fulltext_millis() > 0);
}

#[test]
fn packed_mutation_records_round_trip_and_legacy_records_remain_readable() {
    let packed =
        encode_fulltext_mutation_batch(17, FullTextMutationKind::UpsertKey, &["doc:1", "doc:2"])
            .unwrap();
    let decoded = decode_fulltext_mutation_records(&packed).unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].incarnation, 17);
    assert_eq!(decoded[0].kind, FullTextMutationKind::UpsertKey);
    assert_eq!(decoded[0].key, "doc:1");
    assert_eq!(decoded[1].key, "doc:2");
    assert!(decoded.iter().all(|record| record.projection.is_none()));

    let projected = encode_fulltext_projected_mutation_batch(
        18,
        vec![FullTextProjectedMutation {
            key: "doc:projected".to_string(),
            projection: FullTextIndexedProjection {
                fields: vec![("title".to_string(), "hello".to_string())],
                expires_at_ms: 123,
            },
        }],
    )
    .unwrap();
    let projected = decode_fulltext_mutation_records(&projected).unwrap();
    assert_eq!(projected[0].incarnation, 18);
    assert_eq!(projected[0].key, "doc:projected");
    assert_eq!(projected[0].projection.as_ref().unwrap().expires_at_ms, 123);

    let legacy = encode_record(&FullTextMutationRecordV1 {
        incarnation: 9,
        kind: FullTextMutationKind::DeleteKey,
        key: "doc:old".to_string(),
    })
    .unwrap();
    let decoded = decode_fulltext_mutation_records(&legacy).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].key, "doc:old");
}
