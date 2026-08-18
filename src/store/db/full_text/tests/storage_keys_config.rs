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
