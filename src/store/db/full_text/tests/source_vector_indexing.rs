use super::super::*;
use super::support::*;

#[test]
fn json_paths_indexing_encoding_and_legacy_decode_are_covered() {
    let value = serde_json::json!({
        "items": [
            {"name": "book", "price": 10, "tags": ["a", true]},
            {"name": "pen", "price": "2.5"}
        ],
        "flag": false
    });
    let tokens = parse_fulltext_json_path("$.items[*].name").unwrap();
    assert_eq!(
        fulltext_json_path_values(&value, &tokens)
            .into_iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["book", "pen"]
    );
    assert_eq!(parse_fulltext_json_path("$").unwrap(), Vec::new());
    assert_eq!(parse_fulltext_json_path(".").unwrap(), Vec::new());
    assert!(parse_fulltext_json_path("$.").is_err());
    assert!(parse_fulltext_json_path("$[x]").is_err());
    assert!(parse_fulltext_json_path("$[*x]").is_err());

    assert_eq!(
        json_index_strings(&serde_json::json!(["x", 1, true, {"skip": 1}])),
        vec!["x", "1", "true"]
    );
    assert_eq!(
        json_index_tag_values(&serde_json::json!(["x", 1, false])),
        vec!["x", "1", "false"]
    );
    assert_eq!(
        json_index_numeric_values(&serde_json::json!(["2.5", 4, "bad", false])),
        vec!["2.5", "4"]
    );

    let schema = vec![
        text_field("title"),
        {
            let mut tag = field("tag", FullTextFieldKind::Tag);
            tag.options.alias = Some("t".to_string());
            tag
        },
        {
            let mut vector = field("vec", FullTextFieldKind::Vector);
            vector.options.vector = Some(vector_options());
            vector
        },
    ];
    let schema_frame = fulltext_schema_frame(&schema);
    let schema_text = schema_frame.to_string();
    assert!(schema_text.contains("title"));
    assert!(schema_text.contains("VECTOR"));
    assert!(schema_text.contains("HNSW"));

    let meta_record = meta(schema.clone());
    let encoded = encode_record(&meta_record).unwrap();
    let decoded = decode_fulltext_meta(&encoded).unwrap();
    assert_eq!(decoded.schema.len(), 3);

    let legacy = LegacyFullTextIndexMetaV1 {
        source_type: meta_record.source_type,
        prefixes: meta_record.prefixes.clone(),
        schema,
        aliases: meta_record.aliases.clone(),
        index_options: meta_record.index_options.clone(),
        state: meta_record.state,
        generation: 17,
        backfill_cursor: meta_record.backfill_cursor.clone(),
        last_indexed_outbox_seq: 23,
        refresh_policy: meta_record.refresh_policy.clone(),
    };
    let legacy_encoded = encode_record(&legacy).unwrap();
    let legacy_decoded = decode_fulltext_meta_for_index("legacy-index", &legacy_encoded).unwrap();
    assert_eq!(legacy_decoded.incarnation, 17);
    assert_eq!(legacy_decoded.generation, 17);
    assert_eq!(legacy_decoded.revision, 0);
    assert_eq!(legacy_decoded.active_storage, "legacy-index");
    assert_eq!(legacy_decoded.last_indexed_outbox_seq, 23);

    assert!(decode_fulltext_meta(b"not-bincode").is_err());
}
