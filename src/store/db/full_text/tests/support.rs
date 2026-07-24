use super::super::*;
use crate::store::kv_store::KvStore;

pub(super) fn test_store(label: &str) -> KvStore {
    let unique = format!(
        "onedis-fulltext-test-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target"))
        .join("onedis-test-data")
        .join(unique);
    let db_path = base.join("db");
    let wal_dir = base.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    KvStore::new(db_path, wal_dir, 1)
}

pub(super) fn text_field(name: &str) -> FullTextFieldSchema {
    FullTextFieldSchema {
        name: name.to_string(),
        kind: FullTextFieldKind::Text,
        options: FullTextFieldOptions::default(),
    }
}

pub(super) fn field(name: &str, kind: FullTextFieldKind) -> FullTextFieldSchema {
    FullTextFieldSchema {
        name: name.to_string(),
        kind,
        options: FullTextFieldOptions::default(),
    }
}

pub(super) fn vector_options() -> FullTextVectorOptions {
    FullTextVectorOptions {
        algorithm: FullTextVectorAlgorithm::Hnsw,
        attributes: vec![
            ("TYPE".to_string(), "FLOAT32".to_string()),
            ("DIM".to_string(), "3".to_string()),
            ("DISTANCE_METRIC".to_string(), "COSINE".to_string()),
            ("M".to_string(), "16".to_string()),
            ("EF_CONSTRUCTION".to_string(), "200".to_string()),
        ],
    }
}

pub(super) fn search_options() -> FullTextSearchOptions {
    FullTextSearchOptions {
        offset: 0,
        limit: 10,
        return_fields: None,
        no_content: false,
        with_scores: false,
        with_payloads: false,
        with_sort_keys: false,
        filters: Vec::new(),
        geo_filters: Vec::new(),
        in_keys: None,
        in_fields: None,
        sort_by: None,
        timeout_ms: None,
        slop: None,
        inorder: false,
        language: None,
        payload: None,
        scorer: FullTextScorer::Bm25Std,
        summarize: None,
        highlight: None,
        explain_score: false,
        params: HashMap::new(),
        dialect: 2,
        dialect_explicit: false,
    }
}

pub(super) fn meta(schema: Vec<FullTextFieldSchema>) -> FullTextIndexMeta {
    FullTextIndexMeta {
        revision: 1,
        source_type: FullTextSourceType::Hash,
        prefixes: vec!["doc:".to_string()],
        schema,
        aliases: Vec::new(),
        index_options: FullTextIndexOptions::default(),
        state: FullTextIndexState::Ready,
        incarnation: 1,
        generation: 1,
        active_storage: "idx".to_string(),
        backfill_cursor: None,
        last_indexed_outbox_seq: 0,
        indexed_docs: 0,
        indexed_bytes: 0,
        refresh_policy: FullTextRefreshPolicy::default(),
    }
}

pub(super) fn runtime_config() -> FullTextRuntimeConfig {
    FullTextRuntimeConfig {
        writer_heap_bytes: FULLTEXT_WRITER_HEAP_BYTES,
        min_prefix: 2,
        max_expansions: 200,
        max_prefix_expansions: 200,
    }
}

pub(super) fn search_deadline() -> FullTextSearchDeadline {
    FullTextSearchDeadline {
        at: Instant::now() + Duration::from_secs(60),
        fail_on_timeout: true,
    }
}

pub(super) fn row(key: &str, score: f64, fields: &[(&str, &str)]) -> FullTextAggregateRow {
    let hit = FullTextLiveHit {
        key: key.to_string(),
        score: score as f32,
        fields: fields
            .iter()
            .map(|(field, value)| ((*field).to_string(), (*value).to_string()))
            .collect(),
        sort_key: None,
        payload: None,
    };
    fulltext_aggregate_row_from_hit(hit, None).unwrap()
}

pub(super) fn number_value(value: &FullTextAggregateValue) -> f64 {
    match value {
        FullTextAggregateValue::Number(value) => *value,
        _ => panic!("expected numeric aggregate value"),
    }
}

pub(super) fn string_value(value: &FullTextAggregateValue) -> &str {
    match value {
        FullTextAggregateValue::String(value) => value,
        _ => panic!("expected string aggregate value"),
    }
}
