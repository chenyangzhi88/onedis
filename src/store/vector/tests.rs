use super::*;
use crate::store::{
    db::KEY_ENCODING_LAYOUT_META_KEY,
    kv_store::KvStore,
    ttl::{TtlConfig, TtlManager, VersionCounter},
};

fn integration_test_db(prefix: &str, layout: KeyEncodingLayout) -> Db {
    let unique = format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let root = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("target"))
        .join("onedis-test-data")
        .join(unique);
    let db_path = root.join("db");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&db_path).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let store = KvStore::new(db_path, wal_dir, 1);
    store
        .for_db_index(0)
        .put_raw(KEY_ENCODING_LAYOUT_META_KEY, layout.encode());
    let version_counter = Arc::new(VersionCounter::new());
    let ttl_manager = TtlManager::new(store.clone(), TtlConfig::default());
    Db::new(0, store, version_counter, ttl_manager)
}

fn create_options(distance: &str, segment_max_docs: Option<u64>) -> VectorCreateOptions {
    VectorCreateOptions {
        dim: 2,
        source_dim: None,
        distance: distance.to_string(),
        schema: schema(),
        segment_max_docs,
        m: Some(4),
        ef_construction: Some(8),
        ef_runtime: Some(8),
        initial_cap: Some(8),
        quantization: VectorQuantization::F32,
    }
}

fn search_options(k: usize) -> VectorSearchOptions {
    VectorSearchOptions {
        k,
        filter: None,
        with_scores: true,
        with_attrs: Vec::new(),
        with_attrs_json: false,
        ef: None,
        filter_ef: None,
        exact: false,
        offset: 0,
        limit: None,
    }
}

fn schema() -> Vec<VectorFieldSchema> {
    vec![
        VectorFieldSchema {
            name: "brand".to_string(),
            kind: VectorFieldKind::Tag,
            indexed: true,
        },
        VectorFieldSchema {
            name: "price".to_string(),
            kind: VectorFieldKind::Numeric,
            indexed: true,
        },
        VectorFieldSchema {
            name: "title".to_string(),
            kind: VectorFieldKind::Text,
            indexed: false,
        },
    ]
}

fn meta(distance: VectorDistance) -> VectorIndexMeta {
    VectorIndexMeta {
        dim: 2,
        projection: None,
        distance,
        schema: schema(),
        m: 4,
        ef_construction: 8,
        ef_runtime: 8,
        initial_cap: 8,
        next_doc_version: 1,
        doc_count: 0,
        next_segment_id: 1,
        snapshot_doc_version: 0,
        segment_max_docs: 2,
        max_segment_docs: 128,
        quantization: VectorQuantization::F32,
        internal: false,
        algorithm: VectorIndexAlgorithm::Hnsw,
    }
}

#[test]
fn vector_validation_filters_attrs_and_distance_helpers_cover_edges() {
    assert_eq!(parse_distance("cosine").unwrap(), VectorDistance::Cosine);
    assert_eq!(parse_distance("L2").unwrap(), VectorDistance::L2);
    assert_eq!(parse_distance("ip").unwrap(), VectorDistance::Ip);
    assert!(parse_distance("bad").is_err());
    assert_eq!(distance_name(VectorDistance::Cosine), "COSINE");
    assert_eq!(distance_name(VectorDistance::L2), "L2");
    assert_eq!(distance_name(VectorDistance::Ip), "IP");
    assert_eq!(normalize_hnsw_m(None).unwrap(), DEFAULT_HNSW_M as usize);
    assert!(normalize_hnsw_m(Some(0)).is_err());
    assert!(normalize_hnsw_m(Some(257)).is_err());

    assert!(validate_schema(&schema()).is_ok());
    assert!(
        validate_schema(&[VectorFieldSchema {
            name: String::new(),
            kind: VectorFieldKind::Tag,
            indexed: true,
        }])
        .is_err()
    );
    assert!(
        validate_schema(&[
            VectorFieldSchema {
                name: "dup".to_string(),
                kind: VectorFieldKind::Tag,
                indexed: true,
            },
            VectorFieldSchema {
                name: "dup".to_string(),
                kind: VectorFieldKind::Numeric,
                indexed: false,
            },
        ])
        .is_err()
    );
    assert!(validate_vector(&[1.0, 2.0], 2).is_ok());
    assert!(validate_vector(&[1.0], 2).is_err());
    assert!(validate_vector(&[f32::NAN, 1.0], 2).is_err());
    assert!(validate_vector_for_distance(&[0.0, 0.0], VectorDistance::Cosine).is_err());
    assert!(validate_vector_for_distance(&[0.0, 0.0], VectorDistance::L2).is_ok());

    let attrs = parse_attrs(r#"{"brand":["acme","budget"],"price":12.5,"title":"lamp"}"#).unwrap();
    validate_attrs_against_schema(&schema(), &attrs).unwrap();
    assert!(parse_attrs("[]").is_err());
    assert!(parse_attrs("{bad").is_err());
    assert!(
        validate_attrs_against_schema(&schema(), &serde_json::json!({"brand":[1],"price":1}))
            .is_ok()
    );
    assert!(
        validate_attrs_against_schema(
            &schema(),
            &serde_json::json!({"brand":"acme","price":"bad"})
        )
        .is_err()
    );
    assert_eq!(
        tag_values(&serde_json::json!(["a", "b"])).unwrap(),
        vec!["a".to_string(), "b".to_string()]
    );
    assert!(tag_values(&serde_json::json!([1])).is_err());
    assert!(tag_values(&serde_json::json!(1)).is_err());

    let predicates =
        parse_filter(".brand IN ('acme',\"budget\") AND price >= 10 && price < 20").unwrap();
    assert!(matches_filters(&attrs, &predicates));
    assert!(!matches_filters(
        &attrs,
        &parse_filter("brand == other").unwrap()
    ));
    assert!(matches_filters(
        &attrs,
        &parse_filter(r#".brand in ["acme","other"] and .price == 12.5"#).unwrap()
    ));
    assert!(matches_filters(
        &serde_json::json!({"enabled": true}),
        &parse_filter(".enabled == true").unwrap()
    ));
    assert!(matches_filters(
        &attrs,
        &parse_filter(".brand != other && .price != 10").unwrap()
    ));
    assert!(parse_filter("brand IN ()").is_err());
    assert!(parse_filter("brand IN acme").is_err());
    assert!(parse_filter("price >= nope").is_err());
    assert!(parse_filter("unsupported").is_err());
    assert_eq!(normalize_filter_field(" .brand "), "brand");
    assert_eq!(trim_filter_string("'acme'"), "acme");
    assert_eq!(
        collect_return_attrs(&attrs, &["brand".to_string(), "price".to_string()]),
        vec![
            ("brand".to_string(), r#"["acme","budget"]"#.to_string()),
            ("price".to_string(), "12.5".to_string())
        ]
    );

    assert_eq!(
        distance_score(VectorDistance::L2, &[1.0, 2.0], &[2.0, 4.0]).unwrap(),
        5.0
    );
    assert_eq!(
        distance_score(VectorDistance::Ip, &[1.0, 2.0], &[2.0, 4.0]).unwrap(),
        -10.0
    );
    assert!(distance_score(VectorDistance::Cosine, &[0.0, 0.0], &[1.0, 0.0]).is_err());
}

#[test]
fn vector_doc_result_window_reduce_and_binary_helpers_cover_edges() {
    let meta = meta(VectorDistance::L2);
    let doc = VectorDocRecord {
        id: "doc1".to_string(),
        doc_version: 7,
        vector: vec![1.0, 1.0],
        attrs_json: r#"{"brand":"acme","price":9}"#.to_string(),
        deleted: false,
    };
    let result = runtime_doc_to_search_result(
        "doc1",
        &doc,
        &meta,
        &[1.0, 2.0],
        5.0,
        None,
        &["brand".to_string()],
        true,
        &parse_filter("brand == acme").unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(result.id, "doc1");
    assert_eq!(
        result.attrs_json.as_deref(),
        Some(r#"{"brand":"acme","price":9}"#)
    );
    assert_eq!(
        result.attrs,
        vec![("brand".to_string(), "acme".to_string())]
    );
    assert!(
        runtime_doc_to_search_result(
            "doc1",
            &doc,
            &meta,
            &[1.0, 2.0],
            5.0,
            None,
            &[],
            false,
            &parse_filter("brand == other").unwrap(),
        )
        .unwrap()
        .is_none()
    );
    assert!(decode_record::<VectorDocRecord>(b"bad").is_err());

    let mut results = vec![
        VectorSearchResult {
            id: "b".to_string(),
            score: 0.1,
            attrs: Vec::new(),
            attrs_json: None,
        },
        VectorSearchResult {
            id: "a".to_string(),
            score: 0.1,
            attrs: Vec::new(),
            attrs_json: None,
        },
        VectorSearchResult {
            id: "c".to_string(),
            score: 0.2,
            attrs: Vec::new(),
            attrs_json: None,
        },
    ];
    sort_and_limit_results(&mut results, 2);
    assert_eq!(
        results
            .iter()
            .map(|result| result.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    let windowed = window_results(
        results,
        &VectorSearchOptions {
            k: 10,
            filter: None,
            with_scores: false,
            with_attrs: Vec::new(),
            with_attrs_json: false,
            ef: None,
            filter_ef: None,
            exact: false,
            offset: 1,
            limit: Some(1),
        },
    );
    assert_eq!(windowed.len(), 1);
    assert_eq!(windowed[0].id, "b");

    let reduced = reduce_vector_candidates(
        vec![
            VectorCandidate {
                id: "a".to_string(),
                doc_version: 1,
                distance: 0.2,
            },
            VectorCandidate {
                id: "a".to_string(),
                doc_version: 1,
                distance: 0.1,
            },
            VectorCandidate {
                id: "a".to_string(),
                doc_version: 2,
                distance: 0.1,
            },
        ],
        10,
    )
    .unwrap();
    assert_eq!(reduced.len(), 1);
    assert_eq!(reduced[0].doc_version, 2);
    assert_eq!(sortable_f64(-1.0), sortable_f64(-1.0));
    assert_eq!(unsortable_f64(sortable_f64(12.5)), 12.5);
}

#[test]
fn hnsw_graph_persisted_topology_and_registry_paths_cover_edges() {
    let mut graph = HnswGraph::new(2, VectorDistance::L2, 0, 1, 0, VectorQuantization::F32);
    assert_eq!(graph.m, 1);
    assert_eq!(graph.ef_construction, 1);
    assert!(graph.search(&[1.0, 1.0], 0, 1, None).unwrap().is_empty());
    graph.upsert("a".to_string(), 1, vec![0.0, 0.0]).unwrap();
    graph.upsert("b".to_string(), 2, vec![1.0, 1.0]).unwrap();
    graph.upsert("a".to_string(), 3, vec![0.5, 0.5]).unwrap();
    assert_eq!(graph.len(), 2);
    assert_eq!(graph.max_doc_version(), 3);
    let allow = HashSet::from(["b".to_string()]);
    let filtered = graph.search(&[1.0, 1.0], 10, 1, Some(&allow)).unwrap();
    assert!(filtered.iter().all(|candidate| candidate.id == "b"));
    graph.mark_deleted("b");
    assert_eq!(graph.len(), 1);
    assert!(graph.upsert("bad".to_string(), 4, vec![1.0]).is_err());

    let persisted = graph.to_persisted_index().unwrap();
    persisted.validate().unwrap();
    assert_eq!(persisted.node_count(), 1);
    let current_versions = DashMap::from_iter([("a".to_string(), 3)]);
    assert_eq!(
        persisted
            .search(&[0.5, 0.5], 1, 8, None, &current_versions)
            .unwrap()[0]
            .id,
        "a"
    );
    let mut invalid = persisted.clone();
    invalid.entry_point = u32::MAX;
    assert!(invalid.validate().is_err());
    let legacy = LegacyVectorHnswIndexBlobV1 {
        dim: 2,
        distance: VectorDistance::L2,
        m: 4,
        ef_construction: 8,
        quantization: VectorQuantization::F32,
        entry_point: 0,
        max_layer: 0,
        nodes: vec![LegacyVectorHnswIndexNodeV1 {
            id: "legacy".to_string(),
            doc_version: 1,
            vector: HnswSnapshotVector::F32(vec![0.0, 0.0]),
            layers: vec![Vec::new()],
        }],
    };
    let decoded_legacy = decode_vector_hnsw_index(&encode_record(&legacy).unwrap()).unwrap();
    decoded_legacy.validate().unwrap();
    assert_eq!(decoded_legacy.ids, vec!["legacy"]);

    let mut runtime =
        VectorRuntime::new(2, VectorDistance::L2, 4, 8, 2, 10, VectorQuantization::F32);
    assert!(runtime.memtable_batch(2, false).is_none());
    runtime.upsert("r1".to_string(), 1, vec![1.0, 0.0]).unwrap();
    runtime.upsert("r2".to_string(), 2, vec![0.0, 1.0]).unwrap();
    let source = Arc::new(VectorSegmentBlob {
        entries: runtime
            .memtable_batch(2, false)
            .unwrap()
            .iter()
            .map(VectorSegmentEntry::from)
            .collect(),
    });
    let mut persisted_graph = HnswGraph::new(
        2,
        VectorDistance::L2,
        4,
        8,
        2,
        VectorQuantization::F32,
    );
    for doc in &source.entries {
        persisted_graph
            .upsert(doc.id.clone(), doc.doc_version, doc.vector.clone())
            .unwrap();
    }
    let persisted_index = Arc::new(persisted_graph.to_persisted_index().unwrap());
    let segment = VectorSegmentMeta {
        segment_id: 10,
        level: 0,
        source_key: b"source-key".to_vec(),
        index_key: b"index-key".to_vec(),
        doc_count: 2,
        min_doc_version: 1,
        max_doc_version: 2,
    };
    runtime.publish_segment(
        segment.clone(),
        Arc::clone(&source),
        Some(Arc::clone(&persisted_index)),
    );
    assert_eq!(segment.segment_id, 10);
    assert_eq!(segment.doc_count, 2);
    assert_eq!(runtime.next_segment_id, 11);
    assert_eq!(runtime.segments[0].meta.index_key, b"index-key".to_vec());
    runtime.upsert("r1".to_string(), 3, vec![0.9, 0.1]).unwrap();
    assert_eq!(runtime.len(), 2);
    runtime.upsert("r3".to_string(), 3, vec![0.2, 0.2]).unwrap();
    assert_eq!(runtime.len(), 3);
    assert!(!runtime.search(&[1.0, 0.0], 3, 2, None).unwrap().is_empty());
    runtime.mark_deleted(VectorDocRecord {
        id: "r1".to_string(),
        doc_version: 4,
        vector: vec![0.9, 0.1],
        attrs_json: "{}".to_string(),
        deleted: true,
    });
    let allow = HashSet::from(["r2".to_string()]);
    assert!(
        runtime
            .search(&[0.0, 1.0], 3, 2, Some(&allow))
            .unwrap()
            .iter()
            .all(|candidate| candidate.id == "r2")
    );
    runtime.remove_segments(&HashSet::from([10]));
    assert!(runtime.segments.is_empty());

    let segmented = VectorRuntime::with_segments(
        2,
        VectorDistance::L2,
        4,
        8,
        2,
        20,
        vec![VectorSegmentRuntime {
            meta: segment,
            source: Some(source),
            index: Some(persisted_index),
        }],
        VectorQuantization::F32,
    );
    assert_eq!(segmented.next_segment_id, 20);
    assert_eq!(segmented.segments.len(), 1);

    let mut recovered =
        VectorRuntime::new(2, VectorDistance::L2, 4, 8, 2, 1, VectorQuantization::F32);
    recovered.reconcile_docs(
        vec![
            VectorDocRecord {
                id: "deleted-before-newer-segment".to_string(),
                doc_version: 3,
                vector: vec![1.0, 0.0],
                attrs_json: "{}".to_string(),
                deleted: true,
            },
            VectorDocRecord {
                id: "kept".to_string(),
                doc_version: 2,
                vector: vec![0.0, 1.0],
                attrs_json: "{}".to_string(),
                deleted: false,
            },
            VectorDocRecord {
                id: "newer-a".to_string(),
                doc_version: 4,
                vector: vec![0.2, 0.8],
                attrs_json: "{}".to_string(),
                deleted: false,
            },
            VectorDocRecord {
                id: "newer-b".to_string(),
                doc_version: 5,
                vector: vec![0.3, 0.7],
                attrs_json: "{}".to_string(),
                deleted: false,
            },
        ],
        2,
    );
    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered.memtable_len(), 3);
    assert!(
        recovered
            .search(
                &[1.0, 0.0],
                10,
                10,
                Some(&HashSet::from(["deleted-before-newer-segment".to_string()])),
            )
            .unwrap()
            .is_empty()
    );

    let registry = VectorRuntimeRegistry::default();
    assert!(!registry.has_active_runtimes());
    let runtime_config = VectorRuntimeConfig {
        dim: 2,
        distance: VectorDistance::L2,
        m: 4,
        ef_construction: 8,
        initial_cap: 2,
        quantization: VectorQuantization::F32,
    };
    registry.reset(0, "idx", 1, runtime_config);
    assert!(registry.has_active_runtimes());
    assert!(Arc::ptr_eq(
        &registry.write_lock(0, "idx"),
        &registry.write_lock(0, "idx")
    ));
    registry
        .upsert(
            0,
            "idx",
            1,
            runtime_config,
            VectorRuntimeEntry {
                id: "id".to_string(),
                doc_version: 1,
                vector: vec![1.0, 0.0],
                attrs_json: "{}".to_string(),
            },
        )
        .unwrap();
    assert_eq!(registry.get(0, "idx", 1).unwrap().read().unwrap().len(), 1);
    registry.mark_deleted(
        0,
        "idx",
        1,
        VectorDocRecord {
            id: "id".to_string(),
            doc_version: 2,
            vector: vec![1.0, 0.0],
            attrs_json: "{}".to_string(),
            deleted: true,
        },
    );
    assert_eq!(registry.get(0, "idx", 1).unwrap().read().unwrap().len(), 0);
    registry.remove(0, "idx", 1);
    assert!(registry.get(0, "idx", 1).is_none());
    assert!(!registry.has_active_runtimes());
    assert!(registry.write_locks.is_empty());
    registry.reset(0, "db0", 1, runtime_config);
    registry.reset(1, "db1", 1, runtime_config);
    assert!(registry.has_active_runtimes());
    registry.write_lock(0, "db0");
    registry.write_lock(1, "db1");
    registry.remove_db(0);
    assert!(registry.get(0, "db0", 1).is_none());
    assert!(registry.get(1, "db1", 1).is_some());
    assert!(registry.has_active_runtimes());
    assert!(
        registry
            .write_locks
            .iter()
            .all(|entry| entry.key().db_index == 1)
    );
    registry.remove_db(1);
    assert!(!registry.has_active_runtimes());
}

#[test]
fn vector_legacy_layout_round_trips_and_drop_deletes_namespace() {
    let db = integration_test_db("onedis-vector-legacy", KeyEncodingLayout::DbPrefixedV1);
    db.vector_create("legacy-index", create_options("L2", Some(2)))
        .unwrap();
    db.vector_add(
        "legacy-index",
        "doc",
        vec![1.0, 2.0],
        Some(r#"{"brand":"acme","price":12}"#.to_string()),
    )
    .unwrap();

    let results = db
        .vector_search("legacy-index", &[1.0, 2.0], search_options(1))
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "doc");
    let (_, version, _) = db.read_vector_meta("legacy-index").unwrap();
    assert!(
        !db.store
            .scan_prefix_raw(&vector_doc_prefix(
                KeyEncodingLayout::DbPrefixedV1,
                0,
                "legacy-index",
                version,
            ))
            .is_empty()
    );

    assert_eq!(db.vector_drop("legacy-index").unwrap(), 1);
    assert!(db.vector_dim("legacy-index").unwrap().is_none());
    assert!(
        db.store
            .scan_prefix_raw(&vector_prefix(
                KeyEncodingLayout::DbPrefixedV1,
                0,
                &VECTOR_DOC_NAMESPACE,
                "legacy-index",
                version,
            ))
            .is_empty()
    );
}

#[test]
fn vector_lsm_flushes_source_then_builds_index() {
    let db = integration_test_db("onedis-vector-segments", KeyEncodingLayout::TableLocalV2);
    db.vector_create("idx", create_options("L2", Some(2)))
        .unwrap();
    db.vector_add("idx", "a", vec![0.0, 0.0], None).unwrap();
    db.vector_add("idx", "b", vec![1.0, 0.0], None).unwrap();
    let (_, version, meta_after_two) = db.read_vector_meta("idx").unwrap();
    let segment_prefix = vector_segment_prefix(db.key_layout, 0, "idx", version);
    assert_eq!(meta_after_two.snapshot_doc_version, 0);
    assert_eq!(db.store.scan_prefix_raw(&segment_prefix).len(), 0);
    let runtime = db.vector_runtimes.get(0, "idx", version).unwrap();
    assert_eq!(runtime.read().unwrap().memtable_len(), 2);

    db.vector_maintenance_tick().unwrap();
    let (_, _, meta_after_two) = db.read_vector_meta("idx").unwrap();
    assert_eq!(meta_after_two.snapshot_doc_version, 2);
    let segment_raw = db.store.scan_prefix_raw(&segment_prefix);
    assert_eq!(segment_raw.len(), 1);
    let segment = decode_record::<VectorSegmentMeta>(&segment_raw[0].1).unwrap();
    assert!(segment.index_key.is_empty());
    assert!(db.store.get_raw(&segment.source_key).is_some());
    let meta_key = vector_meta_key(db.key_layout, 0, "idx", version);
    let meta_before_reload = db.store.get_raw(&meta_key).unwrap();
    db.vector_runtimes.remove(0, "idx", version);
    let brute_force = db
        .vector_search("idx", &[0.0, 0.0], search_options(1))
        .unwrap();
    assert_eq!(brute_force[0].id, "a");
    assert_eq!(db.store.get_raw(&meta_key).unwrap(), meta_before_reload);

    db.vector_maintenance_tick().unwrap();
    let segment_raw = db.store.scan_prefix_raw(&segment_prefix);
    let segment = decode_record::<VectorSegmentMeta>(&segment_raw[0].1).unwrap();
    assert!(!segment.index_key.is_empty());
    assert!(db.store.get_raw(&segment.index_key).is_some());

    db.vector_add("idx", "c", vec![2.0, 0.0], None).unwrap();
    let (_, _, meta_after_three) = db.read_vector_meta("idx").unwrap();
    assert_eq!(meta_after_three.snapshot_doc_version, 2);
    assert_eq!(db.store.scan_prefix_raw(&segment_prefix).len(), 1);
}

#[test]
fn vector_lsm_merges_four_indexed_segments_and_reloads_topology() {
    let db = integration_test_db("onedis-vector-lsm-merge", KeyEncodingLayout::TableLocalV2);
    db.vector_create("idx", create_options("L2", Some(2)))
        .unwrap();
    for pair in 0..4 {
        for offset in 0..2 {
            let value = (pair * 2 + offset) as f32;
            db.vector_add(
                "idx",
                &format!("doc-{pair}-{offset}"),
                vec![value, 0.0],
                None,
            )
            .unwrap();
        }
        // Source publication and topology publication are deliberately two
        // independent maintenance steps.
        db.vector_maintenance_tick().unwrap();
        db.vector_maintenance_tick().unwrap();
    }

    let (_, version, _) = db.read_vector_meta("idx").unwrap();
    let prefix = vector_segment_prefix(db.key_layout, 0, "idx", version);
    let persisted_segments = db.store.scan_prefix_raw(&prefix);
    assert_eq!(persisted_segments.len(), 1);
    let merged = decode_record::<VectorSegmentMeta>(&persisted_segments[0].1).unwrap();
    assert_eq!(merged.level, 1);
    assert_eq!(merged.doc_count, 8);
    assert!(!merged.index_key.is_empty());
    let source = decode_record::<VectorSegmentBlob>(
        &db.store.get_raw(&merged.source_key).unwrap(),
    )
    .unwrap();
    let topology = decode_record::<VectorHnswIndexBlob>(
        &db.store.get_raw(&merged.index_key).unwrap(),
    )
    .unwrap();
    assert_eq!(source.entries.len(), 8);
    assert_eq!(topology.node_count(), 8);
    topology.validate().unwrap();

    let meta_key = vector_meta_key(db.key_layout, 0, "idx", version);
    let meta_before_recovery = db.store.get_raw(&meta_key).unwrap();
    let source_before_recovery = db.store.get_raw(&merged.source_key).unwrap();
    let index_before_recovery = db.store.get_raw(&merged.index_key).unwrap();
    db.vector_runtimes.remove(0, "idx", version);
    let results = db
        .vector_search("idx", &[0.0, 0.0], search_options(2))
        .unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, "doc-0-0");
    assert_eq!(db.store.get_raw(&meta_key).unwrap(), meta_before_recovery);
    assert_eq!(
        db.store.get_raw(&merged.source_key).unwrap(),
        source_before_recovery
    );
    assert_eq!(
        db.store.get_raw(&merged.index_key).unwrap(),
        index_before_recovery
    );
}

#[test]
fn quantized_backends_rerank_with_original_vectors_and_survive_reload() {
    for (suffix, quantization) in [
        ("f32", VectorQuantization::F32),
        ("q8", VectorQuantization::Q8),
        ("bin", VectorQuantization::Binary),
    ] {
        let db = integration_test_db(
            &format!("onedis-vector-quant-{suffix}"),
            KeyEncodingLayout::TableLocalV2,
        );
        let mut options = create_options("COSINE", Some(2));
        options.quantization = quantization;
        db.vector_create("idx", options).unwrap();
        db.vector_add("idx", "x", vec![1.0, 0.1], None).unwrap();
        db.vector_add("idx", "y", vec![0.8, 0.2], None).unwrap();
        db.vector_add("idx", "z", vec![-1.0, 0.0], None).unwrap();

        let results = db
            .vector_search("idx", &[1.0, 0.0], search_options(3))
            .unwrap();
        assert_eq!(results[0].id, "x");
        assert_eq!(results[2].id, "z");
        let info = db
            .vector_info("idx")
            .unwrap()
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(
            info.get("quantization").map(String::as_str),
            Some(quantization_name(quantization))
        );

        db.vector_maintenance_tick().unwrap();
        db.vector_maintenance_tick().unwrap();
        let (_, version, _) = db.read_vector_meta("idx").unwrap();
        db.vector_runtimes.remove(0, "idx", version);
        let approximate = db
            .vector_search("idx", &[1.0, 0.0], search_options(1))
            .unwrap();
        assert_eq!(approximate[0].id, "x");
        let reloaded = db
            .vector_search("idx", &[1.0, 0.0], search_options(3))
            .unwrap();
        assert_eq!(reloaded[0].id, "x");
        assert_eq!(reloaded[2].id, "z");
    }
}

#[test]
fn reduced_vectors_reuse_the_persisted_projection_after_reload() {
    let db = integration_test_db(
        "onedis-vector-reduced-reload",
        KeyEncodingLayout::TableLocalV2,
    );
    let mut options = create_options("COSINE", Some(2));
    options.source_dim = Some(3);
    db.vector_create("idx", options).unwrap();
    db.vector_add("idx", "a", vec![1.0, 0.0, 9.0], None)
        .unwrap();
    db.vector_add("idx", "b", vec![0.0, 1.0, 8.0], None)
        .unwrap();
    let before = db
        .vector_search("idx", &[1.0, 0.0, 9.0], search_options(2))
        .unwrap();
    assert_eq!(db.vector_dim("idx").unwrap(), Some(2));
    assert_eq!(
        db.vector_element("idx", "a").unwrap().unwrap().vector.len(),
        2
    );

    db.vector_maintenance_tick().unwrap();
    let (_, version, _) = db.read_vector_meta("idx").unwrap();
    db.vector_runtimes.remove(0, "idx", version);
    let reloaded = db
        .vector_search("idx", &[1.0, 0.0, 9.0], search_options(2))
        .unwrap();
    assert_eq!(
        before.iter().map(|result| &result.id).collect::<Vec<_>>(),
        reloaded.iter().map(|result| &result.id).collect::<Vec<_>>()
    );
}

#[test]
fn vector_compaction_rewrites_partial_stale_segments_and_recovery_is_read_only() {
    let db = integration_test_db(
        "onedis-vector-partial-compaction",
        KeyEncodingLayout::TableLocalV2,
    );
    db.vector_create("idx", create_options("L2", Some(2)))
        .unwrap();
    db.vector_add("idx", "a", vec![0.0, 0.0], None).unwrap();
    db.vector_add("idx", "b", vec![1.0, 0.0], None).unwrap();
    db.vector_maintenance_tick().unwrap();
    db.vector_add("idx", "a", vec![0.25, 0.0], None).unwrap();
    assert_eq!(db.vector_del("idx", &["b".to_string()]).unwrap(), 1);

    db.vector_compact("idx").unwrap();
    let (_, version, meta) = db.read_vector_meta("idx").unwrap();
    assert_eq!(meta.doc_count, 1);
    let runtime = db.vector_runtimes.get(0, "idx", version).unwrap();
    let (segments, total_nodes, deleted_nodes) = runtime.read().unwrap().segment_stats();
    assert_eq!((segments, total_nodes, deleted_nodes), (1, 1, 0));
    assert!(
        db.store
            .get_raw(&vector_doc_key(db.key_layout, 0, "idx", version, "b"))
            .is_none()
    );

    let meta_key = vector_meta_key(db.key_layout, 0, "idx", version);
    let meta_before = db.store.get_raw(&meta_key).unwrap();
    let segment_count_before = db
        .store
        .scan_prefix_raw(&vector_segment_prefix(db.key_layout, 0, "idx", version))
        .len();
    db.vector_runtimes.remove(0, "idx", version);
    let results = db
        .vector_search("idx", &[0.25, 0.0], search_options(1))
        .unwrap();
    assert_eq!(results[0].id, "a");
    assert_eq!(db.store.get_raw(&meta_key).unwrap(), meta_before);
    assert_eq!(
        db.store
            .scan_prefix_raw(&vector_segment_prefix(db.key_layout, 0, "idx", version))
            .len(),
        segment_count_before
    );
}

#[test]
fn generic_delete_evicts_vector_runtime() {
    let db = integration_test_db(
        "onedis-vector-runtime-delete",
        KeyEncodingLayout::TableLocalV2,
    );
    db.vector_create("idx", create_options("L2", None)).unwrap();
    db.vector_add("idx", "a", vec![0.0, 0.0], None).unwrap();
    let (_, version, _) = db.read_vector_meta("idx").unwrap();
    assert!(db.vector_runtimes.get(0, "idx", version).is_some());

    assert!(db.delete_key("idx"));
    assert!(db.vector_runtimes.get(0, "idx", version).is_none());
}

#[test]
fn stale_vector_commit_cannot_resurrect_a_deleted_key() {
    let db = integration_test_db("onedis-vector-stale-cas", KeyEncodingLayout::TableLocalV2);
    db.vector_create("idx", create_options("L2", None)).unwrap();
    let (expire_ms, version, meta, marker_raw, meta_raw) =
        db.read_vector_meta_observed("idx").unwrap();
    assert!(db.delete_key("idx"));

    let mut stale_batch = WriteBatch::new();
    put_vector_marker_to_batch(
        &mut stale_batch,
        db.key_layout,
        db.db_index,
        "idx",
        expire_ms,
        version,
        meta.dim,
        meta.internal,
    )
    .unwrap();
    stale_batch
        .put(
            &vector_meta_key(db.key_layout, db.db_index, "idx", version),
            &meta_raw,
        )
        .unwrap();
    assert!(
        db.commit_vector_batch_if_marker_unchanged(
            "idx",
            meta.internal,
            version,
            &marker_raw,
            &meta_raw,
            &stale_batch,
        )
        .is_err()
    );
    assert!(db.store.get_raw(&db.mk("idx")).is_none());
}

#[test]
fn concurrent_runtime_initialization_and_write_preserve_all_docs() {
    let db = Arc::new(integration_test_db(
        "onedis-vector-runtime-race",
        KeyEncodingLayout::TableLocalV2,
    ));
    db.vector_create("idx", create_options("L2", None)).unwrap();
    db.vector_add("idx", "seed", vec![0.0, 0.0], None).unwrap();
    let (_, version, _) = db.read_vector_meta("idx").unwrap();

    for generation in 1..=16 {
        db.vector_runtimes.remove(0, "idx", version);
        let barrier = Arc::new(std::sync::Barrier::new(3));
        std::thread::scope(|scope| {
            let search_db = db.clone();
            let search_barrier = barrier.clone();
            scope.spawn(move || {
                search_barrier.wait();
                search_db
                    .vector_search("idx", &[0.0, 0.0], search_options(2))
                    .unwrap();
            });
            let write_db = db.clone();
            let write_barrier = barrier.clone();
            scope.spawn(move || {
                write_barrier.wait();
                write_db
                    .vector_add("idx", "live", vec![generation as f32, 0.0], None)
                    .unwrap();
            });
            barrier.wait();
        });
        assert_eq!(db.vector_runtime_len("idx", version, 0), 2);
    }
}

#[test]
fn vector_config_distance_and_topk_limits_are_enforced() {
    let db = integration_test_db("onedis-vector-limits", KeyEncodingLayout::TableLocalV2);
    let mut invalid = create_options("L2", None);
    invalid.dim = MAX_VECTOR_DIMENSIONS + 1;
    assert!(db.vector_create("bad-dim", invalid).is_err());
    let mut invalid = create_options("L2", None);
    invalid.initial_cap = Some(MAX_VECTOR_INITIAL_CAP + 1);
    assert!(db.vector_create("bad-cap", invalid).is_err());
    let mut invalid = create_options("L2", None);
    invalid.ef_construction = Some(MAX_VECTOR_HNSW_EF + 1);
    assert!(db.vector_create("bad-ef", invalid).is_err());

    assert!(validate_vector_for_distance(&[f32::MAX, 1.0], VectorDistance::L2).is_err());
    assert!(validate_vector_for_distance(&[f32::MAX, 1.0], VectorDistance::Ip).is_err());
    let cosine =
        distance_score(VectorDistance::Cosine, &[f32::MAX, 1.0], &[f32::MAX, 1.0]).unwrap();
    assert!(cosine.is_finite());
    assert_eq!(cosine, 0.0);
    let mut ip_graph = HnswGraph::new(2, VectorDistance::Ip, 4, 8, 2, VectorQuantization::F32);
    ip_graph
        .upsert("larger-dot".to_string(), 1, vec![2.0, 0.0])
        .unwrap();
    ip_graph
        .upsert("smaller-dot".to_string(), 2, vec![1.0, 0.0])
        .unwrap();
    let ip_results = ip_graph.search(&[2.0, 0.0], 1, 8, None).unwrap();
    assert_eq!(ip_results[0].id, "larger-dot");
    assert!(
        distance_score(VectorDistance::Ip, &[2.0, 0.0], &[2.0, 0.0]).unwrap()
            < distance_score(VectorDistance::Ip, &[2.0, 0.0], &[1.0, 0.0]).unwrap()
    );

    let mut topk = TopKVectorResults::new(2, 1024).unwrap();
    for (id, score) in [("c", 3.0), ("a", 1.0), ("b", 2.0)] {
        topk.push(VectorSearchResult {
            id: id.to_string(),
            score,
            attrs: Vec::new(),
            attrs_json: None,
        })
        .unwrap();
    }
    assert_eq!(
        topk.into_sorted()
            .into_iter()
            .map(|result| result.id)
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    let mut topk = TopKVectorResults::new(1, 128).unwrap();
    assert!(
        topk.push(VectorSearchResult {
            id: "x".repeat(1024),
            score: 0.0,
            attrs: Vec::new(),
            attrs_json: None,
        })
        .is_err()
    );
}

#[test]
fn indexed_filters_use_bounded_ordered_ranges() {
    let db = integration_test_db("onedis-vector-filters", KeyEncodingLayout::TableLocalV2);
    db.vector_create("idx", create_options("L2", None)).unwrap();
    for (id, price, brand) in [
        ("low", 5, "budget"),
        ("mid", 10, "acme"),
        ("high", 15, "acme"),
    ] {
        db.vector_add(
            "idx",
            id,
            vec![price as f32, 0.0],
            Some(format!(r#"{{"brand":"{brand}","price":{price}}}"#)),
        )
        .unwrap();
    }

    for (filter, expected) in [
        ("price > 10", vec!["high"]),
        ("price >= 10", vec!["high", "mid"]),
        ("price < 10", vec!["low"]),
        ("price <= 10", vec!["low", "mid"]),
        ("brand IN ('acme','missing')", vec!["high", "mid"]),
    ] {
        let mut options = search_options(10);
        options.filter = Some(filter.to_string());
        let mut ids = db
            .vector_search("idx", &[0.0, 0.0], options)
            .unwrap()
            .into_iter()
            .map(|result| result.id)
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, expected, "filter: {filter}");
    }
}

#[test]
fn ip_candidates_are_comparable_across_segments_with_different_norms() {
    let db = integration_test_db("onedis-vector-ip-segments", KeyEncodingLayout::TableLocalV2);
    let mut options = create_options("IP", Some(32));
    options.ef_runtime = Some(64);
    db.vector_create("idx", options).unwrap();

    for segment in 0..3 {
        for offset in 0..32 {
            let value = match segment {
                0 => 1.0 - offset as f32 * 0.001,
                1 => 1_000.0 - offset as f32,
                _ => 10.0 - offset as f32 * 0.01,
            };
            db.vector_add(
                "idx",
                &format!("segment-{segment}-{offset}"),
                vec![value, 0.0],
                None,
            )
            .unwrap();
        }
        db.vector_maintenance_tick().unwrap();
        db.vector_maintenance_tick().unwrap();
    }

    let mut options = search_options(1);
    options.ef = Some(64);
    let results = db.vector_search("idx", &[1.0, 0.0], options).unwrap();
    assert_eq!(results[0].id, "segment-1-0");
}

#[test]
fn flat_internal_indexes_never_publish_hnsw_segments() {
    let db = integration_test_db("onedis-vector-flat", KeyEncodingLayout::TableLocalV2);
    let index = "__onedis_fulltext_vector__:1:3:idx:3:vec";
    db.vector_create_internal(index, create_options("L2", Some(2)), true)
        .unwrap();
    db.vector_add(index, "a", vec![0.0, 0.0], None)
        .unwrap();
    db.vector_add(index, "b", vec![1.0, 0.0], None)
        .unwrap();
    db.vector_maintenance_tick().unwrap();

    let (_, version, meta) = db.read_vector_meta(index).unwrap();
    assert_eq!(meta.algorithm, VectorIndexAlgorithm::Flat);
    assert!(db.vector_runtimes.get(0, index, version).is_none());
    assert!(
        db.store
            .scan_prefix_raw(&vector_segment_prefix(db.key_layout, 0, index, version))
            .is_empty()
    );
    assert_eq!(
        db.vector_search(index, &[0.0, 0.0], search_options(1))
            .unwrap()[0]
            .id,
        "a"
    );
}

#[test]
fn version_checkpoint_replays_only_the_unflushed_tail() {
    let db = integration_test_db(
        "onedis-vector-version-checkpoint",
        KeyEncodingLayout::TableLocalV2,
    );
    db.vector_create("idx", create_options("L2", Some(2)))
        .unwrap();
    db.vector_add("idx", "a", vec![0.0, 0.0], None)
        .unwrap();
    db.vector_add("idx", "b", vec![1.0, 0.0], None)
        .unwrap();
    db.vector_maintenance_tick().unwrap();
    db.vector_maintenance_tick().unwrap();
    db.vector_compact("idx").unwrap();
    db.vector_add("idx", "c", vec![2.0, 0.0], None)
        .unwrap();

    let (_, version, meta) = db.read_vector_meta("idx").unwrap();
    db.vector_runtimes.remove(0, "idx", version);
    let (versions, tail) = db
        .load_vector_version_state("idx", version, &meta)
        .unwrap();
    assert_eq!(versions.len(), 3);
    assert_eq!(versions.get("c"), Some(&3));
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].id, "c");
}

#[test]
fn parallel_immutable_builder_emits_a_valid_packed_graph() {
    let source = VectorSegmentBlob {
        entries: (0..300)
            .map(|index| VectorSegmentEntry {
                id: format!("doc-{index}"),
                doc_version: index as u64 + 1,
                vector: vec![index as f32, 1.0],
            })
            .collect(),
    };
    let index = VectorHnswIndexBlob::build(&source, &meta(VectorDistance::L2)).unwrap();
    index.validate().unwrap();
    assert_eq!(index.node_count(), 300);
    assert_eq!(index.node_layer_offsets.len(), 301);
    assert!(index.layer_neighbor_offsets.len() > 300);
    assert!(!index.neighbors.is_empty());
}
