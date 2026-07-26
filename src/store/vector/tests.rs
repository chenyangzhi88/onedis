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

    fn create_options(
        distance: &str,
        segment_max_docs: Option<u64>,
    ) -> VectorCreateOptions {
        VectorCreateOptions {
            dim: 2,
            distance: distance.to_string(),
            schema: schema(),
            segment_max_docs,
            m: Some(4),
            ef_construction: Some(8),
            ef_runtime: Some(8),
            initial_cap: Some(8),
        }
    }

    fn search_options(k: usize) -> VectorSearchOptions {
        VectorSearchOptions {
            k,
            filter: None,
            with_scores: true,
            with_attrs: Vec::new(),
            ef: None,
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

        let attrs =
            parse_attrs(r#"{"brand":["acme","budget"],"price":12.5,"title":"lamp"}"#).unwrap();
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
        let raw = encode_record(&VectorDocRecord {
            id: "doc1".to_string(),
            doc_version: 7,
            vector: vec![1.0, 1.0],
            attrs_json: r#"{"brand":"acme","price":9}"#.to_string(),
            deleted: false,
        })
        .unwrap();
        let result = doc_to_search_result(
            &raw,
            &meta,
            &[1.0, 2.0],
            &["brand".to_string()],
            &parse_filter("brand == acme").unwrap(),
            Some(7),
        )
        .unwrap()
        .unwrap();
        assert_eq!(result.id, "doc1");
        assert_eq!(
            result.attrs,
            vec![("brand".to_string(), "acme".to_string())]
        );
        assert!(
            doc_to_search_result(&raw, &meta, &[1.0, 2.0], &[], &[], Some(8))
                .unwrap()
                .is_none()
        );
        assert!(
            doc_to_search_result(
                &raw,
                &meta,
                &[1.0, 2.0],
                &[],
                &parse_filter("brand == other").unwrap(),
                None,
            )
            .unwrap()
            .is_none()
        );
        let deleted = encode_record(&VectorDocRecord {
            id: "doc2".to_string(),
            doc_version: 8,
            vector: vec![2.0, 2.0],
            attrs_json: "{}".to_string(),
            deleted: true,
        })
        .unwrap();
        assert!(
            doc_to_search_result(&deleted, &meta, &[1.0, 2.0], &[], &[], None)
                .unwrap()
                .is_none()
        );
        assert!(decode_record::<VectorDocRecord>(b"bad").is_err());

        let mut results = vec![
            VectorSearchResult {
                id: "b".to_string(),
                score: 0.1,
                attrs: Vec::new(),
            },
            VectorSearchResult {
                id: "a".to_string(),
                score: 0.1,
                attrs: Vec::new(),
            },
            VectorSearchResult {
                id: "c".to_string(),
                score: 0.2,
                attrs: Vec::new(),
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
                ef: None,
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
    fn hnsw_graph_runtime_snapshot_and_registry_paths_cover_edges() {
        let mut graph = HnswGraph::new(2, VectorDistance::L2, 0, 1, 0);
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

        let snapshot = graph.to_snapshot();
        let rebuilt = HnswGraph::from_snapshot(snapshot).unwrap();
        assert_eq!(rebuilt.len(), 1);
        assert!(
            HnswGraph::from_snapshot(HnswGraphSnapshot {
                dim: 2,
                distance: VectorDistance::Cosine,
                m: 4,
                ef_construction: 8,
                nodes: vec![HnswSnapshotNode {
                    id: "zero".to_string(),
                    doc_version: 1,
                    vector: vec![0.0, 0.0],
                    deleted: false,
                }],
            })
            .is_err()
        );

        let mut runtime = VectorRuntime::new(2, VectorDistance::L2, 4, 8, 2, 10);
        assert!(runtime.freeze_active().is_none());
        runtime.upsert("r1".to_string(), 1, vec![1.0, 0.0]).unwrap();
        runtime.upsert("r2".to_string(), 2, vec![0.0, 1.0]).unwrap();
        let (mut segment, snapshot) = runtime.freeze_active().unwrap();
        assert_eq!(segment.segment_id, 10);
        assert_eq!(segment.doc_count, 2);
        assert_eq!(runtime.next_segment_id, 11);
        segment.graph_key = b"graph-key".to_vec();
        runtime.set_segment_graph_key(10, segment.graph_key.clone());
        assert_eq!(runtime.segments[0].meta.graph_key, b"graph-key".to_vec());
        runtime.upsert("r1".to_string(), 3, vec![0.9, 0.1]).unwrap();
        assert_eq!(runtime.len(), 2);
        runtime.upsert("r3".to_string(), 3, vec![0.2, 0.2]).unwrap();
        assert_eq!(runtime.len(), 3);
        assert!(!runtime.search(&[1.0, 0.0], 3, 2, None).unwrap().is_empty());
        runtime.mark_deleted("r1");
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
                graph: HnswGraph::from_snapshot(snapshot).unwrap(),
            }],
        );
        assert_eq!(segmented.next_segment_id, 20);
        assert_eq!(segmented.segments.len(), 1);

        let mut recovered = VectorRuntime::new(2, VectorDistance::L2, 4, 8, 2, 1);
        recovered
            .upsert("deleted-before-newer-segment".to_string(), 1, vec![1.0, 0.0])
            .unwrap();
        recovered
            .upsert("kept".to_string(), 2, vec![0.0, 1.0])
            .unwrap();
        recovered.freeze_active().unwrap();
        recovered
            .upsert("newer-a".to_string(), 4, vec![0.2, 0.8])
            .unwrap();
        recovered
            .upsert("newer-b".to_string(), 5, vec![0.3, 0.7])
            .unwrap();
        recovered.freeze_active().unwrap();
        recovered
            .reconcile_docs(vec![
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
            ])
            .unwrap();
        assert_eq!(recovered.len(), 3);
        assert!(
            recovered
                .search(
                    &[1.0, 0.0],
                    10,
                    10,
                    Some(&HashSet::from([
                        "deleted-before-newer-segment".to_string()
                    ])),
                )
                .unwrap()
                .is_empty()
        );

        let registry = VectorRuntimeRegistry::default();
        let runtime_config = VectorRuntimeConfig {
            dim: 2,
            distance: VectorDistance::L2,
            m: 4,
            ef_construction: 8,
            initial_cap: 2,
        };
        registry.reset(0, "idx", 1, runtime_config);
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
                },
            )
            .unwrap();
        assert_eq!(registry.get(0, "idx", 1).unwrap().read().unwrap().len(), 1);
        registry.mark_deleted(0, "idx", 1, "id");
        assert_eq!(registry.get(0, "idx", 1).unwrap().read().unwrap().len(), 0);
        registry.remove(0, "idx", 1);
        assert!(registry.get(0, "idx", 1).is_none());
        assert!(registry.write_locks.is_empty());
        registry.reset(0, "db0", 1, runtime_config);
        registry.reset(1, "db1", 1, runtime_config);
        registry.write_lock(0, "db0");
        registry.write_lock(1, "db1");
        registry.remove_db(0);
        assert!(registry.get(0, "db0", 1).is_none());
        assert!(registry.get(1, "db1", 1).is_some());
        assert!(
            registry
                .write_locks
                .iter()
                .all(|entry| entry.key().db_index == 1)
        );
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
    fn vector_segment_freeze_uses_delta_since_snapshot() {
        let db = integration_test_db("onedis-vector-segments", KeyEncodingLayout::TableLocalV2);
        db.vector_create("idx", create_options("L2", Some(2)))
            .unwrap();
        db.vector_add("idx", "a", vec![0.0, 0.0], None)
            .unwrap();
        db.vector_add("idx", "b", vec![1.0, 0.0], None)
            .unwrap();
        let (_, version, meta_after_two) = db.read_vector_meta("idx").unwrap();
        let segment_prefix = vector_segment_prefix(db.key_layout, 0, "idx", version);
        assert_eq!(meta_after_two.snapshot_doc_version, 2);
        assert_eq!(db.store.scan_prefix_raw(&segment_prefix).len(), 1);

        db.vector_add("idx", "c", vec![2.0, 0.0], None)
            .unwrap();
        let (_, _, meta_after_three) = db.read_vector_meta("idx").unwrap();
        assert_eq!(meta_after_three.snapshot_doc_version, 2);
        assert_eq!(db.store.scan_prefix_raw(&segment_prefix).len(), 1);
    }

    #[test]
    fn generic_delete_evicts_vector_runtime() {
        let db = integration_test_db(
            "onedis-vector-runtime-delete",
            KeyEncodingLayout::TableLocalV2,
        );
        db.vector_create("idx", create_options("L2", None))
            .unwrap();
        db.vector_add("idx", "a", vec![0.0, 0.0], None)
            .unwrap();
        let (_, version, _) = db.read_vector_meta("idx").unwrap();
        assert!(db.vector_runtimes.get(0, "idx", version).is_some());

        assert!(db.delete_key("idx"));
        assert!(db.vector_runtimes.get(0, "idx", version).is_none());
    }

    #[test]
    fn concurrent_runtime_initialization_and_write_preserve_all_docs() {
        let db = Arc::new(integration_test_db(
            "onedis-vector-runtime-race",
            KeyEncodingLayout::TableLocalV2,
        ));
        db.vector_create("idx", create_options("L2", None))
            .unwrap();
        db.vector_add("idx", "seed", vec![0.0, 0.0], None)
            .unwrap();
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
        assert!(
            DistInnerProduct.eval(&[2.0, 0.0], &[2.0, 0.0])
                < DistInnerProduct.eval(&[1.0, 0.0], &[2.0, 0.0])
        );

        let mut topk = TopKVectorResults::new(2, 1024).unwrap();
        for (id, score) in [("c", 3.0), ("a", 1.0), ("b", 2.0)] {
            topk
                .push(VectorSearchResult {
                    id: id.to_string(),
                    score,
                    attrs: Vec::new(),
                })
                .unwrap();
        }
        assert_eq!(
            topk
                .into_sorted()
                .into_iter()
                .map(|result| result.id)
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        let mut topk = TopKVectorResults::new(1, 128).unwrap();
        assert!(
            topk
                .push(VectorSearchResult {
                    id: "x".repeat(1024),
                    score: 0.0,
                    attrs: Vec::new(),
                })
                .is_err()
        );
    }

    #[test]
    fn indexed_filters_use_bounded_ordered_ranges() {
        let db = integration_test_db("onedis-vector-filters", KeyEncodingLayout::TableLocalV2);
        db.vector_create("idx", create_options("L2", None))
            .unwrap();
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
