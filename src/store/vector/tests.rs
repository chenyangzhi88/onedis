    use super::*;

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
            raw.clone(),
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
            doc_to_search_result(raw.clone(), &meta, &[1.0, 2.0], &[], &[], Some(8))
                .unwrap()
                .is_none()
        );
        assert!(
            doc_to_search_result(
                raw,
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
            doc_to_search_result(deleted, &meta, &[1.0, 2.0], &[], &[], None)
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
        assert_eq!(reduced.len(), 2);
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

        let registry = VectorRuntimeRegistry::default();
        registry.reset(0, "idx", 1, 2, VectorDistance::L2, 4, 8, 2);
        assert!(Arc::ptr_eq(
            &registry.write_lock(0, "idx"),
            &registry.write_lock(0, "idx")
        ));
        registry
            .upsert(
                0,
                "idx",
                1,
                2,
                VectorDistance::L2,
                4,
                8,
                2,
                "id".to_string(),
                1,
                vec![1.0, 0.0],
            )
            .unwrap();
        assert_eq!(registry.get(0, "idx", 1).unwrap().read().unwrap().len(), 1);
        registry.mark_deleted(0, "idx", 1, "id");
        assert_eq!(registry.get(0, "idx", 1).unwrap().read().unwrap().len(), 0);
        registry.remove(0, "idx", 1);
        assert!(registry.get(0, "idx", 1).is_none());
    }
