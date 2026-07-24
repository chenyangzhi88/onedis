use super::super::*;
use super::support::*;

#[test]
fn runtime_indexes_searches_deletes_synonyms_and_registry_paths() {
    let store = test_store("runtime");
    store.put_raw(
        &fulltext_syn_key(0, "idx", "g1"),
        &encode_record(&FullTextSynonymGroup {
            terms: vec!["car".to_string(), "automobile".to_string()],
        })
        .unwrap(),
    );

    let meta = meta(vec![
        {
            let mut title = text_field("title");
            title.options.alias = Some("t".to_string());
            title.options.phonetic = Some("dm:en".to_string());
            title.options.with_suffix_trie = true;
            title
        },
        field("tag", FullTextFieldKind::Tag),
        field("price", FullTextFieldKind::Numeric),
        {
            let mut ignored = text_field("ignored");
            ignored.options.noindex = true;
            ignored
        },
    ]);
    let mut runtime =
        FullTextRuntime::new(store.clone(), 0, "idx", "idx", &meta, &runtime_config()).unwrap();
    assert_eq!(runtime.synonyms.get("car").unwrap().len(), 1);
    assert!(runtime.refresh_due(&FullTextRefreshPolicy {
        refresh_interval_ms: 0,
        ..FullTextRefreshPolicy::default()
    }));
    assert!(!runtime.refresh_due(&FullTextRefreshPolicy {
        refresh_interval_ms: 60_000,
        ..FullTextRefreshPolicy::default()
    }));

    runtime
        .upsert_hash(
            "doc:1",
            &[
                ("title".to_string(), "fast automobile".to_string()),
                ("tag".to_string(), "vehicle".to_string()),
                ("price".to_string(), "10".to_string()),
                ("ignored".to_string(), "secret".to_string()),
            ],
        )
        .unwrap();
    runtime
        .upsert_fields(
            "doc:2",
            &[
                ("title".to_string(), "slow train".to_string()),
                ("tag".to_string(), "rail".to_string()),
                ("price".to_string(), "25".to_string()),
            ],
        )
        .unwrap();
    runtime
        .upsert_fields(
            "doc:3",
            &[("title".to_string(), "running swiftly".to_string())],
        )
        .unwrap();
    runtime.publish().unwrap();

    let options = search_options();
    assert_eq!(
        runtime
            .search("*", &options, None, search_deadline())
            .unwrap()
            .hits
            .len(),
        3
    );
    assert_eq!(
        runtime
            .search("car", &options, None, search_deadline())
            .unwrap()
            .hits[0]
            .key,
        "doc:1"
    );
    assert_eq!(
        runtime
            .search_ast(
                &FullTextQueryAst::Tag {
                    field: "tag".to_string(),
                    values: vec!["rail".to_string()],
                },
                &options,
                10,
                search_deadline(),
            )
            .unwrap()[0]
            .key,
        "doc:2"
    );
    assert_eq!(
        runtime
            .search_ast(
                &FullTextQueryAst::Numeric {
                    field: "price".to_string(),
                    min: FullTextNumericBound::Inclusive(9.0),
                    max: FullTextNumericBound::Exclusive(11.0),
                },
                &options,
                10,
                search_deadline(),
            )
            .unwrap()[0]
            .key,
        "doc:1"
    );
    assert_eq!(
        runtime
            .search("@t:fast", &options, None, search_deadline())
            .unwrap()
            .hits[0]
            .key,
        "doc:1"
    );
    assert_eq!(
        runtime
            .search("auto*", &options, None, search_deadline())
            .unwrap()
            .hits[0]
            .key,
        "doc:1"
    );
    assert_eq!(
        runtime
            .search("a?tomobile", &options, None, search_deadline())
            .unwrap()
            .hits[0]
            .key,
        "doc:1"
    );
    assert_eq!(
        runtime
            .search("%automobiel%", &options, None, search_deadline())
            .unwrap()
            .hits[0]
            .key,
        "doc:1"
    );
    assert!(runtime.build_query("\"fast automobile\"", &options).is_ok());
    assert_eq!(
        runtime
            .search("run", &options, None, search_deadline())
            .unwrap()
            .hits[0]
            .key,
        "doc:3"
    );
    assert!(
        runtime
            .search("\"run swiftly\"", &options, None, search_deadline())
            .unwrap()
            .hits
            .is_empty()
    );
    assert_eq!(
        runtime
            .search("\"running swiftly\"", &options, None, search_deadline())
            .unwrap()
            .hits[0]
            .key,
        "doc:3"
    );

    let scoped = FullTextQueryAst::Field {
        fields: vec!["t".to_string()],
        expr: Box::new(FullTextQueryAst::Text("train".to_string())),
    };
    assert_eq!(
        runtime
            .search_ast(&scoped, &options, 10, search_deadline())
            .unwrap()[0]
            .key,
        "doc:2"
    );
    assert!(
        runtime
            .plan_text_query("x", Some(&["price".to_string()]), &options)
            .is_err()
    );
    assert!(
        runtime
            .plan_tag_query("missing", &["x".to_string()])
            .is_err()
    );
    assert!(
        runtime
            .plan_numeric_query(
                "tag",
                FullTextNumericBound::NegInf,
                FullTextNumericBound::PosInf,
            )
            .is_err()
    );
    assert!(
        runtime
            .plan_boolean(&[], Occur::Must, None, &options)
            .is_ok()
    );
    assert!(
        runtime
            .plan_query(
                &FullTextQueryAst::Geo {
                    field: "loc".to_string(),
                    lon: 0.0,
                    lat: 0.0,
                    radius: 1.0,
                    unit: "m".to_string(),
                },
                None,
                &options,
            )
            .is_err()
    );
    assert!(
        runtime
            .plan_query(
                &FullTextQueryAst::VectorRange {
                    field: "vec".to_string(),
                    radius: 1.0,
                    blob_param: "q".to_string(),
                },
                None,
                &options,
            )
            .is_err()
    );

    runtime.delete_hash("doc:1");
    runtime.publish().unwrap();
    assert!(
        runtime
            .search("car", &options, None, search_deadline())
            .unwrap()
            .hits
            .is_empty()
    );

    let registry = FullTextRuntimeRegistry::default();
    registry.insert(0, "idx", runtime);
    assert!(registry.get(0, "idx").is_some());
    registry.remove(0, "idx");
    assert!(registry.get(0, "idx").is_none());

    let runtime = FullTextRuntime::new(store, 0, "idx2", "idx2", &meta, &runtime_config()).unwrap();
    registry.insert(0, "idx2", runtime);
    assert!(registry.get(0, "idx2").is_some());
    registry.remove_db(0);
    assert!(registry.get(0, "idx2").is_none());

    let store = test_store("runtime-no-positions");
    let mut no_positions_meta = super::support::meta(vec![text_field("title")]);
    no_positions_meta.index_options.no_offsets = true;
    no_positions_meta.active_storage = "idx-no-positions".to_string();
    let mut no_positions = FullTextRuntime::new(
        store,
        0,
        "idx-no-positions",
        "idx-no-positions",
        &no_positions_meta,
        &runtime_config(),
    )
    .unwrap();
    no_positions
        .upsert_fields(
            "doc:1",
            &[("title".to_string(), "ordinary token".to_string())],
        )
        .unwrap();
    no_positions.publish().unwrap();
    assert_eq!(
        no_positions
            .search("ordinary", &options, None, search_deadline())
            .unwrap()
            .hits[0]
            .key,
        "doc:1"
    );
    assert!(
        no_positions
            .search("\"ordinary token\"", &options, None, search_deadline())
            .is_err()
    );
    assert!(
        no_positions
            .search(
                "*",
                &options,
                None,
                FullTextSearchDeadline {
                    at: Instant::now(),
                    fail_on_timeout: true,
                },
            )
            .is_err()
    );

    let store = test_store("runtime-no-highlight");
    let mut no_highlight_meta = super::support::meta(vec![text_field("title")]);
    no_highlight_meta.index_options.no_hl = true;
    no_highlight_meta.active_storage = "idx-no-highlight".to_string();
    let mut no_highlight = FullTextRuntime::new(
        store,
        0,
        "idx-no-highlight",
        "idx-no-highlight",
        &no_highlight_meta,
        &runtime_config(),
    )
    .unwrap();
    no_highlight
        .upsert_fields(
            "doc:1",
            &[("title".to_string(), "ordinary token".to_string())],
        )
        .unwrap();
    no_highlight.publish().unwrap();
    assert_eq!(
        no_highlight
            .search("\"ordinary token\"", &options, None, search_deadline(),)
            .unwrap()
            .hits[0]
            .key,
        "doc:1"
    );

    let store = test_store("runtime-stopword-positions");
    let mut stopword_meta = super::support::meta(vec![text_field("title")]);
    stopword_meta.index_options.stopwords = Some(vec!["the".to_string()]);
    stopword_meta.active_storage = "idx-stopword-positions".to_string();
    let mut stopword_runtime = FullTextRuntime::new(
        store,
        0,
        "idx-stopword-positions",
        "idx-stopword-positions",
        &stopword_meta,
        &runtime_config(),
    )
    .unwrap();
    stopword_runtime
        .upsert_fields(
            "doc:1",
            &[("title".to_string(), "quick the fox".to_string())],
        )
        .unwrap();
    stopword_runtime.publish().unwrap();
    assert!(
        stopword_runtime
            .search("the", &options, None, search_deadline())
            .unwrap()
            .hits
            .is_empty()
    );
    assert!(
        stopword_runtime
            .search("\"quick fox\"", &options, None, search_deadline())
            .unwrap()
            .hits
            .is_empty()
    );
    assert_eq!(
        stopword_runtime
            .search("\"quick the fox\"", &options, None, search_deadline(),)
            .unwrap()
            .hits[0]
            .key,
        "doc:1"
    );

    let store = test_store("runtime-expansions");
    let expansion_meta = super::support::meta(vec![text_field("title")]);
    let mut expansion_config = runtime_config();
    expansion_config.max_expansions = 1;
    expansion_config.max_prefix_expansions = 1;
    let mut expansion_runtime = FullTextRuntime::new(
        store,
        0,
        "idx-expansions",
        "idx-expansions",
        &expansion_meta,
        &expansion_config,
    )
    .unwrap();
    expansion_runtime
        .upsert_fields("doc:1", &[("title".to_string(), "apple".to_string())])
        .unwrap();
    expansion_runtime
        .upsert_fields("doc:2", &[("title".to_string(), "azure".to_string())])
        .unwrap();
    expansion_runtime
        .upsert_fields("doc:3", &[("title".to_string(), "apricot".to_string())])
        .unwrap();
    expansion_runtime.publish().unwrap();
    assert!(
        expansion_runtime
            .search("a?*", &options, None, search_deadline())
            .is_err()
    );
    assert!(
        expansion_runtime
            .search("ap*", &options, None, search_deadline())
            .is_err()
    );
}
